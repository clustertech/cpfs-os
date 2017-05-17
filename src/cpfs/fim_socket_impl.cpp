/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of cpfs::IFimSocket interface for send and receive
 * FIM messages over an Asio-type socket.
 */
#include "fim_socket_impl.hpp"

#include <exception>
#include <queue>
#include <string>

#include <boost/array.hpp>
#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/functional/factory.hpp>
#include <boost/functional/forward_adapter.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "fim.hpp"
#include "fim_maker.hpp"
#include "fim_processor.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "periodic_timer.hpp"
#include "req_tracker.hpp"

namespace cpfs {

namespace {

/**
 * Tolerance of periodic timer to use.
 */
const double kTimerTolerance = 1.0 / 32;

/**
 * Implement IFimSocket interface for send and receive of FIM messages
 */
class FimSocket : public IFimSocket {
 public:
  /**
   * Constructs a FimSocket to send/receive FIMs
   *
   * @param socket The socket
   *
   * @param policy The socket policy to use
   */
  explicit FimSocket(TcpSocket* socket, IAsioPolicy* policy);
  IAsioPolicy* asio_policy() const;
  TcpSocket* socket() const;
  std::string name() const;
  std::string remote_info() const;
  void SetFimProcessor(IFimProcessor* fim_processor);
  void StartRead();
  void SetIdleTimer(const boost::shared_ptr<IPeriodicTimer>& timer);
  void WriteMsg(const FIM_PTR<IFim>& write_msg);
  bool pending_write() const;
  void SetReqTracker(const boost::shared_ptr<IReqTracker>& req_tracker);
  IReqTracker* GetReqTracker() const;
  void SetHeartbeatTimer(const boost::shared_ptr<IPeriodicTimer>& timer);
  void Migrate(IAsioPolicy* policy);
  void Reset();
  void Shutdown();
  bool ShutdownCalled();
  void OnCleanup(FimSocketCleanupCallback cleanup_callback);

 private:
  IAsioPolicy* policy_; /**< The asio policy used */
  boost::scoped_ptr<TcpSocket> socket_; /**< The socket for I/O */
  // Only one thread do the reading, so the following does not need locking
  boost::array<char, 16> header_buf_; /**< Hold Fim header received */
  FIM_PTR<IFim> read_fim_; /**< Fim currently being read */
  boost::atomic<IReqTracker*> f_req_tracker_; /**< fast-path to find tracker */

  boost::shared_ptr<IReqTracker> req_tracker_; /**< keep tracker from deleted */
  mutable MUTEX_TYPE data_mutex_; /**< Protect data below */
  IFimProcessor* fim_processor_; /**< For handling Fim objects received */
  std::queue<FIM_PTR<IFim> > send_queue_; /**< Fims to be sent */

  SIOHandler header_handler_; /**< Cached handler for header */
  SIOHandler body_handler_; /**< Cached handler for body */
  SIOHandler write_handler_; /**< Cached handler for write completion */

  boost::shared_ptr<IPeriodicTimer> heartbeat_timer_; /**< Generate heartbeat */
  boost::shared_ptr<IPeriodicTimer> idle_timer_; /**< Detect heartbeat loss */

  FimSocketCleanupCallback cleanup_callback_; /**< callback to do cleanup */
  bool shutdown_; /**< Whether Shutdown() is called */
  boost::atomic<bool> atomic_shutdown_; /**< Same but atomic */

  boost::shared_ptr<FimSocket> GetShared() {
    return boost::static_pointer_cast<FimSocket>(shared_from_this());
  }

  void CheckHandler_() {
    if (header_handler_)
      return;
    header_handler_ = boost::bind(&FimSocket::HandleHeader,
                                  GetShared(),
                                  boost::asio::placeholders::error);
    body_handler_ = boost::bind(&FimSocket::HandleBody,
                                GetShared(),
                                boost::asio::placeholders::error);
    write_handler_ = boost::bind(&FimSocket::HandleWrite,
                                 GetShared(),
                                 boost::asio::placeholders::error);
  }

  /**
   * Handle the FIM protocol header by constructing a full length
   * FIM for further read
   *
   * @param error The error code from async_read operation
   */
  void HandleHeader(const boost::system::error_code& error);

  /**
   * Handle the FIM protocol body.
   *
   * @param error The error code from async_read operation
   */
  void HandleBody(const boost::system::error_code& error);

  /**
   * Handle the FIM by calling the processor.  Not expected to throw.
   */
  void HandleFim();

  /**
   * Send the FIM message that is queued in the front
   */
  void ProcessSendQueue_();

  /**
   * Handle the FIM write. The next pending FIM message will be written
   *
   * @param error The error code from async_write operation
   */
  void HandleWrite(const boost::system::error_code& error);

  /**
   * Send heartbeat
   *
   * @param wptr The weak pointer to the FimSocket
   *
   * @return true To indicate that the timer should be reset and waiting
   * for next timeout
   */
  static bool SendHeartbeat(boost::weak_ptr<FimSocket> wptr);

  /**
   * Handle read idle timeout, heartbeat is not received within timeout.
   *
   * @param wptr The weak pointer to the FimSocket
   *
   * @return false To indicate that the timer should be stopped
   */
  static bool HandleIdleTimeout(boost::weak_ptr<FimSocket> wptr);

  /**
   * The actual code of Reset().
   */
  void Reset_();
};

FimSocket::FimSocket(TcpSocket* socket, IAsioPolicy* policy)
    : policy_(policy), socket_(socket), f_req_tracker_(0),
      data_mutex_(MUTEX_INIT), fim_processor_(0),
      shutdown_(!socket), atomic_shutdown_(shutdown_) {
  if (socket)
    policy_->SetTcpNoDelay(socket);
}

IAsioPolicy* FimSocket::asio_policy() const {
  return policy_;
}

TcpSocket* FimSocket::socket() const {
  return socket_.get();
}

std::string FimSocket::name() const {
  IReqTracker* req_tracker = f_req_tracker_;
  if (!req_tracker)
    return std::string();
  return req_tracker->name();
}

std::string FimSocket::remote_info() const {
  std::stringstream ss;
  try {
    TcpEndpoint endpoint = policy_->GetRemoteEndpoint(socket_.get());
    ss << endpoint;
  } catch (std::exception&) {
    ss << "Unconnected FIM socket";
  }
  return ss.str();
}

void FimSocket::SetFimProcessor(IFimProcessor* fim_processor) {
  MUTEX_LOCK_GUARD(data_mutex_);
  fim_processor_ = fim_processor;
}

void FimSocket::StartRead() {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (shutdown_)
    return;
  CheckHandler_();
  LOG(debug, Fim, "<... ",  PVal(*this));
  read_fim_.reset();
  policy_->AsyncRead(socket_.get(), boost::asio::buffer(header_buf_),
                     header_handler_);
}

void FimSocket::HandleHeader(const boost::system::error_code& error) {
  if (error) {
    if (error.value() != boost::asio::error::eof
        && error.value() != boost::asio::error::operation_aborted)
      LOG(warning, Fim, "FIM header: corrupted FIM received from ",
          PVal(*this), ": ", error.message().c_str());
    Shutdown();
    return;
  }
  // Construct a full-length FIM from header info
  try {
    read_fim_ = GetFimsMaker().FromHead(&header_buf_[0]);
  } catch (const std::exception& ex) {
    LOG(error, Fim, "FIM header: failed interpreting FIM received from ",
        PVal(*this), ": ", ex.what());
    Shutdown();
    return;
  }
  if (idle_timer_)
    idle_timer_->Reset(kTimerTolerance);
  // Read body of this FIM
  if (read_fim_->body_size() > 0) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (shutdown_)
      return;
    policy_->AsyncRead(
        socket_.get(),
        boost::asio::buffer(read_fim_->body(), read_fim_->body_size()),
        body_handler_);
  } else {
    HandleFim();
    StartRead();
  }
}

void FimSocket::HandleBody(const boost::system::error_code& error) {
  if (error) {
    LOG(warning, Fim, "FIM body: failed interpreting FIM received from ",
        PVal(*this), ": ", error.message().c_str());
    Shutdown();
    return;
  }
  HandleFim();
  StartRead();
}

void FimSocket::HandleFim() {
  IFimProcessor* fim_processor;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    fim_processor = fim_processor_;
  }
  if (!fim_processor) {
    LOG(error, Fim, "FIM processing: No processor for ", PVal(*this),
        " when processing ", PVal(read_fim_));
    return;
  }
  bool ret;
  try {
    LOG(debug, Fim, "<p ", PVal(*this), " ", PVal(read_fim_));
    ret = fim_processor->Process(read_fim_, shared_from_this());
  } catch (const std::exception& ex) {
    LOG(warning, Fim, "FIM processing: Failed processing ", PVal(read_fim_),
        " from ", PVal(*this), ": ", ex.what());
    return;
  }
  if (ret)
    LOG(debug, Fim, "<< ", PVal(*this), " ", PVal(read_fim_));
  else
    LOG(warning, Fim, "FIM processing: Uninteresting ", PVal(read_fim_),
        " from ", PVal(*this));
}

void FimSocket::SetIdleTimer(const boost::shared_ptr<IPeriodicTimer>& timer) {
  MUTEX_LOCK_GUARD(data_mutex_);
  idle_timer_ = timer;
  idle_timer_->OnTimeout(
      boost::bind(&FimSocket::HandleIdleTimeout,
                  boost::weak_ptr<FimSocket>(GetShared())));
}

bool FimSocket::HandleIdleTimeout(boost::weak_ptr<FimSocket> wptr) {
  if (boost::shared_ptr<FimSocket> ptr = wptr.lock()) {
    LOG(warning, Fim, "FIM idle timeout: triggered for ", PVal(*ptr));
    ptr->Shutdown();
  }
  return false;
}

void FimSocket::WriteMsg(const FIM_PTR<IFim>& write_msg) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (shutdown_)
    return;
  CheckHandler_();
  LOG(debug, Fim, ">q ", PVal(*this), " ", PVal(write_msg));
  send_queue_.push(write_msg);
  if (send_queue_.size() == 1)
    ProcessSendQueue_();
}

void FimSocket::ProcessSendQueue_() {
  const FIM_PTR<IFim>& fim = send_queue_.front();
  LOG(debug, Fim, ">> ", PVal(*this), " ", PVal(fim),
      ", qlen = ", PINT(send_queue_.size()));
  policy_->AsyncWrite(socket_.get(),
                      boost::asio::buffer(fim->msg(), fim->len()),
                      write_handler_);
}

void FimSocket::HandleWrite(const boost::system::error_code& error) {
  if (error) {
    LOG(warning, Fim, "FIM sending: sending to ", PVal(*this), " aborted: ",
        error.message().c_str());
    Shutdown();
    return;
  }
  MUTEX_LOCK_GUARD(data_mutex_);
  if (heartbeat_timer_)
    heartbeat_timer_->Reset(kTimerTolerance);
  send_queue_.pop();
  if (shutdown_)
    return;
  if (!send_queue_.empty())
    ProcessSendQueue_();
  else
    LOG(debug, Fim, ">... ", PVal(*this));
}

bool FimSocket::pending_write() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return send_queue_.size() > 0;
}

void FimSocket::SetHeartbeatTimer(
    const boost::shared_ptr<IPeriodicTimer>& timer) {
  MUTEX_LOCK_GUARD(data_mutex_);
  heartbeat_timer_ = timer;
  heartbeat_timer_->OnTimeout(
      boost::bind(&FimSocket::SendHeartbeat,
                  boost::weak_ptr<FimSocket>(GetShared())));
}

bool FimSocket::SendHeartbeat(boost::weak_ptr<FimSocket> wptr) {
  boost::shared_ptr<FimSocket> ptr = wptr.lock();
  if (ptr)
    ptr->WriteMsg(HeartbeatFim::MakePtr());
  return bool(ptr);
}

void FimSocket::SetReqTracker(
    const boost::shared_ptr<IReqTracker>& req_tracker) {
  req_tracker_ = req_tracker;
  f_req_tracker_ = req_tracker.get();
}

IReqTracker* FimSocket::GetReqTracker() const {
  return f_req_tracker_;
}

void FimSocket::Migrate(IAsioPolicy* policy) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (idle_timer_) {
    boost::shared_ptr<IPeriodicTimer> orig_idle_timer = idle_timer_;
    idle_timer_ = idle_timer_->Duplicate(policy->io_service());
    orig_idle_timer->Stop();
  }
  if (heartbeat_timer_) {
    boost::shared_ptr<IPeriodicTimer> orig_heartbeat_timer = heartbeat_timer_;
    heartbeat_timer_ = heartbeat_timer_->Duplicate(policy->io_service());
    orig_heartbeat_timer->Stop();
  }
  socket_.reset(policy->DuplicateSocket(socket_.get()));
  policy_ = policy;
}

void FimSocket::Reset() {
  MUTEX_LOCK_GUARD(data_mutex_);
  Reset_();
}

void FimSocket::Reset_() {
  header_handler_ = body_handler_ = write_handler_ = SIOHandler();
  idle_timer_.reset();
  heartbeat_timer_.reset();
}

void FimSocket::Shutdown() {
  FimSocketCleanupCallback cleanup_callback;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    // Prevent Shutdown() from being called again by async handlers
    if (shutdown_)
      return;
    atomic_shutdown_ = shutdown_ = true;
    LOG(notice, Server, PVal(*this), " disconnected");
    Reset_();
    cleanup_callback = cleanup_callback_;
    cleanup_callback_ = FimSocketCleanupCallback();
  }
  if (cleanup_callback)
    cleanup_callback();
  IReqTracker* req_tracker = f_req_tracker_.load(boost::memory_order_acquire);
  if (req_tracker && req_tracker->GetFimSocket().get() == this)
    req_tracker->SetFimSocket(boost::shared_ptr<IFimSocket>());
  // Best effort to shutdown the socket, do not throw errors
  boost::system::error_code ec_shutdown, ec_close;
  socket_->shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec_shutdown);
  socket_->close(ec_close);
}

bool FimSocket::ShutdownCalled() {
  return atomic_shutdown_;
}

void FimSocket::OnCleanup(FimSocketCleanupCallback cleanup_callback) {
  MUTEX_LOCK_GUARD(data_mutex_);
  cleanup_callback_ = cleanup_callback;
}

}  // namespace

FimSocketMaker kFimSocketMaker = boost::forward_adapter<
  boost::factory<boost::shared_ptr<FimSocket> > >();

}  // namespace cpfs
