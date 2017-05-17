/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of ThreadFimProcessor.
 */

#include "thread_fim_processor_impl.hpp"

#include <deque>
#include <exception>
#include <utility>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/functional/factory.hpp>
#include <boost/functional/forward_adapter.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/thread.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_processor.hpp"
#include "fim_processor_base.hpp"
#include "fim_socket.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_tracker.hpp"
#include "thread_fim_processor.hpp"

namespace cpfs {

namespace {

/**
 * Implement the IThreadFimProcessor interface.
 */
class ThreadFimProcessor : public IThreadFimProcessor, boost::noncopyable {
 public:
  /**
   * @param processor The FimProcessor to call within the thread. Its
   * ownership is transferred to the ThreadFimProcessor after the
   * constructor
   */
  explicit ThreadFimProcessor(IFimProcessor* processor)
      : int_processor_(processor), data_mutex_(MUTEX_INIT), to_stop(false) {}

  ~ThreadFimProcessor() {
    Stop();
    Join();
  }

  void Start();
  void Stop();
  void Join();
  bool Accept(const FIM_PTR<IFim>& fim) const;
  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& sender);
  bool SocketPending(const boost::shared_ptr<IFimSocket>& sender) const;

 private:
  typedef std::pair<
    FIM_PTR<IFim>,
    boost::shared_ptr<IFimSocket>
  > EltType;

  boost::scoped_ptr<IFimProcessor> int_processor_;
  boost::scoped_ptr<boost::thread> thread_; /**< The running thread */
  mutable MUTEX_TYPE data_mutex_; /**< Protect the whole thread structure */
  boost::condition_variable wake_; /**< Wake up main loop */
  bool to_stop; /**< Whether stop is requested */
  std::deque<EltType> server_queue_; /**< The server queue */
  std::deque<EltType> client_queue_; /**< The client queue */

  void Run();
  void HandleFim(const FIM_PTR<IFim>& fim,
                 const boost::shared_ptr<IFimSocket>& sender);
};

void ThreadFimProcessor::Start() {
  to_stop = false;
  if (thread_)
    return;
  thread_.reset(new boost::thread(boost::bind(&ThreadFimProcessor::Run, this)));
}

void ThreadFimProcessor::Stop() {
  MUTEX_LOCK_GUARD(data_mutex_);
  to_stop = true;
  wake_.notify_all();
}

void ThreadFimProcessor::Join() {
  if (!thread_)
    return;
  thread_->join();
  thread_.reset();
}

void ThreadFimProcessor::Run() {
  for (;;) {  // Each iteration handle one Fim
    EltType entry;
    bool is_server;
    {
      MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
      while (!to_stop && server_queue_.empty() && client_queue_.empty())
        MUTEX_WAIT(lock, wake_);
      if (to_stop)
        break;
      is_server = !server_queue_.empty();
      if (is_server)
        entry = server_queue_.front();
      else
        entry = client_queue_.front();
    }  // Give up mutex here so that EnqueueFim() and Stop() can run
    LOG(debug, Fim, "*p ", PVal(entry.first));
    HandleFim(entry.first, entry.second);
    {
      MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
      (is_server ? server_queue_ : client_queue_).pop_front();
    }
    LOG(debug, Fim, "** ", PVal(entry.first));
  }
}

void ThreadFimProcessor::HandleFim(
    const FIM_PTR<IFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  bool ret;
  try {
    // Fims from killed sockets may have been in the queue for a long
    // time.  Processing them is dangerous.
    if (sender && sender->ShutdownCalled())
      return;
    ret = int_processor_->Process(fim, sender);
  } catch (const std::exception& ex) {
    LOG(error, Fim, "Exception processing Fim ", PVal(fim),
        ": ", ex.what());
    return;
  }
  if (!ret)
    LOG(warning, Fim, "Unprocessed Fim ", PVal(fim));
}

bool ThreadFimProcessor::Accept(const FIM_PTR<IFim>& fim) const {
  return int_processor_->Accept(fim);
}

bool ThreadFimProcessor::Process(
    const FIM_PTR<IFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (!Accept(fim))
    return false;
  bool is_server = true;
  if (sender) {
    IReqTracker* tracker = sender->GetReqTracker();
    if (tracker && tracker->peer_client_num() != kNotClient)
      is_server = false;
  }
  if (is_server)
    server_queue_.push_back(std::make_pair(fim, sender));
  else
    client_queue_.push_back(std::make_pair(fim, sender));
  wake_.notify_all();
  return true;
}

bool ThreadFimProcessor::SocketPending(
    const boost::shared_ptr<IFimSocket>& sender) const {
  MUTEX_LOCK_GUARD(data_mutex_);
  BOOST_FOREACH(const EltType& elt, server_queue_) {
    if (elt.second == sender)
      return true;
  }
  BOOST_FOREACH(const EltType& elt, client_queue_) {
    if (elt.second == sender)
      return true;
  }
  return false;
}

/**
 * IThreadFimProcessor implementing BaseFimProcessor functionality
 */
class BasicThreadFimProcessor : public ThreadFimProcessor {
 public:
  /**
   * @param processor The worker processor to run
   */
  explicit BasicThreadFimProcessor(IFimProcessor* processor)
      : ThreadFimProcessor(processor) {}

  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket) {
    if (base_fim_processor_.Process(fim, socket))
      return true;
    return ThreadFimProcessor::Process(fim, socket);
  }

 private:
  BaseFimProcessor base_fim_processor_;
};

}  // namespace

ThreadFimProcessorMaker kThreadFimProcessorMaker = boost::forward_adapter<
  boost::factory<ThreadFimProcessor*> >();
ThreadFimProcessorMaker kBasicThreadFimProcessorMaker = boost::forward_adapter<
  boost::factory<BasicThreadFimProcessor*> >();

}  // namespace cpfs
