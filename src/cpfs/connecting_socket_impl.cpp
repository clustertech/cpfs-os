/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IConnectingSocket interface for connection
 * initiation.
 */

#include "connecting_socket_impl.hpp"

#include <memory>

#include <boost/bind.hpp>
#include <boost/chrono/duration.hpp>
#include <boost/chrono/system_clocks.hpp>
#include <boost/chrono/time_point.hpp>
#include <boost/function.hpp>
#include <boost/functional/factory.hpp>
#include <boost/functional/forward_adapter.hpp>
#include <boost/ratio/ratio.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "connecting_socket.hpp"
#include "logger.hpp"

namespace cpfs {

namespace {

/**
 * ConnectingSocket class for initiating connection.  Handler is
 * invoked when connection succeeds or fails.
 */
class ConnectingSocket : public IConnectingSocket {
 public:
  /**
   * Create the ConnectingSocket that connects to target endpoint
   *
   * @param asio_policy The asio policy to use
   *
   * @param target_addr The peer endpoint to connect to
   *
   * @param success_handler How to handle successful connections
   */
  ConnectingSocket(IAsioPolicy* asio_policy,
                   const TcpEndpoint& target_addr,
                   ConnectSuccessHandler success_handler)
      : asio_policy_(asio_policy),
        target_addr_(target_addr),
        success_handler_(success_handler),
        timeout_(0),
        error_handler_(&DefaultConnectErrorHandler),
        connected_(false),
        timer_(*asio_policy->io_service()) {}

  void AsyncConnect() {
    connected_ = false;
    connect_start_ = boost::chrono::steady_clock::now();
    DoConnect();
  }

  void SetTimeout(double timeout, ConnectErrorHandler handler) {
    timeout_ = timeout;
    error_handler_ = handler;
    if (socket_.get() && !connected_)
      SetErrorTimer();
  }

  bool connected() const {
    return connected_;
  }

  double AttemptElapsed() {
    boost::chrono::duration<double> elapsed =
        boost::chrono::steady_clock::now() - attempt_start_;
    return elapsed.count();
  }

  double TotalElapsed() {
    boost::chrono::duration<double> elapsed =
        boost::chrono::steady_clock::now() - connect_start_;
    return elapsed.count();
  }

 protected:
  /**
   * Connect to the endpoint. When error occurrs, connection is attempted
   * again
   */
  void DoConnect() {
    socket_.reset(new TcpSocket(*asio_policy_->io_service()));
    attempt_start_ = boost::chrono::steady_clock::now();
    asio_policy_->AsyncConnect(
        socket_.get(),
        target_addr_,
        boost::bind(&ConnectingSocket::ConnectionCompleted,
                    GetShared(),
                    boost::asio::placeholders::error));
    SetErrorTimer();
  }

  /**
   * Handling connection completion.
   */
  void ConnectionCompleted(const boost::system::error_code& error) {
    asio_policy_->SetDeadlineTimer(&timer_, 0, IOHandler());
    if (!error) {  // All done
      connected_ = true;
      success_handler_(socket_.release());
    } else {
      LOG(error, Fim, "Error in connection attempt to ",
          PVal(target_addr_), ". ", error.message());
      double reconnect_timeout = error_handler_(GetShared(), error);
      socket_.reset();
      if (reconnect_timeout > 0)
        asio_policy_->SetDeadlineTimer(
            &timer_,
            reconnect_timeout,
            boost::bind(&ConnectingSocket::DoReconnect,
                        GetShared(),
                        boost::asio::placeholders::error));
      else if (reconnect_timeout == 0)
        DoReconnect(boost::system::error_code());
    }
  }

  /**
   * Retry a failed connection.
   */
  void DoReconnect(const boost::system::error_code& error) {
    if (!error)
      DoConnect();
    // Otherwise, it is supposedly a timer cancellation
  }

  /**
   * Set the timer when Connect() is pending.
   */
  void SetErrorTimer() {
    if (timeout_) {
      asio_policy_->SetDeadlineTimer(
          &timer_, timeout_,
          boost::bind(&ConnectingSocket::TimeoutTriggered,
                      GetShared(),
                      boost::asio::placeholders::error));
    } else {
      asio_policy_->SetDeadlineTimer(&timer_, 0, IOHandler());
    }
  }

  /**
   * Handle connection timeout. Reset timers and close the socket.
   */
  void TimeoutTriggered(const boost::system::error_code& error) {
    if (!error) {
      boost::system::error_code ec_shutdown, ec_close;
      socket_->shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                        ec_shutdown);
      socket_->close(ec_close);
      ConnectionCompleted(boost::asio::error::timed_out);
    }  // Otherwise, supposedly it is a timer cancel, and can be ignored
  }

 private:
  IAsioPolicy* asio_policy_;
  TcpEndpoint target_addr_;
  ConnectSuccessHandler success_handler_;
  double timeout_;
  ConnectErrorHandler error_handler_;
  std::auto_ptr<TcpSocket> socket_;
  bool connected_;
  DeadlineTimer timer_;
  boost::chrono::time_point<boost::chrono::steady_clock> connect_start_;
  boost::chrono::time_point<boost::chrono::steady_clock> attempt_start_;

  boost::shared_ptr<ConnectingSocket> GetShared() {
    return boost::static_pointer_cast<ConnectingSocket>(
        shared_from_this());
  }
};

}  // namespace

ConnectingSocketMaker kConnectingSocketMaker = boost::forward_adapter<
  boost::factory<boost::shared_ptr<ConnectingSocket> > >();

}  // namespace cpfs
