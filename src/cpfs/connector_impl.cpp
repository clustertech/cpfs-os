/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the cpfs::Connector to create FimSocket by making
 * connections with Asio.
 */

#include "connector_impl.hpp"

#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "authenticator.hpp"
#include "connecting_socket.hpp"
#include "connector.hpp"
#include "fim_socket.hpp"
#include "listening_socket.hpp"
#include "periodic_timer.hpp"
#include "periodic_timer_impl.hpp"

namespace cpfs {

class IFimProcessor;

namespace {

/**
 * Arguments needed for calling AsyncConnect
 */
struct ConnectArgs {
  std::string host;
  int port;
  IFimProcessor* fim_processor;
  FimSocketCreatedHandler callback;
  bool monitor;
  double timeout;
  ConnectErrorHandler err_handler;
  IAsioPolicy* asio_policy;
  ConnectArgs(std::string host, int port, IFimProcessor* fim_processor,
              FimSocketCreatedHandler callback, bool monitor, double timeout,
              ConnectErrorHandler err_handler, IAsioPolicy* asio_policy) :
      host(host), port(port), fim_processor(fim_processor), callback(callback),
      monitor(monitor), timeout(timeout), err_handler(err_handler),
      asio_policy(asio_policy) {}
};

/**
 * Connect to the target address, callbacks are called when connected
 * or FIM received.  Each callback has ptr to ConnectParam as the
 * first parameter. It will be kept alive as long as the FimSocket's
 * callbacks is not reset (still pass boost::shared_ptr<ConnectParam>
 * as the first parameter).
 */
class Connector : public IConnector {
 public:
  /**
   * Construct the connector
   *
   * @param asio_policy The IAsioPolicy to perform async IO operations
   *
   * @param authenticator The authenticator for authenticating connections
   *
   * @param monitor_accepted Whether accepted connections should use heartbeat
   */
  Connector(IAsioPolicy* asio_policy, IAuthenticator* authenticator,
            bool monitor_accepted)
      : asio_policy_(asio_policy),
        authenticator_(authenticator),
        monitor_accepted_(monitor_accepted),
        heartbeat_interval_(0),
        socket_read_timeout_(0) {}

  void AsyncConnect(std::string host, int port, IFimProcessor* fim_processor,
                    FimSocketCreatedHandler callback, bool monitor,
                    double timeout, ConnectErrorHandler err_handler,
                    IAsioPolicy* asio_policy) {
    if (!asio_policy)
      asio_policy = asio_policy_;
    TcpEndpoint target_addr(boost::asio::ip::address::from_string(host), port);
    ConnectArgs args(host, port, fim_processor, callback, monitor, timeout,
                     err_handler, asio_policy);
    boost::shared_ptr<IConnectingSocket> connecting_sock =
        connecting_socket_maker_(
            asio_policy,
            target_addr,
            boost::bind(&Connector::HandleConnected, this, _1, args));
    if (timeout > 0)
      connecting_sock->SetTimeout(timeout, err_handler);
    connecting_sock->AsyncConnect();
  }

  void Listen(const std::string& ip, int port,
              IFimProcessor* init_processor) {
    TcpEndpoint bind_addr = TcpEndpoint(
        boost::asio::ip::address::from_string(ip), port);
    listening_socket_ = listening_socket_maker_(asio_policy_);
    listening_socket_->
        Listen(bind_addr,
               boost::bind(&Connector::HandleAccept, this, _1, init_processor));
  }

  void SetFimSocketMaker(FimSocketMaker fim_socket_maker) {
    fim_socket_maker_ = fim_socket_maker;
  }

  void SetConnectingSocketMaker(ConnectingSocketMaker connecting_socket_maker) {
    connecting_socket_maker_ = connecting_socket_maker;
  }

  void SetListeningSocketMaker(ListeningSocketMaker listening_socket_maker) {
    listening_socket_maker_ = listening_socket_maker;
  }

  void set_heartbeat_interval(double heartbeat_interval) {
    heartbeat_interval_ = heartbeat_interval;
  }

  void set_socket_read_timeout(double socket_read_timeout) {
    socket_read_timeout_ = socket_read_timeout;
  }

 protected:
  /**
   * Callback called by ConnectingSocket when connection is made
   *
   * @param socket The socket used
   *
   * @param fim_processor How to handle FIM received
   *
   * @param monitor Whether to use heartbeat on this connection
   *
   * @param callback Callback when connected
   *
   * @param asio_policy The Asio policy to use
   */
  void HandleConnected(TcpSocket* socket, ConnectArgs args) {
    IAsioPolicy* asio_policy = args.asio_policy;
    boost::shared_ptr<IFimSocket> fim_socket =
        fim_socket_maker_(socket, asio_policy);
    if (args.monitor) {
      fim_socket->SetHeartbeatTimer(
          kPeriodicTimerMaker(asio_policy->io_service(), heartbeat_interval_));
      fim_socket->SetIdleTimer(
          kPeriodicTimerMaker(asio_policy->io_service(), socket_read_timeout_));
    }
    fim_socket->OnCleanup(
        boost::bind(&Connector::AsyncConnectArgs, this, args));
    authenticator_->AsyncAuth(fim_socket, boost::bind(
        &Connector::AuthCompleted, this,
        fim_socket, args.fim_processor, args.callback), true);
  }

  void AsyncConnectArgs(ConnectArgs args) {
    AsyncConnect(args.host, args.port, args.fim_processor, args.callback,
                 args.monitor, args.timeout, args.err_handler,
                 args.asio_policy);
  }

  /**
   * Callback called by ListeningSocket when connection is accepted
   *
   * @param socket The socket created
   *
   * @param init_processor The processor to use
   */
  void HandleAccept(TcpSocket* socket, IFimProcessor* init_processor) {
    boost::shared_ptr<IFimSocket> fim_socket =
        fim_socket_maker_(socket, asio_policy_);
    if (monitor_accepted_) {
      fim_socket->SetHeartbeatTimer(
          kPeriodicTimerMaker(asio_policy_->io_service(), heartbeat_interval_));
      fim_socket->SetIdleTimer(
          kPeriodicTimerMaker(asio_policy_->io_service(),
                              socket_read_timeout_));
    }
    authenticator_->AsyncAuth(fim_socket, boost::bind(
        &Connector::HandleAcceptAuthed, this, fim_socket, init_processor),
        false);
  }

  /**
   * Callback called by authenticator socket connect is authenticated
   *
   * @param fim_socket The FimSocket authenticated
   *
   * @param fim_processor How to handle FIM received
   *
   * @param callback Callback when connected
   */
  void AuthCompleted(boost::shared_ptr<IFimSocket> fim_socket,
                     IFimProcessor* fim_processor,
                     FimSocketCreatedHandler callback) {
    fim_socket->OnCleanup(FimSocketCleanupCallback());
    if (fim_processor)
      fim_socket->SetFimProcessor(fim_processor);
    if (callback)
      callback(fim_socket);
  }

  /**
   * Callback called by authenticator when socket accept is authenticated
   *
   * @param fim_socket The FimSocket authenticated
   *
   * @param init_processor The processor to use
   */
  void HandleAcceptAuthed(boost::shared_ptr<IFimSocket> fim_socket,
                          IFimProcessor* init_processor) {
    fim_socket->SetFimProcessor(init_processor);
  }

 private:
  IAsioPolicy* asio_policy_; /**< How to do async IO operations */
  IAuthenticator* authenticator_;
  bool monitor_accepted_;
  /** How to make ConnectingSocket */
  ConnectingSocketMaker connecting_socket_maker_;
  /** How to make ListeningSocket */
  ListeningSocketMaker listening_socket_maker_;
  boost::shared_ptr<IListeningSocket> listening_socket_;
  FimSocketMaker fim_socket_maker_; /**< How to create FimSocket's */
  double heartbeat_interval_;
  double socket_read_timeout_;
};

}  // namespace

IConnector* MakeConnector(
    IAsioPolicy* asio_policy,
    IAuthenticator* authenticator,
    bool monitor_accepted) {
  return new Connector(asio_policy, authenticator, monitor_accepted);
}

}  // namespace cpfs
