/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define policies for async operations.
 */
#include "asio_policy_impl.hpp"

#include <unistd.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>

#include <boost/function.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "util.hpp"

namespace cpfs {

namespace {

/**
 * AsioPolicy implements the IAsioPolicy interface.
 */
class AsioPolicy : public IAsioPolicy {
 public:
  IOService* io_service() {
    return &io_service_;
  }

  void Post(ServiceTask task) {
    io_service_.post(task);
  }

  bool SocketIsOpen(TcpSocket* socket) {
    return socket->is_open();
  }

  void SetTcpNoDelay(TcpSocket* socket) {
    boost::asio::ip::tcp::no_delay option(true);
    socket->set_option(option);
  }

  TcpEndpoint GetRemoteEndpoint(TcpSocket* socket) {
    return socket->remote_endpoint();
  }

  TcpEndpoint GetLocalEndpoint(TcpSocket* socket) {
    return socket->local_endpoint();
  }

  TcpSocket* DuplicateSocket(TcpSocket* socket) {
    return new TcpSocket(
        io_service_,
        socket->local_endpoint().protocol(),
        dup(socket->native_handle()));
  }

  void AsyncRead(TcpSocket* socket, const MIOBufs& buf, SIOHandler handler) {
    boost::asio::async_read(*socket, buf, handler);
  }

  void AsyncWrite(TcpSocket* socket, const CIOBufs& buf, SIOHandler handler) {
    boost::asio::async_write(*socket, buf, handler);
  }

  TcpEndpoint TcpResolve(const char* host, const char* service) {
    using boost::asio::ip::tcp;
    tcp::resolver::iterator iterator = tcp::resolver(io_service_).resolve(
        tcp::resolver::query(tcp::v4(), host, service));
    return *iterator;
  }

  void ParseHostPort(const std::string& hostPort,
                     std::string* host_ret, int* port_ret) {
    std::size_t pos = hostPort.find(":");
    if (pos == std::string::npos)
      throw std::runtime_error("Malformed host and port: " + hostPort);
    TcpEndpoint ep = TcpResolve(hostPort.substr(0, pos).c_str(),
                                hostPort.substr(pos + 1).c_str());
    *host_ret = ep.address().to_string();
    *port_ret = ep.port();
  }

  TcpAcceptor* MakeAcceptor(const TcpEndpoint& bind_addr) {
    UNIQUE_PTR<TcpAcceptor> acceptor(new TcpAcceptor(io_service_, bind_addr));
    boost::asio::socket_base::reuse_address option(true);
    acceptor->set_option(option);
    return acceptor.release();
  }

  void AsyncAccept(TcpAcceptor* acceptor,
                   TcpSocket* socket, IOHandler handler) {
    acceptor->async_accept(*socket, handler);
  }

  void AsyncConnect(TcpSocket* socket,
                    TcpEndpoint endpoint, IOHandler handler) {
    socket->async_connect(endpoint, handler);
  }

  DeadlineTimer* MakeDeadlineTimer() {
    return new DeadlineTimer(io_service_);
  }

  void SetDeadlineTimer(DeadlineTimer* timer, double timeout,
                        IOHandler handler) {
    if (timeout) {
      timer->expires_from_now(ToTimeDuration(timeout));
      timer->async_wait(handler);
    } else {
      timer->cancel();
    }
  }

  SignalSet* MakeSignalSet() {
    return new SignalSet(io_service_);
  }

  void SetSignalHandler(SignalSet* signal_set,
                        const int signals[], unsigned num_signals,
                        SignalHandler handler) {
    signal_set->cancel();
    signal_set->clear();
    if (handler) {
      for (unsigned i = 0; i < num_signals; ++i)
        signal_set->add(signals[i]);
      signal_set->async_wait(handler);
    }
  }

 private:
  IOService io_service_;
};

}  // namespace

/**
 * Create an Asio policy.
 *
 * @return The created policy
 */
IAsioPolicy* MakeAsioPolicy() {
  return new AsioPolicy();
}

}  // namespace cpfs
