#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define policies for async operations.
 */

#include <cstddef>
#include <string>

#include <boost/function.hpp>

#include "asio_common.hpp"

namespace cpfs {

/**
 * Type for buffers for AsyncRead()
 */
typedef boost::asio::mutable_buffers_1 MIOBufs;

/**
 * Type for buffers for AsyncWrite()
 */
typedef boost::asio::const_buffers_1 CIOBufs;

/**
 * Type for tasks for Post()
 */
typedef boost::function<void()> ServiceTask;

/**
 * Type for data handlers.
 */
typedef boost::function <void(const boost::system::error_code&,
                              std::size_t)> SIOHandler;

/**
 * Type for connection and accept handlers.
 */
typedef boost::function <void(const boost::system::error_code&)> IOHandler;

/**
 * Type for signal handler
 */
typedef boost::function <void(const boost::system::error_code& error,
                              int signal_number)> SignalHandler;

/**
 * IAsioPolicy is an interface for async operations using an
 * IOService.  Each policy is associated with its own IOService, which
 * can be obtained using io_service().
 */
class IAsioPolicy {
 public:
  virtual ~IAsioPolicy() {}

  /**
   * Get the associated IO service.
   */
  virtual IOService* io_service() = 0;

  /**
   * Post task to be done by IO service.
   *
   * @param task The task
   */
  virtual void Post(ServiceTask task) = 0;

  /**
   * Check whether a socket is open.
   *
   * @param socket The socket to check
   *
   * @return Whether the socket is open
   */
  virtual bool SocketIsOpen(TcpSocket* socket) = 0;

  /**
   * Set TCP no delay option for a socket.
   *
   * @param socket The socket to set the option
   */
  virtual void SetTcpNoDelay(TcpSocket* socket) = 0;

  /**
   * Get the remote end-point of a socket.
   *
   * @param socket The socket
   */
  virtual TcpEndpoint GetRemoteEndpoint(TcpSocket* socket) = 0;

  /**
   * Get the local end-point of a socket.
   *
   * @param socket The socket
   */
  virtual TcpEndpoint GetLocalEndpoint(TcpSocket* socket) = 0;

  /**
   * Duplicate a TCP socket from a different policy to this policy.
   * As Asio would not expect that two file handles are actually
   * referring to the same file object, the caller should probably
   * ensure that the other policy no longer need the original socket,
   * i.e., no operation is pending or will be issued for it.
   *
   * @param socket The socket to duplicate
   *
   * @return The duplicate
   */
  virtual TcpSocket* DuplicateSocket(TcpSocket* socket) = 0;

  /**
   * Asynchronously read data to a buffer.
   *
   * @param socket The socket to read data from
   *
   * @param buf The buffer to hold the data received
   *
   * @param handler The callback to call once operation is completed
   */
  virtual void AsyncRead(TcpSocket* socket,
                         const MIOBufs& buf, SIOHandler handler) = 0;

  /**
   * Asynchronously write data to a socket.
   *
   * @param socket The socket to write data to
   *
   * @param buf The buffer containing the data
   *
   * @param handler The callback to call once operation is completed
   */
  virtual void AsyncWrite(TcpSocket* socket,
                          const CIOBufs& buf, SIOHandler handler) = 0;

  /**
   * Resolve names for TCP and get the first entry of the result.
   *
   * @param host The host to connect to
   *
   * @param service The service port to connect to
   */
  virtual TcpEndpoint TcpResolve(const char* host, const char* service) = 0;

  /**
   * Parse host:port into IP and port number.
   *
   * @param hostPort The host:port string
   *
   * @param host_ret Where to return the host IP
   *
   * @param port_ret Where to return the port number
   */
  virtual void ParseHostPort(
      const std::string& hostPort, std::string* host_ret, int* port_ret) = 0;

  /**
   * Create an acceptor binding (listening) to a local address.
   *
   * @param bind_addr The bind address
   *
   * @return An acceptor
   */
  virtual TcpAcceptor* MakeAcceptor(const TcpEndpoint& bind_addr) = 0;

  /**
   * Asynchronously accept a connection.
   *
   * @param acceptor The acceptor being used
   *
   * @param socket The socket to accept the connection from
   *
   * @param handler The callback to call once operation is completed
   */
  virtual void AsyncAccept(TcpAcceptor* acceptor,
                           TcpSocket* socket,
                           IOHandler handler) = 0;

  /**
   * Asynchronously connect to a server.
   *
   * @param socket The socket for making a connection
   *
   * @param endpoint The endpoint to connect to
   *
   * @param handler The callback to call once operation is completed
  */
  virtual void AsyncConnect(TcpSocket* socket,
                            TcpEndpoint endpoint, IOHandler handler) = 0;

  /**
   * @return A new deadline timer using the embedded IOService
   */
  virtual DeadlineTimer* MakeDeadlineTimer() = 0;

  /**
   * Set a deadline timer to run certain event upon expiry.
   *
   * @param timer The timer to set
   *
   * @param timeout The number of seconds before expiry.  If not
   * positive, cancel the timer.  In this case handler is ignored
   *
   * @param handler What to call upon timer expiry
   */
  virtual void SetDeadlineTimer(DeadlineTimer* timer, double timeout,
                                IOHandler handler) = 0;

  /**
   * @return A new signal set using the embedded IOService
   */
  virtual SignalSet* MakeSignalSet() = 0;

  /**
   * Set handler for a signal set.  Previously set handlers are
   * cancelled.  Like the boost::signal_set that this wraps, the
   * handler (if provided) will be called exactly once and is unset
   * afterwards.
   *
   * @param signal_set The signal set to configure
   *
   * @param signals Signals to handle (needed only if non-empty
   * handler is provided)
   *
   * @param num_signals Size of the signals array
   *
   * @param handler The signal handler.  If empty, do not set a new
   * handler (only unset the old one)
   */
  virtual void SetSignalHandler(SignalSet* signal_set,
                                const int signals[], unsigned num_signals,
                                SignalHandler handler) = 0;
};

}  // namespace cpfs
