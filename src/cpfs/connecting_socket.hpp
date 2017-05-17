#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the IConnectingSocket interface for connection initiation.
 */

#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>

#include "asio_common.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IAsioPolicy;
class IConnectingSocket;

/**
 * Function type for connecting socket on successful connection.
 */
typedef boost::function<void(TcpSocket* sock)> ConnectSuccessHandler;

/**
 * Handler function for connection error.  It is called with the
 * connecting socket as argument.  The function should return a number
 * indicating the retry strategy: returning a non-negative number
 * means to retry after that many seconds, and -1 means to give up.
 */
typedef boost::function<double(boost::shared_ptr<IConnectingSocket>,
                               const boost::system::error_code& error)>
ConnectErrorHandler;

/**
 * The default connect error handler.  It simply returns 1.0.
 */
double DefaultConnectErrorHandler(boost::shared_ptr<IConnectingSocket>,
                                  const boost::system::error_code& error);

/**
 * The interface for ConnectingSocket.  Note that such connecting
 * socket is not thread safe: only the IO service thread used to
 * create the socket is expected to call the member functions.
 */
class IConnectingSocket
    : public boost::enable_shared_from_this<IConnectingSocket> {
 public:
  virtual ~IConnectingSocket() {}

  /**
   * Start connecting to remote endpoint.  This can be called multiple
   * times provided that the previously established connection has
   * already been closed.
   */
  virtual void AsyncConnect() = 0;

  /**
   * Set timeout and error handler.  The default corresponds to having
   * timeout = 0 and handler = DefaultConnectErrorHandler.  This can
   * be called either before Connect() is first called, or before a
   * pending Connect() is completed.  In the latter case, the current
   * timeout is changed to timeout seconds from now.
   *
   * @param timeout Timeout in seconds (unless TCP error occurs).  If
   * 0, never timeout, and connection will fail only if the underlying
   * TCP returns failure
   *
   * @param handler Handler to invoke when timeout expired
   */
  virtual void SetTimeout(double timeout,
                          ConnectErrorHandler handler) = 0;

  /**
   * @return Whether the connection has been established
   */
  virtual bool connected() const = 0;

  /**
   * @return Number of seconds since the last connection attempt is
   * tried.  This might be useful for writing error handlers
   */
  virtual double AttemptElapsed() = 0;

  /**
   * @return Number of seconds since the first Connect() is initially
   * called.  This might be useful for writing error handlers
   */
  virtual double TotalElapsed() = 0;
};

/**
 * Type for changing the behavior of creating IConnectingSocket
 * instances.  The first argument is the IAsioPolicy object used to
 * perform async IO operations.  The second argument is the peer
 * endpoint to connect to.  The third argument is the handler to call
 * once connected.
 */
typedef boost::function<
  boost::shared_ptr<IConnectingSocket>(
      IAsioPolicy* policy,
      const TcpEndpoint& target_addr,
      ConnectSuccessHandler success_handler)
> ConnectingSocketMaker;

}  // namespace cpfs
