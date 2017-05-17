#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::Connector to create FimSocket by making or accepting
 * connections with Asio.
 */
#include <string>

#include <boost/function.hpp>

#include "asio_common.hpp"
#include "connecting_socket.hpp"
#include "fim_socket.hpp"
#include "listening_socket.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimProcessor;
class IFimSocket;

/**
 * Callback function for Connector on connection completed.
 */
typedef boost::function<void(boost::shared_ptr<IFimSocket>)>
FimSocketCreatedHandler;

/**
 * Interface for the Connector
 */
class IConnector {
 public:
  virtual ~IConnector() {}

  /**
   * Connecting to target with options given in op
   *
   * @param host The target IP
   *
   * @param port The target port
   *
   * @param fim_processor How to handle FIM received
   *
   * @param handler Handler when the Fim socket is created
   *
   * @param monitor Whether to use heartbeat on this connection
   *
   * @param timeout Timeout for connection attempt
   *
   * @param err_handler Error handler for connection
   *
   * @param asio_policy If non-null, override default asio policy
   */
  virtual void AsyncConnect(
      std::string host, int port,
      IFimProcessor* fim_processor,
      FimSocketCreatedHandler handler,
      bool monitor = true,
      double timeout = 0,
      ConnectErrorHandler err_handler = &DefaultConnectErrorHandler,
      IAsioPolicy* asio_policy = 0) = 0;

  /**
   * Listen to incoming connections
   *
   * @param ip The IP address to listen to
   *
   * @param port The port to listen to
   *
   * @param init_processor The processor to use when connection is accepted
   */
  virtual void Listen(const std::string& ip, int port,
                      IFimProcessor* init_processor) = 0;

  /**
   * Set how to create Fim sockets.  Must be set before calling Connect().
   *
   * @param fim_socket_maker The FimSocketMaker to use
   */
  virtual void SetFimSocketMaker(FimSocketMaker fim_socket_maker) = 0;

  /**
   * Set how to create connecting sockets.  Must be set before calling
   * Connect().
   *
   * @param connecting_socket_maker The ConnectingSocketMaker to use
   */
  virtual void SetConnectingSocketMaker(
      ConnectingSocketMaker connecting_socket_maker) = 0;

  /**
   * Set how to create listening sockets.  Must be set before calling
   * Listen().
   *
   * @param listening_socket_maker The ListeningSocketMaker to use
   */
  virtual void SetListeningSocketMaker(
      ListeningSocketMaker listening_socket_maker) = 0;

  /**
   * Set the heartbeat interval
   *
   * @param heartbeat_interval The heartbeat interval in seconds
   */
  virtual void set_heartbeat_interval(double heartbeat_interval) = 0;

  /**
   * Set the socket read timeout
   *
   * @param socket_read_timeout The socket read timeout in seconds
   */
  virtual void set_socket_read_timeout(double socket_read_timeout) = 0;
};

}  // namespace cpfs
