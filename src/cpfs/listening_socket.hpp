#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::ListeningSocket for meta / data server
 */
#include <boost/function.hpp>

#include "asio_common.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IAsioPolicy;

/**
 * Function type of the callback called when a connection is accepted.
 */
typedef boost::function<void(TcpSocket* sock)> AcceptCallback;

/**
 * Handle the Asio procedures for accepting connections.
 */
class IListeningSocket {
 public:
  virtual ~IListeningSocket() {}

  /**
   * Listen for connections
   *
   * @param bind_addr The address to bind to for others to connect
   *
   * @param callback The callback to run upon successful connection
   */
  virtual void Listen(const TcpEndpoint& bind_addr,
                      AcceptCallback callback) = 0;
  // N.B.: This should be called OnConnection under our naming
  // convention, but because everybody else call it listen, following
  // the convention does not make things clearer.
};

/** Type for ListeningSocket maker */
typedef boost::function<
  boost::shared_ptr<IListeningSocket>(IAsioPolicy* policy)
> ListeningSocketMaker;

}  // namespace cpfs
