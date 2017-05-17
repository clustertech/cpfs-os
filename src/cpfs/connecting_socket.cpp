/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement common functions for the IConnectingSocket interface.
 */

#include "connecting_socket.hpp"

#include <boost/shared_ptr.hpp>

namespace cpfs {

double DefaultConnectErrorHandler(boost::shared_ptr<IConnectingSocket> socket,
                                  const boost::system::error_code& error) {
  (void) socket;
  (void) error;
  return 1.0;
}

}  // namespace cpfs
