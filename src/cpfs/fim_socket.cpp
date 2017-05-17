/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement common functions for the IFimSocket interface.
 */

#include "fim_socket.hpp"

#include <ostream>
#include <string>

namespace cpfs {

std::ostream& operator<<(std::ostream& ost, const IFimSocket& socket) {
  if (!socket.name().empty())
    ost << socket.name();
  else
    ost << socket.remote_info();
  return ost;
}

}  // namespace cpfs
