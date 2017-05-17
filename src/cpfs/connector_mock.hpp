#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/shared_ptr.hpp>

#include "connector.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((AsyncConnect, void,                                                 \
    (std::string)(int)(IFimProcessor*)(FimSocketCreatedHandler)(bool)   \
    (double)(ConnectErrorHandler)(IAsioPolicy*)))                       \
  ((Listen, void, (const std::string&)(int)(IFimProcessor*)))           \
  ((SetFimSocketMaker, void, (FimSocketMaker)))                         \
  ((SetConnectingSocketMaker, void, (ConnectingSocketMaker)))           \
  ((SetListeningSocketMaker, void, (ListeningSocketMaker)))             \
  ((set_heartbeat_interval, void, (double)))                            \
  ((set_socket_read_timeout, void, (double)))

class MockIConnector : public IConnector {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
