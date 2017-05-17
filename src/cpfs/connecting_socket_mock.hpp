#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "connecting_socket.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((AsyncConnect, void,))                                               \
  ((SetTimeout, void, (double)(ConnectErrorHandler)))                   \
  ((connected, bool,, CONST))                                           \
  ((AttemptElapsed, double,))                                           \
  ((TotalElapsed, double,))

class MockIConnectingSocket : public IConnectingSocket {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Make, boost::shared_ptr<IConnectingSocket>,                         \
    (IAsioPolicy*)(const TcpEndpoint&)(ConnectSuccessHandler)))

class MockConnectingSocketMaker {
  MAKE_MOCK_METHODS(OBJ_METHODS);

  ConnectingSocketMaker GetMaker() {
    return boost::bind(&MockConnectingSocketMaker::Make, this, _1, _2, _3);
  }
};

#undef OBJ_METHODS

}  // namespace cpfs
