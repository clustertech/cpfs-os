#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "listening_socket.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Listen, void, (const TcpEndpoint&)(AcceptCallback)))

class MockIListeningSocket : public IListeningSocket {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Make, boost::shared_ptr<IListeningSocket>,                          \
    (IAsioPolicy*)))

class MockListeningSocketMaker {
  MAKE_MOCK_METHODS(OBJ_METHODS);

  ListeningSocketMaker GetMaker() {
    return boost::bind(&MockListeningSocketMaker::Make, this, _1);
  }
};

#undef OBJ_METHODS

}  // namespace cpfs
