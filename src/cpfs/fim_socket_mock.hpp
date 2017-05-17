#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/bind.hpp>

#include "fim_socket.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((asio_policy, IAsioPolicy*,, CONST))                                 \
  ((socket, TcpSocket*,, CONST))                                        \
  ((name, std::string,, CONST))                                         \
  ((remote_info, std::string,, CONST))                                  \
  ((SetFimProcessor, void, (IFimProcessor*)))                           \
  ((StartRead, void,))                                                  \
  ((SetIdleTimer, void, (const boost::shared_ptr<IPeriodicTimer>&)))    \
  ((WriteMsg, void, (const FIM_PTR<IFim>&)))                            \
  ((pending_write, bool,, CONST))                                       \
  ((SetHeartbeatTimer, void,                                            \
    (const boost::shared_ptr<IPeriodicTimer>&)))                        \
  ((SetReqTracker, void, (const boost::shared_ptr<IReqTracker>&)))      \
  ((GetReqTracker, IReqTracker*,, CONST))                               \
  ((Migrate, void, (IAsioPolicy*)))                                     \
  ((Reset, void,))                                                      \
  ((Shutdown, void,))                                                   \
  ((ShutdownCalled, bool,))                                             \
  ((OnCleanup, void, (FimSocketCleanupCallback)))

class MockIFimSocket : public IFimSocket {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Make, boost::shared_ptr<IFimSocket>, (TcpSocket*)(IAsioPolicy*)))

class MockFimSocketMaker {
  MAKE_MOCK_METHODS(OBJ_METHODS);

  FimSocketMaker GetMaker() {
    return boost::bind(&MockFimSocketMaker::Make, this, _1, _2);
  }
};

#undef OBJ_METHODS

}  // namespace cpfs
