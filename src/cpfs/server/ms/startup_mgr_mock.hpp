#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/startup_mgr.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((SetPeriodicTimerMaker, void, (PeriodicTimerMaker)))                 \
  ((Init, void,))                                                       \
  ((Reset, void, (const char*)))                                        \
  ((dsg_degraded, bool, (GroupId)(GroupRole*)))                         \
  ((set_dsg_degraded, void, (GroupId)(bool)(GroupRole)))

class MockIStartupMgr : public IStartupMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
