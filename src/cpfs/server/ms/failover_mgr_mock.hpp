#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/failover_mgr.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((SetPeriodicTimerMaker, void, (PeriodicTimerMaker)))                 \
  ((Start, void, (double)))                                             \
  ((AddReconfirmDone, void, (ClientNum)))

class MockIFailoverMgr : public IFailoverMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
