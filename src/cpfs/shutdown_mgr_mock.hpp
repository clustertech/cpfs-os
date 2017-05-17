#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include <boost/function.hpp>

#include "mock_helper.hpp"
#include "shutdown_mgr.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((SetAsioPolicy, void, (IAsioPolicy*)))                               \
  ((SetupSignals, void, (const int[])(unsigned)))                       \
  ((Init, bool, (double)))                                              \
  ((Shutdown, bool,))                                                   \
  ((inited, bool,, CONST))                                              \
  ((shutting_down, bool,, CONST))

class MockIShutdownMgr : public IShutdownMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
