#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include "time_keeper.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Start, void,))                                                      \
  ((Stop, void,))                                                       \
  ((Update, bool,))                                                     \
  ((GetLastUpdate, uint64_t,, CONST))

class MockITimeKeeper : public ITimeKeeper {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
