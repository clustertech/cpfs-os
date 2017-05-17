#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "service.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((Run, int,))                                                         \
  ((Shutdown, void,))

class MockIService : public IService {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
