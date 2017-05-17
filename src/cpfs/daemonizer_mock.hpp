#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include "daemonizer.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Daemonize, void,))

class MockIDaemonizer : public IDaemonizer {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
