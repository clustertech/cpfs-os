#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "io_service_runner.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((AddService, void, (IOService*)))                                    \
  ((Run, void,))                                                        \
  ((Stop, void,))                                                       \
  ((Join, void,))

class MockIOServiceRunner : public IIOServiceRunner {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
