#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include <string>
#include <vector>

#include "mock_helper.hpp"
#include "tracer.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Log, void, (const char*)(InodeNum)(ClientNum)))                     \
  ((DumpAll, std::vector<std::string>,, CONST))

class MockITracer : public ITracer {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
