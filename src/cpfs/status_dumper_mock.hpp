#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "status_dumper.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Dump, void,))

class MockIStatusDumpable : public IStatusDumpable {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((SetAsioPolicy, void, (IAsioPolicy*)))

class MockIStatusDumper : public IStatusDumper {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
