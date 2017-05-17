#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include "dir_iterator.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((GetNext, bool, (std::string*)(bool*)(struct stat*)))                \
  ((SetFilterCTime, void, (uint64_t)))                                  \
  ((missing, bool,, CONST))

class MockIDirIterator : public IDirIterator {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
