#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/bind.hpp>

#include "mock_helper.hpp"
#include "posix_fs.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Pread, ssize_t, (int)(void*)(size_t)(off_t)))                       \
  ((Pwrite, ssize_t, (int)(const void*)(size_t)(off_t)))                \
  ((Lsetxattr, ssize_t,                                                 \
    (const char*)(const char*)(const void*)(size_t)(int)))              \
  ((Sync, void, ))

class MockIPosixFS : public IPosixFS {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
