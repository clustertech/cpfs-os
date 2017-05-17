#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "client/cpfs_admin.hpp"  // IWYU pragma: export

namespace cpfs {
namespace client {

#define OBJ_METHODS

class MockClusterInfo : public ClusterInfo {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Init, bool, (double)(std::vector<bool>*)))                          \
  ((QueryStatus, ClusterInfo,))                                         \
  ((QueryDiskInfo, DiskInfoList,))                                      \
  ((ListConfig, ConfigList,))                                           \
  ((ChangeConfig, bool,                                                 \
    (const std::string&)(const std::string&)(const std::string&)))      \
  ((SystemShutdown, bool,))                                             \
  ((ForceStart, bool, (unsigned)))

class MockICpfsAdmin : public ICpfsAdmin {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace client
}  // namespace cpfs
