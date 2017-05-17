#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/store_checker.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Init, bool, (IStoreChecker*)(bool)))                                \
  ((InfoFound, bool, (std::string)(std::string)))                       \
  ((InfoScanCompleted, bool,))                                          \
  ((InodeFound, bool, (InodeNum)(struct stat*)(int)))                   \
  ((UnknownNodeFound, bool, (std::string)))                             \
  ((InodeScanCompleted, bool,))                                         \
  ((DentryFound, bool,                                                  \
    (InodeNum)(std::string)(InodeNum)(struct stat*)))                   \
  ((UnknownDentryFound, bool, (InodeNum)(std::string)))                 \
  ((DentryScanCompleted, bool,))

class MockIStoreCheckerPlugin : public IStoreCheckerPlugin {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((RegisterPlugin, void, (IStoreCheckerPlugin*)))                      \
  ((Run, bool, (bool)))                                                 \
  ((GetRoot, std::string,))                                             \
  ((GetInodePath, std::string, (InodeNum)))                             \
  ((GetDentryPath, std::string, (InodeNum)(std::string)))               \
  ((GetConsole, IConsole*,))                                            \
  ((GetStore, IStore*,))

class MockIStoreChecker : public IStoreChecker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
