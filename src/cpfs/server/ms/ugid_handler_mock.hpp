#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/ugid_handler.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS

class MockIUgidSetterGuard : public IUgidSetterGuard {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

// Note: need proxying for SetFsIds
#define OBJ_METHODS                                                     \
  ((SetCheck, void, (bool)))                                            \
  ((SetFsIdsProxied, IUgidSetterGuard*, (uid_t)(gid_t)))                \
  ((HasSupplementaryGroup, bool, (uid_t)(gid_t)))                       \
  ((Clean, void,))

class MockIUgidHandler : public IUgidHandler {
  MAKE_MOCK_METHODS(OBJ_METHODS);
  UNIQUE_PTR<IUgidSetterGuard> SetFsIds(uid_t uid, gid_t gid) {
    return UNIQUE_PTR<IUgidSetterGuard>(SetFsIdsProxied(uid, gid));
  }
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
