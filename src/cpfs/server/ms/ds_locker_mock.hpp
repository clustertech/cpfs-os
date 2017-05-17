#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/ds_locker.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((PrepareAcquire, void,))                                             \
  ((WaitAcquireCompletion, void,))                                      \
  ((PrepareRelease, void,))

class MockIMetaDSLock : public IMetaDSLock {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Lock, void,                                                         \
    (InodeNum)(const std::vector<GroupId>&)                             \
    (std::vector<boost::shared_ptr<IMetaDSLock> >*)))

class MockIDSLocker : public IDSLocker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
