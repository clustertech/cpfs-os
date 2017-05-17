#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "client/inode_usage.hpp"  // IWYU pragma: export

namespace cpfs {
namespace client {

#define OBJ_METHODS                                                     \
  ((UpdateOpened, void,                                                 \
    (InodeNum)(bool)(boost::scoped_ptr<IInodeUsageGuard>*)))            \
  ((StartLockedSetattr, bool,                                           \
    (InodeNum)(boost::scoped_ptr<IInodeUsageGuard>*)))                  \
  ((StopLockedSetattr, void, (InodeNum)))                               \
  ((SetDirty, void, (InodeNum)))                                        \
  ((UpdateClosed, int,                                                  \
    (InodeNum)(bool)(bool*)(boost::scoped_ptr<IInodeUsageGuard>*)))

class MockIInodeUsageSet : public IInodeUsageSet {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace client
}  // namespace cpfs
