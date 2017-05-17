#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/dirty_inode.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Reset, void, (bool)))                                               \
  ((GetList, DirtyInodeMap,))                                           \
  ((version, uint64_t, , CONST))                                        \
  ((IsVolatile, bool, (InodeNum)(uint64_t*), CONST))                    \
  ((SetVolatile, bool, (InodeNum)(bool)(bool)(uint64_t*)))              \
  ((NotifyAttrSet, void, (InodeNum)))                                   \
  ((StartCleaning, bool, (InodeNum)))                                   \
  ((ClearCleaning, void,))                                              \
  ((Clean, bool,                                                        \
    (InodeNum)(uint64_t*)(boost::unique_lock<MUTEX_TYPE>*)))

class MockIDirtyInodeMgr : public IDirtyInodeMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((AsyncUpdateAttr, bool,                                              \
    (InodeNum)(FSTime)(uint64_t)(AttrUpdateHandler)))

class MockIAttrUpdater : public IAttrUpdater {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
