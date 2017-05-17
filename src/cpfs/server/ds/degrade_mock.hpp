#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ds/degrade.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ds {

#define OBJ_METHODS                                                     \
  ((initialized, bool,))                                                \
  ((Initialize, void,))                                                 \
  ((Allocate, void,))                                                   \
  ((data, char*,))                                                      \
  ((Read, void, (std::size_t)(char*)(std::size_t)))                     \
  ((Write, void, (std::size_t)(const char*)(std::size_t)(char*)))       \
  ((RevertWrite, void, (std::size_t)(const char*)(std::size_t)))

class MockIDegradedCacheHandle : public IDegradedCacheHandle {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((IsActive, bool,))                                                   \
  ((SetActive, void, (bool)))                                           \
  ((GetHandle, boost::shared_ptr<IDegradedCacheHandle>,                 \
    (InodeNum)(std::size_t)))                                           \
  ((FreeInode, void, (InodeNum)))                                       \
  ((Truncate, void, (InodeNum)(std::size_t)))

class MockIDegradedCache : public IDegradedCache {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((SetStarted, bool,))                                                 \
  ((AddData, bool, (char*)(const boost::shared_ptr<IFimSocket>&)))      \
  ((RecoverData, bool, (char*)))                                        \
  ((QueueFim, void,                                                     \
    (const FIM_PTR<IFim>&)                                              \
    (const boost::shared_ptr<IFimSocket>&)))                            \
  ((DataSent, bool, (const boost::shared_ptr<IFimSocket>&)))            \
  ((UnqueueFim, FIM_PTR<IFim>,                                          \
    (boost::shared_ptr<IFimSocket>*)))

class MockIDataRecoveryHandle : public IDataRecoveryHandle {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((GetHandle, boost::shared_ptr<IDataRecoveryHandle>,                  \
    (InodeNum)(std::size_t)(bool)))                                     \
  ((DropHandle, void, (InodeNum)(std::size_t)))

class MockIDataRecoveryMgr : public IDataRecoveryMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ds
}  // namespace server
}  // namespace cpfs
