#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/fim_defer.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((IsLocked, bool, (InodeNum), CONST))                                 \
  ((SetLocked, void, (InodeNum)(bool)))                                 \
  ((LockedInodes, std::vector<InodeNum>, , CONST))                      \
  ((AddFimEntry, void,                                                  \
    (InodeNum)(const FIM_PTR<IFim>&)                                    \
    (const boost::shared_ptr<IFimSocket>&)                              \
  ))                                                                    \
  ((HasFimEntry, bool, (InodeNum), CONST))                              \
  ((GetNextFimEntry, DeferredFimEntry, (InodeNum)))

class MockIInodeFimDeferMgr : public IInodeFimDeferMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((IsLocked, bool, (InodeNum)(uint64_t), CONST))                       \
  ((SetLocked, void, (InodeNum)(uint64_t)(bool)))                       \
  ((AddFimEntry, void,                                                  \
    (InodeNum)(uint64_t)(const FIM_PTR<IFim>&)                          \
    (const boost::shared_ptr<IFimSocket>&)))                            \
  ((SegmentHasFimEntry, bool, (InodeNum)(uint64_t), CONST))             \
  ((GetSegmentNextFimEntry, DeferredFimEntry, (InodeNum)(uint64_t)))    \
  ((ClearNextFimEntry, bool, (DeferredFimEntry*)))

class MockISegmentFimDeferMgr : public ISegmentFimDeferMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
