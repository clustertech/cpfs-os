#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "req_completion.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((RegisterOp, void, (const void*)))                                   \
  ((CompleteOp, void, (const void*)))                                   \
  ((OnCompleteAll, void, (ReqCompletionCallback)))                      \
  ((AllCompleted, bool,))

class MockIReqCompletionChecker : public IReqCompletionChecker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Get, boost::shared_ptr<IReqCompletionChecker>, (InodeNum)))         \
  ((CompleteOp, void, (InodeNum)(const void*)))                         \
  ((OnCompleteAll, void, (InodeNum)(ReqCompletionCallback)))            \
  ((OnCompleteAllGlobal, void, (ReqCompletionCallback)))                \
  ((OnCompleteAllSubset, void,                                          \
    (const std::vector<InodeNum>&)(ReqCompletionCallback)))             \
  ((Clean, void, (InodeNum)))

class MockIReqCompletionCheckerSet : public IReqCompletionCheckerSet {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
