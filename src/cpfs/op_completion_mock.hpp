#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "op_completion.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((RegisterOp, void, (const void*)))                                   \
  ((CompleteOp, void, (const void*)))                                   \
  ((OnCompleteAll, void, (OpCompletionCallback)))                      \
  ((AllCompleted, bool,))

class MockIOpCompletionChecker : public IOpCompletionChecker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Get, boost::shared_ptr<IOpCompletionChecker>, (InodeNum)))          \
  ((CompleteOp, void, (InodeNum)(const void*)))                         \
  ((OnCompleteAll, void, (InodeNum)(OpCompletionCallback)))             \
  ((OnCompleteAllGlobal, void, (OpCompletionCallback)))                 \
  ((OnCompleteAllSubset, void,                                          \
    (const std::vector<InodeNum>&)(OpCompletionCallback)))              \
  ((Clean, void, (InodeNum)))

class MockIOpCompletionCheckerSet : public IOpCompletionCheckerSet {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
