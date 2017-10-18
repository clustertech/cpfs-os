#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "req_completion.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((RegisterReq, void, (const boost::shared_ptr<IReqEntry>&)))          \
  ((CompleteReq, void, (const boost::shared_ptr<IReqEntry>&)))          \
  ((OnCompleteAll, void, (ReqCompletionCallback)))                      \
  ((AllCompleted, bool,))

class MockIReqCompletionChecker : public IReqCompletionChecker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Get, boost::shared_ptr<IReqCompletionChecker>, (InodeNum)))         \
  ((GetReqAckCallback, ReqAckCallback,                                  \
    (InodeNum)(const boost::shared_ptr<IFimSocket>&)))                  \
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
