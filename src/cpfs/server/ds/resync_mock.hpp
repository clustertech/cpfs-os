#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/bind.hpp>

#include "mock_helper.hpp"
#include "server/ds/resync.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ds {

#define OBJ_METHODS                                                     \
  ((Run, void,))                                                        \
  ((SetShapedSenderMaker, void, (ShapedSenderMaker)))                   \
  ((SendDirFims, void, (boost::shared_ptr<IReqTracker>)))               \
  ((ReadResyncList, void, (boost::shared_ptr<IReqTracker>)))            \
  ((StartResyncPhase, size_t, (boost::shared_ptr<IReqTracker>)))        \
  ((SendDataRemoval, void, (boost::shared_ptr<IReqTracker>)))           \
  ((SendAllResync, void, (boost::shared_ptr<IReqTracker>)))

class MockIResyncSender : public IResyncSender {
 public:
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Make, IResyncSender*, (BaseDataServer*)(GroupRole)))

class MockResyncSenderMaker {
 public:
  MAKE_MOCK_METHODS(OBJ_METHODS);

  ResyncSenderMaker GetMaker() {
    return boost::bind(&MockResyncSenderMaker::Make, this, _1, _2);
  }
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Start, void, (GroupRole)))                                          \
  ((IsStarted, bool, (GroupRole*)))                                     \
  ((SetResyncSenderMaker, void, (ResyncSenderMaker)))               \
  ((SetShapedSenderMaker, void, (ShapedSenderMaker)))

class MockIResyncMgr : public IResyncMgr {
 public:
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Accept, bool, (const FIM_PTR<IFim>&), CONST))                       \
  ((Process, bool,                                                      \
    (const FIM_PTR<IFim>&)                                              \
    (const boost::shared_ptr<IFimSocket>&)))                            \
  ((AsyncResync, void, (ResyncCompleteHandler)))

class MockIResyncFimProcessor : public IResyncFimProcessor {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ds
}  // namespace server
}  // namespace cpfs
