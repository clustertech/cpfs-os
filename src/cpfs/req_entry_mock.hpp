#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "req_entry.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((req_id, ReqId, , CONST))                                            \
  ((request, FIM_PTR<cpfs::IFim>, , CONST))                             \
  ((reply, const FIM_PTR<cpfs::IFim>&, , CONST))                        \
  ((reply_no, uint64_t, , CONST))                                       \
  ((WaitReply, const FIM_PTR<cpfs::IFim>&, ))                           \
  ((OnAck, void, (ReqAckCallback)(bool)))                               \
  ((FillLock, void, (boost::unique_lock<MUTEX_TYPE>*)))                 \
  ((set_sent, void, (bool)))                                            \
  ((sent, bool, , CONST))                                               \
  ((set_reply_error, void, (bool)))                                     \
  ((SetReply, void, (const FIM_PTR<IFim>&)(uint64_t)))                  \
  ((PreQueueHook, void,))                                               \
  ((IsPersistent, bool, , CONST))                                       \
  ((UnsetRequest, void, , ))                                            \
  ((Dump, void, , CONST))

class MockIReqEntry : public IReqEntry {
  MAKE_MOCK_METHODS(OBJ_METHODS)
};

#undef OBJ_METHODS

}  // namespace cpfs
