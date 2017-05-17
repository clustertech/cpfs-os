#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "req_tracker.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((name, std::string, , CONST))                                        \
  ((set_limiter_max_send, void, (uint64_t)))                            \
  ((req_id_gen, ReqIdGenerator*,))                                      \
  ((peer_client_num, ClientNum, ))                                      \
  ((SetFimSocket, void, (const boost::shared_ptr<IFimSocket>&)(bool)))  \
  ((GetFimSocket, boost::shared_ptr<IFimSocket>, ))                     \
  ((SetExpectingFimSocket, void, (bool)))                               \
  ((AddRequest, boost::shared_ptr<IReqEntry>,                           \
    (const FIM_PTR<IFim>&)                                              \
    (boost::unique_lock<MUTEX_TYPE>*)))                                 \
  ((AddTransientRequest, boost::shared_ptr<IReqEntry>,                  \
    (const FIM_PTR<IFim>&)                                              \
    (boost::unique_lock<MUTEX_TYPE>*)))                                 \
  ((AddRequestEntry, bool,                                              \
    (const boost::shared_ptr<IReqEntry>&)                               \
    (boost::unique_lock<MUTEX_TYPE>*)))                                 \
  ((GetRequestEntry, boost::shared_ptr<IReqEntry>, (ReqId), CONST))     \
  ((AddReply, void, (const FIM_PTR<IFim>&)))                            \
  ((ResendReplied, bool, ))                                             \
  ((RedirectRequest, void, (ReqId)(boost::shared_ptr<IReqTracker>)))    \
  ((RedirectRequests, bool, (ReqRedirector)))                           \
  ((Plug, void, (bool)))                                                \
  ((GetReqLimiter, IReqLimiter*, ))                                     \
  ((DumpPendingRequests, void,, CONST))                                 \
  ((Shutdown, void,))

class MockIReqTracker : public IReqTracker {
  MAKE_MOCK_METHODS(OBJ_METHODS)
};

#undef OBJ_METHODS

}  // namespace cpfs
