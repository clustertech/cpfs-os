#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "fim.hpp"
#include "mock_helper.hpp"
#include "tracker_mapper.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Reset, void,))                                                      \
  ((SetClientNum, void, (ClientNum)))                                   \
  ((SetLimiterMaxSend, void, (uint64_t)))                               \
  ((GetMSTracker, boost::shared_ptr<IReqTracker>,))                     \
  ((GetMSFimSocket, boost::shared_ptr<IFimSocket>,))                    \
  ((SetMSFimSocket, void, (boost::shared_ptr<IFimSocket>)(bool)))       \
  ((GetDSTracker, boost::shared_ptr<IReqTracker>,                       \
    (GroupId)(GroupRole)))                                              \
  ((GetDSFimSocket, boost::shared_ptr<IFimSocket>,                      \
    (GroupId)(GroupRole)))                                              \
  ((SetDSFimSocket, void,                                               \
    (boost::shared_ptr<IFimSocket>)(GroupId)(GroupRole)(bool)))         \
  ((FindDSRole, bool,                                                   \
    (boost::shared_ptr<IFimSocket>)(GroupId*)(GroupRole*)))             \
  ((GetFCTracker, boost::shared_ptr<IReqTracker>, (ClientNum)))         \
  ((GetFCFimSocket, boost::shared_ptr<IFimSocket>, (ClientNum)))        \
  ((SetFCFimSocket, void,                                               \
    (boost::shared_ptr<IFimSocket>)(ClientNum)(bool)))                  \
  ((DSGBroadcast, void, (GroupId)(FIM_PTR<IFim>)(GroupRole)))           \
  ((FCBroadcast, void, (FIM_PTR<IFim>)))                                \
  ((Plug, void, (bool)))                                                \
  ((HasPendingWrite, bool,, CONST))                                     \
  ((DumpPendingRequests, void,, CONST))                                 \
  ((Shutdown, void,))                                                   \
  ((GetAdminTracker, boost::shared_ptr<IReqTracker>, (ClientNum)))      \
  ((GetAdminFimSocket, boost::shared_ptr<IFimSocket>, (ClientNum)))     \
  ((SetAdminFimSocket, void,                                            \
    (boost::shared_ptr<IFimSocket>)(ClientNum)(bool)))

class MockITrackerMapper : public ITrackerMapper {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
