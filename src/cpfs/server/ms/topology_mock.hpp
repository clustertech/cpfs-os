#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "admin_info.hpp"
#include "mock_helper.hpp"
#include "server/ms/topology.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((num_groups, GroupId,, CONST))                                       \
  ((set_num_groups, void, (GroupId)))                                   \
  ((HasDS, bool, (GroupId)(GroupRole), CONST))                          \
  ((GetDSGState, DSGroupState, (GroupId)(GroupRole*)))                  \
  ((SetDSGState, void, (GroupId)(GroupRole)(DSGroupState)(uint64_t)))   \
  ((SetDSLost, void, (GroupId)(GroupRole)))                             \
  ((AddDS, bool, (GroupId)(GroupRole)(NodeInfo)(bool)(bool*)))          \
  ((RemoveDS, void, (GroupId)(GroupRole)(bool*)))                       \
  ((DSRecovered, bool, (GroupId)(GroupRole)(char)))                     \
  ((DSGReady, bool, (GroupId), CONST))                                  \
  ((AllDSReady, bool,))                                                 \
  ((SuggestDSRole, bool, (GroupId*)(GroupRole*)))                       \
  ((SendAllDSInfo, void, (boost::shared_ptr<IFimSocket>)))              \
  ((AnnounceDS, void, (GroupId)(GroupRole)(bool)(bool)))                \
  ((AnnounceDSGState, void, (GroupId)))                                 \
  ((IsDSGStateChanging, bool,))                                         \
  ((AckDSGStateChangeWait, void, (ClientNum)))                          \
  ((AckDSGStateChange, bool,                                            \
    (uint64_t)(boost::shared_ptr<IFimSocket>)(GroupId*)(DSGroupState*)  \
  ))                                                                    \
  ((AllDSGStartable, bool,, CONST))                                     \
  ((ForceStartDSG, bool,))                                              \
  ((GetFCs, std::vector<ClientNum>,, CONST))                            \
  ((GetNumFCs, unsigned,, CONST))                                       \
  ((AddFC, bool, (ClientNum)(NodeInfo)))                                \
  ((RemoveFC, void, (ClientNum)))                                       \
  ((SetFCTerminating, void, (ClientNum)))                               \
  ((SuggestFCId, bool, (ClientNum*)))                                   \
  ((SetNextFCId, void, (ClientNum)))                                    \
  ((AnnounceFC, void, (ClientNum)(bool)))                               \
  ((SendAllFCInfo, void,))                                              \
  ((AnnounceShutdown, void,))                                           \
  ((AnnounceHalt, void,))                                               \
  ((AckDSShutdown, bool, (boost::shared_ptr<IFimSocket>)))              \
  ((StartStopWorker, void,))                                            \
  ((SetDSGDistressed, void, (GroupId)(bool)))                           \
  ((GetDSInfos, std::vector<NodeInfo>,))                                \
  ((GetFCInfos, std::vector<NodeInfo>,))                                \
  ((AddMS, bool, (NodeInfo)(bool)))

class MockITopologyMgr : public ITopologyMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
