/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/topology_impl.hpp"

#include <stdint.h>

#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "admin_info.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "util.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/stat_keeper_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/store_mock.hpp"
#include "server/ms/topology.hpp"
#include "server/server_info_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::ElementsAre;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const GroupId kNumDSGroups = 2;

class TopologyTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  boost::scoped_ptr<ITopologyMgr> topology_mgr_;
  MockITrackerMapper* tracker_mapper_;
  boost::shared_ptr<MockIFimSocket> ms_fim_socket_;
  boost::shared_ptr<MockIFimSocket> ds_fim_socket_[kNumDSPerGroup * 2];
  boost::shared_ptr<MockIFimSocket> fc_fim_socket_;
  MockIStateMgr* state_mgr_;
  MockIThreadGroup* thread_group_;
  MockIStatKeeper* stat_keeper_;
  MockIServerInfo* server_info_;
  MockITimeKeeper* peer_time_keeper_;
  MockIStore* store_;
  SpaceStatCallback callback;

  static ConfigMgr MakeConfig() {
    ConfigMgr ret;
    ret.set_role("MS1");
    return ret;
  }

  TopologyTest()
      : server_(MakeConfig()),
        topology_mgr_(MakeTopologyMgr(&server_)),
        ms_fim_socket_(new MockIFimSocket),
        fc_fim_socket_(new MockIFimSocket) {
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    for (GroupRole i = 0; i < 2 * kNumDSPerGroup; ++i)
      ds_fim_socket_[i] = boost::make_shared<MockIFimSocket>();
    server_.set_state_mgr(state_mgr_ = new MockIStateMgr);
    server_.set_thread_group(thread_group_ = new MockIThreadGroup);
    server_.set_stat_keeper(stat_keeper_ = new MockIStatKeeper);
    server_.set_server_info(server_info_ = new MockIServerInfo);
    server_.set_peer_time_keeper(peer_time_keeper_ = new MockITimeKeeper);
    server_.set_store(store_ = new MockIStore);
    // Init, get callback
    EXPECT_CALL(*stat_keeper_, OnAllStat(_))
        .WillOnce(SaveArg<0>(&callback));
    EXPECT_CALL(*server_info_, Get("max-dsg-id", "0"))
        .WillOnce(Return(std::string("0")));
    EXPECT_CALL(*server_info_, Get("max-ndsg", "1"))
        .WillOnce(Return(std::string("2")));
    EXPECT_CALL(*store_, LoadAllUUID())
        .WillOnce(Return(std::map<std::string, std::string>()));
    EXPECT_CALL(*store_, GetUUID())
        .WillOnce(Return("abc123-ab12-abcb-a332-eb5599996014"));
    EXPECT_CALL(*store_, PersistAllUUID(_))
      .Times(AtLeast(1));
    topology_mgr_->Init();
  }

  ~TopologyTest() {
    Mock::VerifyAndClear(tracker_mapper_);
  }

  void PrepareDSG(GroupId group, GroupRole numRole, GroupId goffset = 0) {
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      if (r < numRole) {
        EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(group, r))
            .WillRepeatedly(Return(ds_fim_socket_[r + goffset]));
        EXPECT_CALL(*tracker_mapper_, FindDSRole(
            boost::static_pointer_cast<IFimSocket>(
                ds_fim_socket_[r + goffset]), _, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(group),
                                  SetArgPointee<2>(r),
                                  Return(true)));
      } else {
        EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(group, r))
            .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
        EXPECT_CALL(*tracker_mapper_, FindDSRole(
            boost::static_pointer_cast<IFimSocket>(
                ds_fim_socket_[r + goffset]), _, _))
            .WillRepeatedly(Return(false));
      }
    }
  }

  void PrepareDSGFull(GroupId group, GroupId goffset = 0) {
    PrepareDSG(group, kNumDSPerGroup, goffset);
    NodeInfo ni;
    ni.pid = 0;
    std::string s = "aec345-cd34-cdce-b442-eb5599996010";
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      ni.ip = ni.port = 1;
      s[s.length() - 2] = char(group + '0');
      s[s.length() - 1] = char(r + '0');
      ni.SetUUID(s.c_str());
      bool state_changed;
      EXPECT_TRUE(topology_mgr_->AddDS(group, r, ni, false, &state_changed));
      EXPECT_EQ(r == kNumDSPerGroup - 1, state_changed);
    }
  }
};

TEST_F(TopologyTest, AddRemoveDS) {
  GroupRole failed;
  EXPECT_EQ(kDSGPending, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_EQ(2U, topology_mgr_->num_groups());
  EXPECT_FALSE(topology_mgr_->HasDS(0, 0));

  // Fully populate DSG 0
  GroupId g;
  GroupRole r;
  EXPECT_TRUE(topology_mgr_->SuggestDSRole(&g, &r));
  EXPECT_EQ(0U, g);
  EXPECT_EQ(0U, r);
  NodeInfo ni;
  ni.ip = 1;
  ni.port = 2;
  ni.SetUUID("1204d54-abd4-431b-a332-eb5599996014");
  NodeInfo ni2;
  ni2.ip = 2;
  ni2.port = 1;
  ni2.SetUUID("7704d54-cbd4-531b-b632-af5599996014");
  bool state_changed;
  EXPECT_TRUE(topology_mgr_->AddDS(0, 0, ni, false, &state_changed));
  EXPECT_TRUE(topology_mgr_->HasDS(0, 0));
  EXPECT_TRUE(topology_mgr_->AddDS(0, 0, ni, false, &state_changed));
  EXPECT_FALSE(topology_mgr_->AddDS(0, 0, ni2, false, &state_changed));
  for (GroupRole r = 2; r < kNumDSPerGroup; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(0, r, ni, false, &state_changed));
  EXPECT_TRUE(topology_mgr_->SuggestDSRole(&g, &r));
  EXPECT_EQ(0U, g);
  EXPECT_EQ(1U, r);
  EXPECT_EQ(kDSGPending, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_TRUE(topology_mgr_->AddDS(0, 1, ni, false, &state_changed));
  EXPECT_EQ(kDSGReady, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_TRUE(topology_mgr_->SuggestDSRole(&g, &r));
  EXPECT_EQ(1U, g);
  EXPECT_EQ(0U, r);

  // Loss one DS in DSG 0
  EXPECT_TRUE(topology_mgr_->HasDS(0, 2));
  topology_mgr_->RemoveDS(0, 2, &state_changed);
  EXPECT_FALSE(topology_mgr_->HasDS(0, 2));
  EXPECT_TRUE(topology_mgr_->SuggestDSRole(&g, &r));
  EXPECT_EQ(1U, g);
  EXPECT_EQ(0U, r);
  EXPECT_EQ(kDSGDegraded, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_EQ(2U, failed);

  // Fully populate DSG 1
  EXPECT_EQ(kDSGPending, topology_mgr_->GetDSGState(1, &failed));
  for (g = 0; g < kNumDSPerGroup; ++g) {
    EXPECT_FALSE(topology_mgr_->AllDSReady());
    EXPECT_TRUE(topology_mgr_->AddDS(1, g, ni, false, &state_changed));
  }
  EXPECT_TRUE(topology_mgr_->DSGReady(1));
  EXPECT_EQ(kDSGReady, topology_mgr_->GetDSGState(1, &failed));

  // AllDSReady() will return true once all DS fim socket is populated
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(_, _))
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));

  EXPECT_FALSE(topology_mgr_->AllDSReady());

  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(_, _))
      .WillRepeatedly(Return(ds_fim_socket_[0]));

  EXPECT_TRUE(topology_mgr_->AllDSReady());

  // Can suggest to refill the lost role
  EXPECT_TRUE(topology_mgr_->SuggestDSRole(&g, &r));
  EXPECT_EQ(0U, g);
  EXPECT_EQ(2U, r);

  // Can actually refill the lost role
  topology_mgr_->AddDS(0, 2, ni, false, &state_changed);
  EXPECT_FALSE(topology_mgr_->SuggestDSRole(&g, &r));
  EXPECT_EQ(kDSGRecovering, topology_mgr_->GetDSGState(0, &failed));

  // Reacts to failure and recovery completion properly
  topology_mgr_->RemoveDS(0, 2, &state_changed);
  EXPECT_EQ(kDSGDegraded, topology_mgr_->GetDSGState(0, &failed));
  topology_mgr_->AddDS(0, 2, ni, false, &state_changed);
  EXPECT_EQ(kDSGRecovering, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_FALSE(topology_mgr_->DSRecovered(0, 3, 1));
  EXPECT_TRUE(topology_mgr_->DSRecovered(0, 2, 0));
  EXPECT_TRUE(topology_mgr_->DSRecovered(0, 2, 1));
  EXPECT_FALSE(topology_mgr_->DSRecovered(0, 2, 1));
  EXPECT_EQ(kDSGReady, topology_mgr_->GetDSGState(0, &failed));
  topology_mgr_->RemoveDS(0, 2, &state_changed);
  topology_mgr_->AddDS(0, 2, ni, false, &state_changed);
  EXPECT_EQ(kDSGRecovering, topology_mgr_->GetDSGState(0, &failed));

  // Can report failure of DSG
  topology_mgr_->RemoveDS(0, 3, &state_changed);
  EXPECT_EQ(kDSGFailed, topology_mgr_->GetDSGState(0, &failed));
  topology_mgr_->AddDS(0, 3, ni, false, &state_changed);
  EXPECT_EQ(kDSGFailed, topology_mgr_->GetDSGState(0, &failed));

  // Checking of invalid argument
  EXPECT_THROW(topology_mgr_->RemoveDS(0, kNumDSPerGroup, &state_changed),
               std::invalid_argument);
  EXPECT_THROW(topology_mgr_->RemoveDS(5, 0, &state_changed),
               std::invalid_argument);
}

TEST_F(TopologyTest, AddDSInvalidUUID) {
  NodeInfo ni;
  ni.ip = 2;
  ni.port = 1;
  ni.SetUUID("7704d54-cbd4-531b-b632-af5599996014");
  bool state_changed;
  EXPECT_TRUE(topology_mgr_->AddDS(0, 1, ni, false, &state_changed));
  // Join DS again with Invalid UUID
  NodeInfo ni2;
  ni2.ip = 3;
  ni2.port = 4;
  ni2.SetUUID("1122344-5566-77aa-b632-af5599996014");  // Invalid UUID
  topology_mgr_->RemoveDS(0, 1, &state_changed);
  EXPECT_FALSE(topology_mgr_->AddDS(0, 1, ni2, true, &state_changed));
}

TEST_F(TopologyTest, AddMS) {
  NodeInfo ni;
  ni.ip = 2;
  ni.port = 1;
  ni.SetUUID("7704d54-cbd4-531b-b632-af5599996014");
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(0));
  EXPECT_TRUE(topology_mgr_->AddMS(ni, true));
  // Add again
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(1));
  EXPECT_TRUE(topology_mgr_->AddMS(ni, false));
  // Add again with invalid UUID
  ni.SetUUID("112233-cbd4-531b-b632-af5599996014");
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(1));
  EXPECT_FALSE(topology_mgr_->AddMS(ni, false));
}

TEST_F(TopologyTest, SetDSLost) {
  // SetDSLost allows DS to start degraded
  topology_mgr_->SetDSLost(0, 1);
  GroupRole failed = 0;
  EXPECT_EQ(kDSGPending, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_EQ(1U, failed);

  NodeInfo ni;
  ni.ip = 1;
  ni.port = 2;
  bool state_changed = false;
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    if (r == 1)
      continue;
    EXPECT_FALSE(state_changed);
    EXPECT_EQ(kDSGPending, topology_mgr_->GetDSGState(0, &failed));
    EXPECT_TRUE(topology_mgr_->AddDS(0, r, ni, false, &state_changed));
  }
  EXPECT_TRUE(state_changed);
  failed = 0;
  EXPECT_EQ(kDSGDegraded, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_EQ(1U, failed);

  // Once started it has no effect
  topology_mgr_->SetDSLost(0, 1);
  EXPECT_EQ(kDSGDegraded, topology_mgr_->GetDSGState(0, &failed));
}

TEST_F(TopologyTest, SendAllDSInfo) {
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(1, 0))
      .WillRepeatedly(Return(boost::shared_ptr<MockIFimSocket>()));
  NodeInfo ni[2];
  ni[0].ip = 1;
  ni[0].port = 2;
  ni[1].ip = 11;
  ni[1].port = 12;
  for (GroupId g = 0; g < 2U; ++g)
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
      EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(g, r))
          .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 1))
      .WillRepeatedly(Return(ds_fim_socket_[1]));
  bool state_changed;
  for (GroupRole r = 0; r <= 1U; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(0, r, ni[r], false, &state_changed));
  boost::shared_ptr<MockIFimSocket> peer(new MockIFimSocket);
  FIM_PTR<IFim> fim0, fim1, fim2;
  EXPECT_CALL(*peer, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim0))
      .WillOnce(SaveArg<0>(&fim1))
      .WillOnce(SaveArg<0>(&fim2));

  topology_mgr_->SendAllDSInfo(peer);
  TopologyChangeFim& rfim0 = dynamic_cast<TopologyChangeFim&>(*fim0);
  EXPECT_EQ(0U, rfim0->ds_group);
  EXPECT_EQ(1U, rfim0->ds_role);
  DSGStateChangeFim& rfim2 = dynamic_cast<DSGStateChangeFim&>(*fim2);
  EXPECT_EQ(1U, rfim2->ds_group);
  EXPECT_EQ(kDSGPending, rfim2->state);
  EXPECT_EQ(0U, rfim2->distress);
}

TEST_F(TopologyTest, AnnounceDSJoin) {
  NodeInfo ni[2];
  ni[0].ip = 1;
  ni[0].port = 2;
  ni[0].pid = 0;
  ni[0].SetUUID("aec345-cd34-cdce-b442-eb5599996014");
  ni[1].ip = 3;
  ni[1].port = 4;
  ni[1].pid = 0;
  ni[1].SetUUID("dac345-ad74-123e-b442-c45599996064");
  PrepareDSG(1, 2);
  bool state_changed;
  for (GroupRole r = 0; r < 2; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(1, r, ni[r], false, &state_changed));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket_));
  FIM_PTR<IFim> ms_fim, ds_fim, fc_fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(1, _, _))
      .WillOnce(SaveArg<1>(&ds_fim));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim));
  NodeInfo info;
  info.ip = 10000;
  info.port = 1234U;
  info.pid = 1000;
  info.SetUUID("ccc111-cd34-cdce-b442-eb5599996014");
  topology_mgr_->AddFC(1, info);
  topology_mgr_->AnnounceDS(1, 0, true, false);
  TopologyChangeFim& tcfim0 = dynamic_cast<TopologyChangeFim&>(*ms_fim);
  EXPECT_EQ('D', tcfim0->type);
  EXPECT_EQ('\x01', tcfim0->joined);
  EXPECT_EQ(1U, tcfim0->ds_group);
  EXPECT_EQ(0U, tcfim0->ds_role);
  EXPECT_EQ(ni[0].ip, tcfim0->ip);
  EXPECT_EQ(ni[0].port, tcfim0->port);
  TopologyChangeFim& tcfim1 = dynamic_cast<TopologyChangeFim&>(*ds_fim);
  EXPECT_EQ(0, std::memcmp(tcfim0.msg(), tcfim1.msg(), tcfim0.len()));
  TopologyChangeFim& tcfim2 = dynamic_cast<TopologyChangeFim&>(*fc_fim);
  EXPECT_EQ(0, std::memcmp(tcfim0.msg(), tcfim2.msg(), tcfim0.len()));
}

TEST_F(TopologyTest, AnnounceDSGReadyNoFC) {
  PrepareDSGFull(0);

  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket_));
  FIM_PTR<IFim> ms_fim1, ms_fim2, ds_fim1, ds_fim2, fc_fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim1))
      .WillOnce(SaveArg<0>(&ms_fim2));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, _))
      .WillOnce(SaveArg<1>(&ds_fim1))
      .WillOnce(SaveArg<1>(&ds_fim2));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim));

  topology_mgr_->AnnounceDS(0, 0, true, true);
  EXPECT_EQ(kTopologyChangeFim, ms_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, ds_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, fc_fim->type());
  EXPECT_EQ(kDSGStateChangeFim, ms_fim2->type());
  EXPECT_EQ(kDSGStateChangeFim, ds_fim2->type());
}

TEST_F(TopologyTest, AnnounceDSGReadyTwiceWithFC) {
  FIM_PTR<IFim> ms_fim1, ms_fim2, ds_fim1, ds_fim2, fc_fim1, fc_fim2;
  PrepareDSGFull(0);
  PrepareDSGFull(1, kNumDSPerGroup);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket_));

  // Add a FC
  NodeInfo info;
  info.ip = 10000;
  info.port = 1234U;
  info.pid = 1000;
  info.SetUUID("ccc111-cd34-cdce-b442-eb5599996014");

  topology_mgr_->AddFC(1, info);

  // Announce the DSG state change
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim1));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, _))
      .WillOnce(SaveArg<1>(&ds_fim1));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim1))
      .WillOnce(SaveArg<0>(&fc_fim2));
  EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(1))
      .WillOnce(Return(fc_fim_socket_));

  EXPECT_FALSE(topology_mgr_->IsDSGStateChanging());
  topology_mgr_->AnnounceDS(0, 0, true, true);
  EXPECT_TRUE(topology_mgr_->IsDSGStateChanging());
  EXPECT_EQ(kTopologyChangeFim, ms_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, ds_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, fc_fim1->type());
  EXPECT_EQ(kDSGStateChangeWaitFim, fc_fim2->type());

  // Announce another DSG state change, piggyback (last FC FIM not sent)
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim1));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(1, _, _))
      .WillOnce(SaveArg<1>(&ds_fim1));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim1));

  topology_mgr_->AnnounceDS(1, 0, true, true);
  EXPECT_EQ(kTopologyChangeFim, ms_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, ds_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, fc_fim1->type());

  // Wrong FC replies, no effect
  topology_mgr_->AckDSGStateChangeWait(2);

  // Right FC replies, triggers state change Fims
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim1))
      .WillOnce(SaveArg<0>(&ms_fim2));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, _))
      .WillOnce(SaveArg<1>(&ds_fim1));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(1, _, _))
      .WillOnce(SaveArg<1>(&ds_fim2));

  topology_mgr_->AckDSGStateChangeWait(1);
  EXPECT_EQ(kDSGStateChangeFim, ms_fim1->type());
  EXPECT_EQ(kDSGStateChangeFim, ds_fim2->type());
  uint64_t sc_id1 = dynamic_cast<DSGStateChangeFim&>(*ms_fim1)->
      state_change_id;
  uint64_t sc_id2 = dynamic_cast<DSGStateChangeFim&>(*ds_fim2)->
      state_change_id;

  // Group 0 DSs reply, last one triggers FC broadcast
  GroupId group;
  DSGroupState state;
  for (GroupRole r = 0; r < kNumDSPerGroup - 1; ++r) {
    EXPECT_FALSE(topology_mgr_->AckDSGStateChange(
        sc_id1, ds_fim_socket_[r], &group, &state));
  }

  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim1));

  EXPECT_TRUE(topology_mgr_->AckDSGStateChange(
      sc_id1, ds_fim_socket_[kNumDSPerGroup - 1], &group, &state));
  EXPECT_EQ(kDSGStateChangeFim, fc_fim1->type());

  // Group 1 DSs reply, last one triggers FC two broadcasts
  for (GroupRole r = 0; r < kNumDSPerGroup - 1; ++r) {
    EXPECT_FALSE(topology_mgr_->AckDSGStateChange(
        sc_id2, ds_fim_socket_[kNumDSPerGroup + r], &group, &state));
  }

  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim1))
      .WillOnce(SaveArg<0>(&fc_fim2));

  EXPECT_TRUE(topology_mgr_->AckDSGStateChange(
      sc_id2, ds_fim_socket_[2 * kNumDSPerGroup - 1], &group, &state));
  EXPECT_EQ(kDSGStateChangeFim, fc_fim1->type());
  EXPECT_EQ(kDSGStateChangeWaitFim, fc_fim2->type());
}

TEST_F(TopologyTest, AnnounceDSGReadyTwiceWithFCPiggyback) {
  FIM_PTR<IFim> ms_fim1, ms_fim2, ds_fim1, ds_fim2, fc_fim1, fc_fim2;
  PrepareDSGFull(0);
  PrepareDSGFull(1, kNumDSPerGroup);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket_));

  // Add a FC
  NodeInfo info;
  info.ip = 10000;
  info.port = 1234U;
  info.pid = 1000;
  info.SetUUID("ccc111-cd34-cdce-b442-eb5599996014");

  topology_mgr_->AddFC(1, info);

  // Announce the DSG state change
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim1));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, _))
      .WillOnce(SaveArg<1>(&ds_fim1));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim1))
      .WillOnce(SaveArg<0>(&fc_fim2));
  EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(1))
      .WillOnce(Return(fc_fim_socket_));

  topology_mgr_->AnnounceDS(0, 0, true, true);

  // FC replies, triggers state change Fims
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim1));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, _))
      .WillOnce(SaveArg<1>(&ds_fim1));

  topology_mgr_->AckDSGStateChangeWait(1);

  // Other requests now sends immediately
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim1))
      .WillOnce(SaveArg<0>(&ms_fim2));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(1, _, _))
      .WillOnce(SaveArg<1>(&ds_fim1))
      .WillOnce(SaveArg<1>(&ds_fim2));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim1));

  topology_mgr_->AnnounceDS(1, 0, true, true);
  EXPECT_EQ(kTopologyChangeFim, ms_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, ds_fim1->type());
  EXPECT_EQ(kTopologyChangeFim, fc_fim1->type());
  EXPECT_EQ(kDSGStateChangeFim, ms_fim2->type());
  EXPECT_EQ(kDSGStateChangeFim, ds_fim2->type());
}

TEST_F(TopologyTest, AnnounceDSLeave) {
  NodeInfo ds_info[2];
  ds_info[0].ip = 1;
  ds_info[0].port = 2;
  ds_info[1].ip = 3;
  ds_info[1].port = 4;
  PrepareDSG(0, 2);
  bool state_changed;
  for (GroupRole r = 0; r < 2; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(0, r, ds_info[r], false, &state_changed));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket_));
  FIM_PTR<IFim> ms_fim, ms_fim_2, ds_fim, fc_fim, ds_fim_2;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim))
      .WillOnce(SaveArg<0>(&ms_fim_2));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, _))
      .WillOnce(SaveArg<1>(&ds_fim))
      .WillOnce(SaveArg<1>(&ds_fim_2));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .WillOnce(SaveArg<0>(&fc_fim));

  NodeInfo fc_info;
  fc_info.ip = 10000;
  fc_info.port = 1234U;
  fc_info.pid = 2000;
  topology_mgr_->AddFC(1, fc_info);
  topology_mgr_->AnnounceDS(0, 1, false, true);
  TopologyChangeFim& tcfim0 = dynamic_cast<TopologyChangeFim&>(*ms_fim);
  EXPECT_EQ('D', tcfim0->type);
  EXPECT_EQ('\x00', tcfim0->joined);
  EXPECT_EQ(0U, tcfim0->ds_group);
  EXPECT_EQ(1U, tcfim0->ds_role);
  TopologyChangeFim& tcfim1 = dynamic_cast<TopologyChangeFim&>(*ds_fim);
  EXPECT_EQ(0, std::memcmp(tcfim0.msg(), tcfim1.msg(), tcfim0.len()));
  TopologyChangeFim& tcfim2 = dynamic_cast<TopologyChangeFim&>(*fc_fim);
  EXPECT_EQ(0, std::memcmp(tcfim0.msg(), tcfim2.msg(), tcfim0.len()));
  DSGStateChangeFim& dsgfim = dynamic_cast<DSGStateChangeFim&>(*ds_fim_2);
  EXPECT_EQ(0U, dsgfim->ds_group);
  EXPECT_EQ(0U, dsgfim->state_change_id);
  EXPECT_EQ(kDSGPending, dsgfim->state);
  EXPECT_EQ(0U, dsgfim->ready);
  EXPECT_EQ(ms_fim_2, ds_fim_2);
}

TEST_F(TopologyTest, AckDSGStateChange) {
  // Setup: make DSG degraded, add an FC
  PrepareDSG(1, kNumDSPerGroup - 1);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));

  NodeInfo ds_info;
  ds_info.ip = 1;
  ds_info.port = 1234U;
  ds_info.pid = 1999;
  bool state_changed;
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(1, r, ds_info, false, &state_changed));
  EXPECT_TRUE(state_changed);
  topology_mgr_->RemoveDS(1, kNumDSPerGroup - 1, &state_changed);
  EXPECT_TRUE(state_changed);
  NodeInfo fc_info;
  fc_info.ip = 1000;
  fc_info.port = 1234U;
  fc_info.pid = 2000;
  topology_mgr_->AddFC(1, fc_info);

  // Announce it (to start accounting of ack'ed roles)
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(1, _, _)).Times(2);
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_))
      .Times(2).WillRepeatedly(SaveArg<0>(&fim));
  topology_mgr_->AnnounceDS(1, kNumDSPerGroup - 1, false, true);

  // Acked all except 1
  GroupId group;
  DSGroupState state;
  for (GroupRole r = 0; r < kNumDSPerGroup - 2; ++r) {
    EXPECT_FALSE(topology_mgr_->AckDSGStateChange(
        2, ds_fim_socket_[r], &group, &state));
    EXPECT_EQ(1U, group);
    EXPECT_EQ(kDSGDegraded, state);
  }
  // Ack from unknown role, ignored
  EXPECT_FALSE(topology_mgr_->AckDSGStateChange(
      2, ds_fim_socket_[kNumDSPerGroup - 1], &group, &state));
  EXPECT_EQ(kDSGOutdated, state);
  // Reack, no problem
  EXPECT_FALSE(topology_mgr_->AckDSGStateChange(
      2, ds_fim_socket_[0], &group, &state));
  EXPECT_EQ(kDSGDegraded, state);
  // Ignore incorrect state change id
  EXPECT_FALSE(topology_mgr_->AckDSGStateChange(
      1, ds_fim_socket_[kNumDSPerGroup - 2], &group, &state));
  EXPECT_EQ(kDSGOutdated, state);
  // Complete
  EXPECT_TRUE(topology_mgr_->AckDSGStateChange(
      2, ds_fim_socket_[kNumDSPerGroup - 2], &group, &state));
  EXPECT_EQ(kDSGDegraded, state);
  DSGStateChangeFim& dsg_fim = dynamic_cast<DSGStateChangeFim&>(*fim);
  EXPECT_EQ(2U, dsg_fim->state_change_id);
  EXPECT_EQ(kDSGDegraded, dsg_fim->state);
  EXPECT_EQ(1U, dsg_fim->ds_group);
  EXPECT_EQ(kNumDSPerGroup - 1U, dsg_fim->failed);
  EXPECT_EQ(0U, dsg_fim->ready);
}

TEST_F(TopologyTest, AnnounceDSGState) {
  // Optimized resync
  PrepareDSG(1, kNumDSPerGroup);
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(1, _, kNumDSPerGroup));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket_));
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));

  NodeInfo ni_ds2;
  bool state_changed;
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    NodeInfo ni;
    ni.ip = 1;
    ni.port = 2;
    ni.SetUUID(CreateUUID().c_str());
    if (r == 2)
      ni_ds2 = ni;
    topology_mgr_->AddDS(1, r, ni, false, &state_changed);
  }
  topology_mgr_->RemoveDS(1, 2, &state_changed);
  topology_mgr_->AddDS(1, 2, ni_ds2, true, &state_changed);
  topology_mgr_->AnnounceDSGState(1);
  DSGStateChangeFim& scfim = static_cast<DSGStateChangeFim&>(*fim);
  EXPECT_EQ(1U, scfim->ds_group);
  EXPECT_EQ(3U, scfim->state_change_id);
  EXPECT_EQ(2U, scfim->failed);
  EXPECT_EQ('\1', scfim->opt_resync);
  EXPECT_EQ(kDSGRecovering, scfim->state);
  EXPECT_FALSE(scfim->ready);

  // Non-optimized
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(1, _, kNumDSPerGroup));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket_));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));

  topology_mgr_->RemoveDS(1, 2, &state_changed);
  topology_mgr_->AddDS(1, 2, ni_ds2, false, &state_changed);
  topology_mgr_->AnnounceDSGState(1);
  DSGStateChangeFim& scfim2 = static_cast<DSGStateChangeFim&>(*fim);
  EXPECT_EQ('\0', scfim2->opt_resync);
}

TEST_F(TopologyTest, AllDSGStartable) {
  // Cannot start because of insufficient DS
  NodeInfo ni;
  ni.ip = 1;
  ni.port = 2;
  bool state_changed;
  for (GroupRole r = 0; r < kNumDSPerGroup - 2; ++r)
    topology_mgr_->AddDS(0, r, ni, false, &state_changed);
  for (GroupRole r = 0; r < kNumDSPerGroup - 1; ++r)
    topology_mgr_->AddDS(1, r, ni, false, &state_changed);
  EXPECT_FALSE(topology_mgr_->AllDSGStartable());

  // Can start
  topology_mgr_->AddDS(0, kNumDSPerGroup - 2, ni, false, &state_changed);
  EXPECT_TRUE(topology_mgr_->AllDSGStartable());

  // Cannot start because of failure
  topology_mgr_->AddDS(0, kNumDSPerGroup - 1, ni, false, &state_changed);
  topology_mgr_->RemoveDS(0, 0, &state_changed);
  topology_mgr_->RemoveDS(0, kNumDSPerGroup - 1, &state_changed);
  topology_mgr_->AddDS(0, 0, ni, false, &state_changed);
  EXPECT_FALSE(topology_mgr_->AllDSGStartable());
}

TEST_F(TopologyTest, ForceStartDSG) {
  NodeInfo ni;
  ni.ip = 1;
  ni.port = 2;
  bool state_changed;
  for (GroupRole r = 0; r < kNumDSPerGroup - 2; ++r)
    topology_mgr_->AddDS(0, r, ni, false, &state_changed);
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    topology_mgr_->AddDS(1, r, ni, false, &state_changed);
  EXPECT_FALSE(topology_mgr_->ForceStartDSG());

  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, kNumDSPerGroup));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));

  PrepareDSG(0, kNumDSPerGroup - 1);
  PrepareDSG(1, kNumDSPerGroup);
  topology_mgr_->AddDS(0, kNumDSPerGroup - 2, ni, false, &state_changed);
  EXPECT_TRUE(topology_mgr_->ForceStartDSG());
}

TEST_F(TopologyTest, AddRemoveFC) {
  ClientNum fc_id;
  EXPECT_THAT(topology_mgr_->GetFCs(), ElementsAre());
  EXPECT_TRUE(topology_mgr_->SuggestFCId(&fc_id));
  EXPECT_EQ(0U, fc_id);
  EXPECT_TRUE(topology_mgr_->SuggestFCId(&fc_id));
  EXPECT_EQ(1U, fc_id);
  NodeInfo fc_info[2];
  fc_info[0].ip = 10000;
  fc_info[0].port = 1234U;
  fc_info[0].pid = 1001;
  fc_info[1].ip = 10001;
  fc_info[1].port = 1234U;
  fc_info[1].pid = 1002;
  EXPECT_TRUE(topology_mgr_->AddFC(1, fc_info[0]));
  EXPECT_TRUE(topology_mgr_->AddFC(1, fc_info[0]));
  EXPECT_FALSE(topology_mgr_->AddFC(1, fc_info[1]));
  EXPECT_TRUE(topology_mgr_->AddFC(2, fc_info[0]));
  EXPECT_TRUE(topology_mgr_->AddFC(3, fc_info[1]));
  topology_mgr_->RemoveFC(1);
  EXPECT_THAT(topology_mgr_->GetFCs(), ElementsAre(2, 3));
  EXPECT_TRUE(topology_mgr_->SuggestFCId(&fc_id));
  EXPECT_EQ(4U, fc_id);
  topology_mgr_->SetNextFCId(1);
  EXPECT_TRUE(topology_mgr_->SuggestFCId(&fc_id));
  EXPECT_EQ(1U, fc_id);
  EXPECT_TRUE(topology_mgr_->AddFC(0, fc_info[1]));
  EXPECT_TRUE(topology_mgr_->AddFC(1, fc_info[1]));
  EXPECT_EQ(unsigned(4), topology_mgr_->GetNumFCs());
}

TEST_F(TopologyTest, AnnounceFC) {
  // No standby case
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));
  topology_mgr_->AnnounceFC(5, true);

  // With standby case
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket_));
  FIM_PTR<IFim> ms_fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim));

  NodeInfo fc_info;
  fc_info.ip = 10000;
  fc_info.port = 1234U;
  fc_info.pid = 1001;
  EXPECT_TRUE(topology_mgr_->AddFC(5, fc_info));
  topology_mgr_->AnnounceFC(5, true);
  TopologyChangeFim& tcfim = dynamic_cast<TopologyChangeFim&>(*ms_fim);
  EXPECT_EQ('F', tcfim->type);
  EXPECT_EQ('\x01', tcfim->joined);
  EXPECT_EQ(5U, tcfim->client_num);
  EXPECT_EQ(10000U, tcfim->ip);

  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket_));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ms_fim));

  topology_mgr_->AnnounceFC(5, false);
  TopologyChangeFim& tcfim2 = dynamic_cast<TopologyChangeFim&>(*ms_fim);
  EXPECT_EQ('F', tcfim2->type);
  EXPECT_EQ('\x00', tcfim2->joined);
  EXPECT_EQ(5U, tcfim2->client_num);
}

TEST_F(TopologyTest, SendAllFCInfo) {
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket_));

  FIM_PTR<IFim> fim1;
  FIM_PTR<IFim> fim2;
  NodeInfo fc_info[2];
  fc_info[0].ip = 3210;
  fc_info[0].port = 1234U;
  fc_info[0].pid = 2000;
  fc_info[1].ip = 12345;
  fc_info[1].port = 1234U;
  fc_info[1].pid = 2001;
  EXPECT_TRUE(topology_mgr_->AddFC(1, fc_info[0]));
  EXPECT_TRUE(topology_mgr_->AddFC(20, fc_info[1]));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim1))
      .WillOnce(SaveArg<0>(&fim2));
  topology_mgr_->SendAllFCInfo();

  TopologyChangeFim& rfim1 = dynamic_cast<TopologyChangeFim&>(*fim1);
  TopologyChangeFim& rfim2 = dynamic_cast<TopologyChangeFim&>(*fim2);
  EXPECT_EQ('F', rfim1->type);
  EXPECT_EQ(1U, rfim1->client_num);
  EXPECT_EQ(3210U, rfim1->ip);
  EXPECT_EQ('F', rfim2->type);
  EXPECT_EQ(20U, rfim2->client_num);
  EXPECT_EQ(12345U, rfim2->ip);
}

TEST_F(TopologyTest, SetDSGState) {
  topology_mgr_->SetDSGState(0, 2, kDSGDegraded, 1);
  GroupRole failed;
  EXPECT_EQ(kDSGDegraded, topology_mgr_->GetDSGState(0, &failed));
  EXPECT_TRUE(topology_mgr_->DSGReady(0));
  EXPECT_EQ(GroupRole(2), failed);

  // After setting to kDSGShuttingDown, Remove will not change it
  topology_mgr_->SetDSGState(0, 2, kDSGShuttingDown, 2);
  bool state_changed;
  topology_mgr_->RemoveDS(0, 0, &state_changed);
  EXPECT_FALSE(state_changed);
}

TEST_F(TopologyTest, AckDSShutdown) {
  PrepareDSG(1, kNumDSPerGroup);
  NodeInfo ni[5];
  ni[0].ip = 1;
  ni[0].port = 1;
  ni[1].ip = 1;
  ni[1].port = 2;
  ni[2].ip = 1;
  ni[2].port = 3;
  ni[3].ip = 1;
  ni[3].port = 4;
  ni[4].ip = 1;
  ni[4].port = 5;
  bool state_changed;
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(1, r, ni[r], false, &state_changed));

  // Failed one DS. AckDSShutdown() returns true when 4 DS ack'ed
  topology_mgr_->RemoveDS(1, 2, &state_changed);

  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    if (r == 2)  // The failed DS
      continue;
    EXPECT_CALL(*tracker_mapper_, FindDSRole(
        boost::static_pointer_cast<IFimSocket>(ds_fim_socket_[r]), _, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(1),
                              SetArgPointee<2>(r),
                              Return(true)));
    if (r == (kNumDSPerGroup - 1))
      EXPECT_TRUE(topology_mgr_->AckDSShutdown(ds_fim_socket_[r]));
    else
      EXPECT_FALSE(topology_mgr_->AckDSShutdown(ds_fim_socket_[r]));
  }

  // Error case: No such DS group
  boost::shared_ptr<IFimSocket> dummy_socket;
  EXPECT_CALL(*tracker_mapper_, FindDSRole(dummy_socket, _, _));
  EXPECT_FALSE(topology_mgr_->AckDSShutdown(dummy_socket));
}

TEST_F(TopologyTest, StartStopWorker) {
  // DS not ready: stop
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*thread_group_, Stop(2000));

  topology_mgr_->StartStopWorker();

  // Both MS and DS ready: start
  EXPECT_CALL(*thread_group_, Start());

  NodeInfo ni;
  ni.ip = 1;
  ni.port = 1;
  bool state_changed;
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    topology_mgr_->AddDS(0, r, ni, false, &state_changed);
    topology_mgr_->AddDS(1, r, ni, false, &state_changed);
  }
  topology_mgr_->StartStopWorker();

  // MS not ready: stop
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateResync));
  EXPECT_CALL(*thread_group_, Stop(2000));

  topology_mgr_->StartStopWorker();
}

TEST_F(TopologyTest, AnnounceShutdown) {
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket_));
  FIM_PTR<IFim> fim1, fim2, fim3;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim1))
      .WillOnce(SaveArg<0>(&fim2))
      .WillOnce(SaveArg<0>(&fim3));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(_, _, _))
      .Times(kNumDSGroups);

  topology_mgr_->AnnounceShutdown();
  EXPECT_EQ(kSysShutdownReqFim, fim1->type());
  EXPECT_EQ(kDSGStateChangeFim, fim2->type());  // Group 0
  EXPECT_EQ(kDSGStateChangeFim, fim3->type());  // Group 1
}

TEST_F(TopologyTest, AnnounceHalt) {
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket_));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_));
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(_, _, _))
      .Times(kNumDSGroups);

  topology_mgr_->AnnounceHalt();
}

TEST_F(TopologyTest, GetDSInfos) {
  NodeInfo ni[2];
  ni[0].ip = 1;
  ni[0].port = 2;
  ni[1].ip = 3;
  ni[1].port = 4;
  PrepareDSG(0, 1);
  PrepareDSG(1, 1);
  bool state_changed;
  EXPECT_TRUE(topology_mgr_->AddDS(0, 1, ni[0], false, &state_changed));
  EXPECT_TRUE(topology_mgr_->AddDS(1, 1, ni[1], false, &state_changed));
  std::vector<NodeInfo> infos = topology_mgr_->GetDSInfos();
  EXPECT_EQ(2U, infos.size());
}

TEST_F(TopologyTest, GetFCInfos) {
  NodeInfo info[3];
  info[0].ip = 10000;
  info[0].port = 1234U;
  info[0].pid = 1000;
  info[1].ip = 10001;
  info[1].port = 1235U;
  info[1].pid = 1001;
  info[2].ip = 10002;
  info[2].port = 1236U;
  info[2].pid = 1002;
  topology_mgr_->AddFC(1, info[0]);
  topology_mgr_->AddFC(2, info[1]);
  topology_mgr_->AddFC(3, info[2]);
  topology_mgr_->SetFCTerminating(3);
  std::vector<NodeInfo> infos = topology_mgr_->GetFCInfos();
  EXPECT_EQ(2U, infos.size());
}

TEST_F(TopologyTest, DSSpaceStat) {
  // Not distressed
  AllDSSpaceStat stat;
  stat.resize(1);
  stat[0].resize(kNumDSGroups);
  stat[0][0].online = false;
  for (GroupRole r = 1; r < kNumDSGroups; ++r) {
    stat[0][r].online = true;
    stat[0][r].total_space = uint64_t(100) * 1024 * 1024 * 1024;
    stat[0][r].free_space = uint64_t(41) * 1024 * 1024 * 1024;
  }
  stat[0][1].free_space = uint64_t(21) * 1024 * 1024 * 1024;
  callback(stat);

  // Become distressed when one has low free space
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, kNumDSPerGroup))
      .WillOnce(SaveArg<1>(&fim));

  stat[0][1].free_space = uint64_t(19) * 1024 * 1024 * 1024;
  callback(stat);
  DSGDistressModeChangeFim& r_fim =
      static_cast<DSGDistressModeChangeFim&>(*fim);
  EXPECT_TRUE(r_fim->distress);

  // Maintain to be distressed even when going back
  stat[0][1].free_space = uint64_t(21) * 1024 * 1024 * 1024;
  callback(stat);

  // Unless a lot of free space is available, then distress is switched off
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, kNumDSPerGroup))
      .WillOnce(SaveArg<1>(&fim));

  stat[0][1].free_space = uint64_t(41) * 1024 * 1024 * 1024;
  callback(stat);
  DSGDistressModeChangeFim& r_fim2 =
      static_cast<DSGDistressModeChangeFim&>(*fim);
  EXPECT_FALSE(r_fim2->distress);
}

TEST_F(TopologyTest, SetNumDSGRoups) {
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket_));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  // Set number of groups from 2 to 3
  EXPECT_CALL(*server_info_, Set("max-ndsg", "3"));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  topology_mgr_->set_num_groups(GroupId(3));
  EXPECT_EQ(GroupId(3), topology_mgr_->num_groups());
  Mock::VerifyAndClear(server_info_);

  // Set number of groups to 3 again, nothing is changed
  topology_mgr_->set_num_groups(GroupId(3));

  // Join DS group 0 and 1
  EXPECT_FALSE(topology_mgr_->AllDSReady());
  PrepareDSG(0, kNumDSPerGroup);
  PrepareDSG(1, kNumDSPerGroup);
  NodeInfo ni[5];
  ni[0].ip = 1;
  ni[0].port = 1;
  ni[1].ip = 1;
  ni[1].port = 2;
  ni[2].ip = 1;
  ni[2].port = 3;
  ni[3].ip = 1;
  ni[3].port = 4;
  ni[4].ip = 1;
  ni[4].port = 5;
  bool state_changed;
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(0, r, ni[r], false, &state_changed));
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(1, r, ni[r], false, &state_changed));
  // New group 2 is not joined, but CPFS DS group is regarded as functional
  EXPECT_TRUE(topology_mgr_->AllDSReady());

  // Join DS group 2
  PrepareDSG(2, kNumDSPerGroup);
  EXPECT_CALL(*server_info_, Set("max-dsg-id", "2"));
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_TRUE(topology_mgr_->AddDS(2, r, ni[r], false, &state_changed));
  EXPECT_TRUE(topology_mgr_->AllDSReady());

  // Set number of groups to 4
  EXPECT_CALL(*server_info_, Set("max-ndsg", "4"));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  topology_mgr_->set_num_groups(GroupId(4));
  EXPECT_EQ(GroupId(4), topology_mgr_->num_groups());
  EXPECT_TRUE(topology_mgr_->AllDSReady());

  // Remove newly creating DS group 3
  EXPECT_CALL(*stat_keeper_, Run());
  // StartStopWorker()
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*thread_group_, Start());
  // StateNotify
  EXPECT_CALL(*tracker_mapper_, FCBroadcast(_));
  EXPECT_CALL(*server_info_, Set("max-ndsg", "3"));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  // Real call
  topology_mgr_->set_num_groups(GroupId(3));
  EXPECT_EQ(GroupId(3), topology_mgr_->num_groups());
  EXPECT_TRUE(topology_mgr_->AllDSReady());

  // Error case: Set number of groups to 0, nothing is changed
  topology_mgr_->set_num_groups(GroupId(1));
  EXPECT_EQ(GroupId(3), topology_mgr_->num_groups());
  EXPECT_TRUE(topology_mgr_->AllDSReady());

  // Error case: Add number of groups > 1
  topology_mgr_->set_num_groups(GroupId(10));
  EXPECT_EQ(GroupId(3), topology_mgr_->num_groups());
  EXPECT_TRUE(topology_mgr_->AllDSReady());
}

TEST_F(TopologyTest, SetDSGDistressed) {
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*tracker_mapper_, DSGBroadcast(0, _, kNumDSPerGroup))
      .WillOnce(SaveArg<1>(&fim));

  topology_mgr_->SetDSGDistressed(0, true);

  DSGDistressModeChangeFim& r_fim =
      static_cast<DSGDistressModeChangeFim&>(*fim);
  EXPECT_TRUE(r_fim->distress);

  // No change
  topology_mgr_->SetDSGDistressed(0, true);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
