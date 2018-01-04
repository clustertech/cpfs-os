/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/admin_fim_processor.hpp"

#include <stdint.h>

#include <cstddef>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "admin_info.hpp"
#include "base_ms_mock.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker_mock.hpp"
#include "shutdown_mgr_mock.hpp"
#include "state_mgr_mock.hpp"
#include "store_mock.hpp"
#include "topology_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ms/ds_query_mgr_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StartsWith;
using ::testing::Throw;

namespace cpfs {
namespace server {
namespace ms {
namespace {

ConfigMgr InitConfig() {
  ConfigMgr ret;
  ret.set_role("MS1");
  ret.set_log_severity("5");
  ret.set_log_path("/dev/null");
  return ret;
}

class MSAdminFimProcessorTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  MockIDSQueryMgr* ds_query_mgr_;
  MockIStore* store_;
  MockIStateMgr* state_mgr_;
  MockITopologyMgr* topology_;
  MockIShutdownMgr* shutdown_mgr_;
  MockITrackerMapper* tracker_mapper_;
  boost::scoped_ptr<IFimProcessor> proc_;
  boost::shared_ptr<MockIReqTracker> ms_tracker_;
  boost::shared_ptr<MockIReqTracker> ds_tracker_;
  boost::shared_ptr<MockIReqEntry> req_entry_;
  FIM_PTR<IFim> reply_;

  MSAdminFimProcessorTest()
      : server_(InitConfig()),
        proc_(MakeAdminFimProcessor(&server_)),
        ms_tracker_(new MockIReqTracker),
        ds_tracker_(new MockIReqTracker),
        req_entry_(new MockIReqEntry) {
    server_.set_ds_query_mgr(ds_query_mgr_ = new MockIDSQueryMgr);
    server_.set_store(store_ = new MockIStore);
    server_.set_state_mgr(state_mgr_ = new MockIStateMgr);
    server_.set_topology_mgr(topology_ = new MockITopologyMgr);
    server_.set_shutdown_mgr(shutdown_mgr_ = new MockIShutdownMgr);
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    FIM_PTR<ResultCodeReplyFim> reply = ResultCodeReplyFim::MakePtr();
    (*reply)->err_no = 0;
    reply_ = reply;
  }
};

TEST_F(MSAdminFimProcessorTest, StatFS) {
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<FCStatFSFim> req_fim = FCStatFSFim::MakePtr();
  req_fim->set_req_id(1);

  EXPECT_CALL(*topology_, num_groups())
      .WillRepeatedly(Return(1));
  DSReplies replies;
  for (GroupRole role = 0; role < kNumDSPerGroup; ++role) {
    FIM_PTR<FCStatFSReplyFim> fim_reply = FCStatFSReplyFim::MakePtr();
    (*fim_reply)->total_space = 10;
    (*fim_reply)->free_space = 1;
    replies[0][role] = fim_reply;
  }
  EXPECT_CALL(*ds_query_mgr_, Request(_, _))
      .WillOnce(Return(replies));

  EXPECT_CALL(*store_, Stat(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(10000),
                      SetArgPointee<1>(8000),
                      Return(0)));

  FIM_PTR<IFim> result;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result));
  proc_->Process(req_fim, fim_socket);
  Sleep(0.2)();

  const FCStatFSReplyFim& rresult = static_cast<FCStatFSReplyFim&>(*result);
  EXPECT_EQ(40U, rresult->total_space);
  EXPECT_EQ(4U, rresult->free_space);
  EXPECT_EQ(10000U, rresult->total_inodes);
  EXPECT_EQ(8000U, rresult->free_inodes);

  // Unprocessed Fim
  EXPECT_FALSE(proc_->Process(UnlinkFim::MakePtr(), fim_socket));
}

TEST_F(MSAdminFimProcessorTest, StatFSError) {
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<FCStatFSFim> req_fim = FCStatFSFim::MakePtr();
  req_fim->set_req_id(1);

  EXPECT_CALL(*topology_, num_groups())
      .WillRepeatedly(Return(1));
  // Empty replies
  DSReplies replies;
  EXPECT_CALL(*ds_query_mgr_, Request(_, _))
      .WillOnce(Return(replies));

  // Failed to obtain inode statistics
  EXPECT_CALL(*store_, Stat(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(10000),
                      SetArgPointee<1>(8000),
                      Return(-1)));

  FIM_PTR<IFim> result;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result));
  proc_->Process(req_fim, fim_socket);
  Sleep(0.2)();

  const ResultCodeReplyFim& rresult = static_cast<ResultCodeReplyFim&>(*result);
  EXPECT_EQ(1U, rresult->err_no);
}

TEST_F(MSAdminFimProcessorTest, DSGStateChangeReady) {
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<DSGStateChangeReadyFim> req_fim = DSGStateChangeReadyFim::MakePtr();
  req_fim->set_req_id(1);

  MockIReqTracker tracker;
  EXPECT_CALL(*fim_socket, GetReqTracker())
      .WillRepeatedly(Return(&tracker));
  EXPECT_CALL(tracker, peer_client_num())
      .WillRepeatedly(Return(42));
  EXPECT_CALL(*topology_, AckDSGStateChangeWait(42));

  proc_->Process(req_fim, fim_socket);
}

TEST_F(MSAdminFimProcessorTest, ClusterStatus) {
  std::vector<NodeInfo> fc_infos(1);
  fc_infos[0].ip = 3;
  fc_infos[0].port = 4;
  fc_infos[0].pid = 2000;
  fc_infos[0].SetAlias(ClientNum(10));
  EXPECT_CALL(*topology_, GetFCInfos())
      .WillOnce(Return(fc_infos));
  std::vector<NodeInfo> ds_infos(2);
  ds_infos[0].ip = 1;
  ds_infos[0].port = 2;
  ds_infos[0].pid = 1000;
  ds_infos[0].SetAlias(0, 2);
  ds_infos[1].ip = 1;
  ds_infos[1].port = 2;
  ds_infos[1].pid = 1000;
  ds_infos[1].SetAlias(0, 3);
  EXPECT_CALL(*topology_, GetDSInfos())
      .WillOnce(Return(ds_infos));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateActive));
  EXPECT_CALL(*topology_, num_groups())
      .WillOnce(Return(2));
  EXPECT_CALL(*topology_, GetDSGState(0, _))
      .WillOnce(Return(kDSGReady));
  EXPECT_CALL(*topology_, GetDSGState(1, _))
      .WillOnce(Return(kDSGPending));
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<IFim> result;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));

  FIM_PTR<ClusterStatusReqFim> req_fim = ClusterStatusReqFim::MakePtr();
  req_fim->set_req_id(1);
  proc_->Process(req_fim, fim_socket);

  const ClusterStatusReplyFim& rresult =
      static_cast<const ClusterStatusReplyFim&>(*result);
  EXPECT_EQ(kStateDegraded, rresult->ms_state);
  EXPECT_EQ(1U, rresult->ms_role);
  EXPECT_EQ(2U, rresult->num_dsg);
  const uint8_t* dsg_states
      = reinterpret_cast<const uint8_t*>(rresult.tail_buf());
  EXPECT_EQ(kDSGReady, DSGroupState(dsg_states[0]));
  EXPECT_EQ(kDSGPending, DSGroupState(dsg_states[1]));
  const NodeInfo* node_info = reinterpret_cast<const NodeInfo*>(
      rresult.tail_buf() + sizeof(dsg_states[0]) * 2);
  EXPECT_EQ(ds_infos[0], node_info[0]);
  EXPECT_EQ(ds_infos[1], node_info[1]);
  EXPECT_EQ(fc_infos[0], node_info[2]);
}

TEST_F(MSAdminFimProcessorTest, DiskInfo) {
  DSReplies replies;
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<ClusterDiskInfoReqFim> req_fim = ClusterDiskInfoReqFim::MakePtr();
  req_fim->set_req_id(1);

  EXPECT_CALL(*topology_, num_groups())
      .WillRepeatedly(Return(1));
  for (GroupRole role = 0; role < 5; ++role) {
    if (role == 3)  // Skipping DS 3
      continue;
    FIM_PTR<FCStatFSReplyFim> fim_reply = FCStatFSReplyFim::MakePtr();
    (*fim_reply)->total_space = 10;
    (*fim_reply)->free_space = role + 1;
    replies[0][role] = fim_reply;
  }
  EXPECT_CALL(*ds_query_mgr_, Request(_, _)).WillOnce(Return(replies));

  FIM_PTR<IFim> result;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result));
  proc_->Process(req_fim, fim_socket);

  const ClusterDiskInfoReplyFim& rresult =
      reinterpret_cast<const ClusterDiskInfoReplyFim&>(*result);

  const DSDiskInfo* all_disk_info
      = reinterpret_cast<const DSDiskInfo*>(rresult.tail_buf());

  EXPECT_EQ(std::string("DS 0-0"), std::string(all_disk_info[0].alias));
  EXPECT_EQ(10U, all_disk_info[0].total);
  EXPECT_EQ(1U, all_disk_info[0].free);
  EXPECT_EQ(std::string("DS 0-1"), std::string(all_disk_info[1].alias));
  EXPECT_EQ(10U, all_disk_info[1].total);
  EXPECT_EQ(2U, all_disk_info[1].free);
  EXPECT_EQ(std::string("DS 0-2"), std::string(all_disk_info[2].alias));
  EXPECT_EQ(10U, all_disk_info[2].total);
  EXPECT_EQ(3U, all_disk_info[2].free);
  EXPECT_EQ(std::string("DS 0-4"), std::string(all_disk_info[3].alias));
  EXPECT_EQ(10U, all_disk_info[3].total);
  EXPECT_EQ(5U, all_disk_info[3].free);
}

TEST_F(MSAdminFimProcessorTest, ConfigList) {
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<MockIFimSocket> ms_fim_socket =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<ClusterConfigListReqFim> req_fim = ClusterConfigListReqFim::MakePtr();
  req_fim->set_req_id(1);

  EXPECT_CALL(*topology_, num_groups())
      .WillRepeatedly(Return(1));
  // Prepare reply
  std::vector<ConfigItem> ret;
  ret.push_back(ConfigItem("log_severity", "6"));
  std::size_t config_info_size = ret.size() * sizeof(ConfigItem);
  FIM_PTR<ClusterConfigListReplyFim> reply
      = ClusterConfigListReplyFim::MakePtr(config_info_size);
  std::memcpy(reply->tail_buf(), ret.data(), config_info_size);

  // Peer MS reply
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillRepeatedly(Return(ms_tracker_));
  EXPECT_CALL(*ms_tracker_, GetFimSocket())
      .WillRepeatedly(Return(ms_fim_socket));
  FIM_PTR<IFim> req1;
  EXPECT_CALL(*ms_tracker_, AddRequest(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req1),
                      Return(req_entry_)));
  FIM_PTR<IFim> reply_r = boost::static_pointer_cast<IFim>(reply);
  EXPECT_CALL(*req_entry_, WaitReply())
      .WillOnce(ReturnRef(reply_r));

  // DS group replies
  DSReplies replies;
  for (GroupRole role = 0; role < kNumDSPerGroup; ++role)
    replies[0][role] = reply;

  EXPECT_CALL(*ds_query_mgr_, Request(_, _))
      .WillOnce(Return(replies));

  FIM_PTR<IFim> result;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result));
  // Actual call
  proc_->Process(req_fim, fim_socket);

  const ClusterConfigListReplyFim& rresult =
      reinterpret_cast<const ClusterConfigListReplyFim&>(*result);
  const ConfigItem* configs
      = reinterpret_cast<const ConfigItem*>(rresult.tail_buf());
  // MS1
  EXPECT_EQ(std::string(kNodeCfg), configs[0].name);
  EXPECT_EQ(std::string("MS1"), configs[0].value);
  EXPECT_EQ(std::string("log_severity"), configs[1].name);
  EXPECT_EQ(std::string("5"), configs[1].value);
  EXPECT_EQ(std::string("log_path"), configs[2].name);
  EXPECT_EQ(std::string("/dev/null"), configs[2].value);
  EXPECT_EQ(std::string("num_ds_groups"), configs[3].name);
  EXPECT_EQ(std::string("1"), configs[3].value);
  // MS2
  EXPECT_EQ(std::string(kNodeCfg), configs[4].name);
  EXPECT_EQ(std::string("MS2"), configs[4].value);
  EXPECT_EQ(std::string("log_severity"), configs[5].name);
  EXPECT_EQ(std::string("6"), configs[5].value);
  // DS0
  EXPECT_EQ(std::string(kNodeCfg), configs[6].name);
  EXPECT_EQ(std::string("DS 0-0"), configs[6].value);
  EXPECT_EQ(std::string("log_severity"), configs[7].name);
  EXPECT_EQ(std::string("6"), configs[7].value);
  // DS1
  EXPECT_EQ(std::string(kNodeCfg), configs[8].name);
  EXPECT_EQ(std::string("DS 0-1"), configs[8].value);
  EXPECT_EQ(std::string("log_severity"), configs[9].name);
  EXPECT_EQ(std::string("6"), configs[9].value);

  Mock::VerifyAndClear(ms_tracker_.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSAdminFimProcessorTest, ConfigChange) {
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  // log_severity
  FIM_PTR<ClusterConfigChangeReqFim> req_fim1
      = ClusterConfigChangeReqFim::MakePtr();
  req_fim1->set_req_id(1);
  strncpy((*req_fim1)->target, "MS1", 4);
  strncpy((*req_fim1)->name, "log_severity", 13);
  strncpy((*req_fim1)->value, "7", 2);
  FIM_PTR<IFim> result1;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result1));
  // Actual call
  proc_->Process(req_fim1, fim_socket);
  EXPECT_EQ("7", server_.configs().log_severity());
  EXPECT_EQ(kClusterConfigChangeReplyFim, result1->type());

  // log_path
  FIM_PTR<ClusterConfigChangeReqFim> req_fim2
      = ClusterConfigChangeReqFim::MakePtr();
  req_fim2->set_req_id(2);
  strncpy((*req_fim2)->target, "MS1", 4);
  strncpy((*req_fim2)->name, "log_path", 9);
  strncpy((*req_fim2)->value, "/dev/null", 10);
  FIM_PTR<IFim> result2;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result2));
  // Actual call
  proc_->Process(req_fim2, fim_socket);
  EXPECT_EQ(std::string("/dev/null"), server_.configs().log_path());
  EXPECT_EQ(kClusterConfigChangeReplyFim, result2->type());

  // num_ds_groups
  FIM_PTR<ClusterConfigChangeReqFim> req_fim3
      = ClusterConfigChangeReqFim::MakePtr();
  req_fim3->set_req_id(2);
  strncpy((*req_fim3)->target, "MS1", 4);
  strncpy((*req_fim3)->name, "num_ds_groups", 14);
  strncpy((*req_fim3)->value, "3", 2);
  FIM_PTR<IFim> result3;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result3));
  EXPECT_CALL(*topology_, set_num_groups(3));
  // Actual call
  proc_->Process(req_fim3, fim_socket);
  EXPECT_EQ(kClusterConfigChangeReplyFim, result3->type());

  FIM_PTR<IFim> req1, req2;
  // MS2
  FIM_PTR<ClusterConfigChangeReqFim> req_fim4
      = ClusterConfigChangeReqFim::MakePtr();
  req_fim4->set_req_id(3);
  strncpy((*req_fim4)->target, "MS2", 4);
  strncpy((*req_fim4)->name, "log_path", 9);
  strncpy((*req_fim4)->value, "/dev/null", 10);
  FIM_PTR<IFim> result4;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result4));
  // Mock for AddRequest()
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillRepeatedly(Return(ms_tracker_));
  EXPECT_CALL(*ms_tracker_, AddRequest(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req1),
                      Return(req_entry_)));
  EXPECT_CALL(*req_entry_, WaitReply())
      .WillOnce(ReturnRef(reply_));
  // Real call for MS
  proc_->Process(req_fim4, fim_socket);

  // DS 0-1
  FIM_PTR<ClusterConfigChangeReqFim> req_fim5
      = ClusterConfigChangeReqFim::MakePtr();
  req_fim5->set_req_id(4);
  strncpy((*req_fim5)->target, "DS 0-1", 7);
  strncpy((*req_fim5)->name, "log_path", 9);
  strncpy((*req_fim5)->value, "/dev/null", 10);
  FIM_PTR<IFim> result5;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result5));
  // Mock for AddRequest()
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 1))
      .WillRepeatedly(Return(ds_tracker_));
  EXPECT_CALL(*ds_tracker_, AddRequest(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req2),
                      Return(req_entry_)));
  EXPECT_CALL(*req_entry_, WaitReply())
      .WillOnce(ReturnRef(reply_));
  // Real call for DS
  proc_->Process(req_fim5, fim_socket);

  Mock::VerifyAndClear(tracker_mapper_);
  Mock::VerifyAndClear(req_entry_.get());
  Mock::VerifyAndClear(ds_tracker_.get());
  Mock::VerifyAndClear(ms_tracker_.get());
}

TEST_F(MSAdminFimProcessorTest, ConfigChangeError) {
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  // non-existing config
  FIM_PTR<ClusterConfigChangeReqFim> req_fim
      = ClusterConfigChangeReqFim::MakePtr();
  req_fim->set_req_id(1);
  strncpy((*req_fim)->target, "MS1", 4);
  strncpy((*req_fim)->name, "non-existing", 13);
  strncpy((*req_fim)->value, "123", 4);
  FIM_PTR<IFim> result;
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result));
  // Actual call
  proc_->Process(req_fim, fim_socket);
  EXPECT_EQ(kResultCodeReplyFim, result->type());

  // MS2 not connected
  FIM_PTR<ClusterConfigChangeReqFim> req_fim2
      = ClusterConfigChangeReqFim::MakePtr();
  req_fim2->set_req_id(2);
  strncpy((*req_fim2)->target, "MS2", 4);
  strncpy((*req_fim2)->name, "log_path", 9);
  strncpy((*req_fim2)->value, "/dev/null", 10);
  EXPECT_CALL(*fim_socket, WriteMsg(_)).WillOnce(SaveArg<0>(&result));
  // Mock for AddRequest()
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillRepeatedly(Return(boost::shared_ptr<IReqTracker>()));
  // Actual call
  proc_->Process(req_fim2, fim_socket);
  EXPECT_EQ(kResultCodeReplyFim, result->type());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSAdminFimProcessorTest, SystemShutdown) {
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();

  FIM_PTR<ClusterShutdownReqFim> req_fim1 = ClusterShutdownReqFim::MakePtr();
  req_fim1->set_req_id(1);

  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  EXPECT_CALL(*shutdown_mgr_, Init(_));

  // Actual call
  proc_->Process(req_fim1, fim_socket);
  EXPECT_EQ(kClusterShutdownReplyFim, reply->type());
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
