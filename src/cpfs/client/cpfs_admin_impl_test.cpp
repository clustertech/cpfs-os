/* Copyright 2014 ClusterTech Ltd */
#include "client/cpfs_admin.hpp"

#include <stdint.h>

#include <cstddef>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "admin_info.hpp"
#include "common.hpp"
#include "conn_mgr_mock.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "io_service_runner_mock.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "util.hpp"
#include "client/base_client.hpp"
#include "client/cpfs_admin_impl.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::SaveArg;

namespace cpfs {
namespace client {
namespace {

boost::shared_ptr<MockIReqEntry> PrepareAddRequest(
    boost::shared_ptr<MockIReqTracker> tracker,
    FIM_PTR<IFim>* req_fim,
    const FIM_PTR<IFim>& reply_fim) {
  boost::shared_ptr<MockIReqEntry> ret(new MockIReqEntry);
  EXPECT_CALL(*tracker, AddRequest(_, _))
      .WillOnce(DoAll(SaveArg<0>(req_fim),
                                 Return(ret)));
  EXPECT_CALL(*ret, WaitReply())
      .WillOnce(ReturnRef(reply_fim));
  return ret;
}

class CpfsAdminTest : public ::testing::Test {
 protected:
  BaseAdminClient client_;
  MockIOServiceRunner* service_runer_;
  MockITrackerMapper* tracker_mapper_;
  MockIConnMgr* conn_mgr_;
  boost::scoped_ptr<ICpfsAdmin> cpfs_admin_;

  CpfsAdminTest()
      : cpfs_admin_(MakeCpfsAdmin(&client_)) {
    client_.set_service_runner(service_runer_ = new MockIOServiceRunner);
    client_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    client_.set_conn_mgr(conn_mgr_ = new MockIConnMgr);
  }
};

void WaitAdminClientReady(BaseAdminClient* client) {
  client->SetReady(true);
}

TEST_F(CpfsAdminTest, Init) {
  EXPECT_CALL(*service_runer_, Run());
  boost::thread th(boost::bind(&WaitAdminClientReady, &client_));
  EXPECT_CALL(*conn_mgr_, GetMSRejectInfo())
      .WillOnce(Return(std::vector<bool>()));
  std::vector<bool> reject_info;
  cpfs_admin_->Init(1.0, &reject_info);
}

TEST_F(CpfsAdminTest, QueryStatus) {
  boost::shared_ptr<MockIReqTracker> req_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillOnce(Return(req_tracker));

  std::vector<uint8_t> dsg_states;
  dsg_states.push_back(kDSGReady);
  dsg_states.push_back(kDSGPending);

  std::vector<NodeInfo> node_infos(2);
  node_infos[0].ip = IPToInt("127.0.0.1");
  node_infos[0].port = 1234;
  node_infos[0].pid = 5000;
  node_infos[0].SetAlias(0, 1);
  node_infos[1].ip = IPToInt("192.168.0.1");
  node_infos[1].port = 2468;
  node_infos[1].pid = 5001;
  node_infos[1].SetAlias(0, 2);

  std::size_t node_infos_size = node_infos.size() * sizeof(NodeInfo);
  std::size_t dsg_state_size = dsg_states.size() * sizeof(DSGroupState);

  FIM_PTR<ClusterStatusReplyFim> reply
      = ClusterStatusReplyFim::MakePtr(dsg_state_size + node_infos_size);
  (*reply)->ms_state = kStateActive;
  (*reply)->state_changing = 1;
  (*reply)->ms_role = 2;
  (*reply)->num_dsg = 2;
  (*reply)->num_node_info = 2;

  std::memcpy(reply->tail_buf(), dsg_states.data(), dsg_states.size());
  std::memcpy(reply->tail_buf() + dsg_states.size(), node_infos.data(),
              node_infos_size);

  FIM_PTR<IFim> req_fim;
  boost::shared_ptr<MockIReqEntry> req_entry =
      PrepareAddRequest(req_tracker, &req_fim, reply);
  ClusterInfo info = cpfs_admin_->QueryStatus();

  EXPECT_EQ(std::string("Active (DSG state changing)"), info.ms_state);
  EXPECT_EQ(std::string("MS2"), info.ms_role);
  EXPECT_EQ(std::string("Ready"), info.dsg_states[0]);
  EXPECT_EQ(std::string("Pending"), info.dsg_states[1]);
  EXPECT_EQ(2U, info.node_infos.size());
  EXPECT_EQ(std::string("127.0.0.1"), info.node_infos[0]["IP"]);
  EXPECT_EQ(std::string("1234"), info.node_infos[0]["Port"]);
  EXPECT_EQ(std::string("5000"), info.node_infos[0]["PID"]);
  EXPECT_EQ(std::string("DS 0-1"), info.node_infos[0]["Alias"]);
  EXPECT_EQ(std::string("192.168.0.1"), info.node_infos[1]["IP"]);
  EXPECT_EQ(std::string("2468"), info.node_infos[1]["Port"]);
  EXPECT_EQ(std::string("5001"), info.node_infos[1]["PID"]);
  EXPECT_EQ(std::string("DS 0-2"), info.node_infos[1]["Alias"]);
  Mock::VerifyAndClear(req_entry.get());
  Mock::VerifyAndClear(req_tracker.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(CpfsAdminTest, QueryDiskInfo) {
  boost::shared_ptr<MockIReqTracker> req_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillOnce(Return(req_tracker));
  std::vector<DSDiskInfo> disk_info_list;
  for (GroupRole role = 0; role < 3; ++role) {
    DSDiskInfo disk_info;
    disk_info.SetAlias(0, role);
    disk_info.total = 2000;
    disk_info.free = 50 * role;
    disk_info_list.push_back(disk_info);
  }
  std::size_t disk_info_size = sizeof(DSDiskInfo) * disk_info_list.size();
  FIM_PTR<ClusterDiskInfoReplyFim> reply
      = ClusterDiskInfoReplyFim::MakePtr(disk_info_size);
  std::memcpy(reply->tail_buf(), disk_info_list.data(), disk_info_size);
  FIM_PTR<IFim> req_fim;
  boost::shared_ptr<MockIReqEntry> req_entry =
      PrepareAddRequest(req_tracker, &req_fim, reply);

  // Real call
  DiskInfoList ret = cpfs_admin_->QueryDiskInfo();

  EXPECT_EQ(3U, ret.size());
  EXPECT_EQ(std::string("DS 0-0"), ret[0]["Alias"]);
  EXPECT_EQ(std::string("2000 Bytes"), ret[0]["Total"]);
  EXPECT_EQ(std::string("0"), ret[0]["Free"]);
  EXPECT_EQ(std::string("DS 0-1"), ret[1]["Alias"]);
  EXPECT_EQ(std::string("2000 Bytes"), ret[1]["Total"]);
  EXPECT_EQ(std::string("50 Bytes"), ret[1]["Free"]);
  EXPECT_EQ(std::string("DS 0-2"), ret[2]["Alias"]);
  EXPECT_EQ(std::string("2000 Bytes"), ret[2]["Total"]);
  EXPECT_EQ(std::string("100 Bytes"), ret[2]["Free"]);

  Mock::VerifyAndClear(req_entry.get());
  Mock::VerifyAndClear(req_tracker.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(CpfsAdminTest, ListConfig) {
  boost::shared_ptr<MockIReqTracker> req_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillOnce(Return(req_tracker));
  std::vector<ConfigItem> configs;
  {
    ConfigItem cfg;
    strncpy(cfg.name, "log_severity", 12);
    strncpy(cfg.value, "5", 1);
    configs.push_back(cfg);
  }
  {
    ConfigItem cfg;
    strncpy(cfg.name, "log_path", 8);
    strncpy(cfg.value, "/dev/null", 9);
    configs.push_back(cfg);
  }
  std::size_t config_size = sizeof(ConfigItem) * configs.size();
  FIM_PTR<ClusterConfigListReplyFim> reply
      = ClusterConfigListReplyFim::MakePtr(config_size);
  std::memcpy(reply->tail_buf(), configs.data(), config_size);

  FIM_PTR<IFim> req_fim;
  boost::shared_ptr<MockIReqEntry> req_entry =
      PrepareAddRequest(req_tracker, &req_fim, reply);

  // Real call
  ConfigList ret = cpfs_admin_->ListConfig();

  EXPECT_EQ(std::string("log_severity"), ret[0].first);
  EXPECT_EQ(std::string("5"), ret[0].second);
  EXPECT_EQ(std::string("log_path"), ret[1].first);
  EXPECT_EQ(std::string("/dev/null"), ret[1].second);

  Mock::VerifyAndClear(req_entry.get());
  Mock::VerifyAndClear(req_tracker.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(CpfsAdminTest, ChangeConfig) {
  boost::shared_ptr<MockIReqTracker> req_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillRepeatedly(Return(req_tracker));

  // log_severity
  FIM_PTR<ClusterConfigChangeReplyFim> reply1
      = ClusterConfigChangeReplyFim::MakePtr();
  FIM_PTR<IFim> req_fim1;
  boost::shared_ptr<MockIReqEntry> req_entry1 =
      PrepareAddRequest(req_tracker, &req_fim1, reply1);
  // Real call
  EXPECT_TRUE(cpfs_admin_->ChangeConfig("MS1", "log_severity", "2"));
  // Verify
  const ClusterConfigChangeReqFim& rreq_fim1 =
      static_cast<const ClusterConfigChangeReqFim&>(*req_fim1);
  EXPECT_EQ(std::string("MS1"), std::string(rreq_fim1->target));
  EXPECT_EQ(std::string("log_severity"), std::string(rreq_fim1->name));
  EXPECT_EQ(std::string("2"), std::string(rreq_fim1->value));

  // non_existing
  FIM_PTR<ResultCodeReplyFim> reply2 = ResultCodeReplyFim::MakePtr();
  FIM_PTR<IFim> req_fim2;
  boost::shared_ptr<MockIReqEntry> req_entry2 =
      PrepareAddRequest(req_tracker, &req_fim2, reply2);
  // Real call
  EXPECT_FALSE(cpfs_admin_->ChangeConfig("MS1", "non_existing", "123"));

  Mock::VerifyAndClear(req_entry1.get());
  Mock::VerifyAndClear(req_entry2.get());
  Mock::VerifyAndClear(req_tracker.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(CpfsAdminTest, Shutdown) {
  boost::shared_ptr<MockIReqTracker> req_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillRepeatedly(Return(req_tracker));

  FIM_PTR<ClusterShutdownReplyFim> reply1
      = ClusterShutdownReplyFim::MakePtr();
  FIM_PTR<IFim> req_fim1;
  boost::shared_ptr<MockIReqEntry> req_entry1 =
      PrepareAddRequest(req_tracker, &req_fim1, reply1);
  // Real call
  EXPECT_TRUE(cpfs_admin_->SystemShutdown());

  Mock::VerifyAndClear(req_entry1.get());
  Mock::VerifyAndClear(req_tracker.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(CpfsAdminTest, ForceStart) {
  EXPECT_CALL(*conn_mgr_, SetForceStartMS(0));
  // Real call
  EXPECT_TRUE(cpfs_admin_->ForceStart(0));
}

}  // namespace
}  // namespace client
}  // namespace cpfs
