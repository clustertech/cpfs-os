/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/conn_mgr_impl.hpp"

#include <stdint.h>  // IWYU pragma: keep

#include <cstring>
#include <string>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_set.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "connector_mock.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "req_tracker_mock.hpp"
#include "shutdown_mgr_mock.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/conn_mgr.hpp"
#include "server/ms/failover_mgr_mock.hpp"
#include "server/ms/inode_src_mock.hpp"
#include "server/ms/inode_usage_mock.hpp"
#include "server/ms/resync_mgr_mock.hpp"
#include "server/ms/startup_mgr_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/store_mock.hpp"
#include "server/ms/topology_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class MSConnMgrTest : public ::testing::Test {
 protected:
  MockBaseMetaServer meta_server_;
  MockIFimProcessor* ms_fim_processor_;
  MockIFimProcessor* fc_fim_processor_;
  MockITrackerMapper* tracker_mapper_;
  boost::scoped_ptr<IConnMgr> conn_mgr_;
  MockIConnector* conn_;
  MockIHACounter* ha_counter_;
  MockIStateMgr* state_mgr_;
  MockIStartupMgr* startup_mgr_;
  MockIFailoverMgr* failover_mgr_;
  MockITimeKeeper* peer_time_keeper_;
  MockIInodeRemovalTracker* inode_removal_tracker_;
  MockIDurableRange* durable_range_;
  MockIInodeSrc* inode_src_;
  MockIResyncMgr* resync_mgr_;
  MockITopologyMgr* topology_mgr_;
  MockIAsioPolicy* asio_policy_;
  MockIInodeUsage* inode_usage_;
  MockIShutdownMgr* shutdown_mgr_;
  MockIStore* store_;
  IOService io_service_;
  boost::shared_ptr<MockIFimSocket> ms_fim_socket_;
  boost::shared_ptr<MockIReqTracker> ms_req_tracker_;

  MSConnMgrTest(ConfigMgr config = MakeConfig())
      : meta_server_(config),
        conn_mgr_(MakeConnMgr(&meta_server_)),
        ms_fim_socket_(new MockIFimSocket),
        ms_req_tracker_(new MockIReqTracker) {
    meta_server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    meta_server_.set_ms_fim_processor(
        ms_fim_processor_ = new MockIFimProcessor);
    meta_server_.set_fc_fim_processor(
        fc_fim_processor_ = new MockIFimProcessor);
    meta_server_.set_connector(conn_ = new MockIConnector);
    meta_server_.set_ha_counter(ha_counter_ = new MockIHACounter);
    meta_server_.set_state_mgr(state_mgr_ = new MockIStateMgr);
    meta_server_.set_startup_mgr(startup_mgr_ = new MockIStartupMgr);
    meta_server_.set_failover_mgr(failover_mgr_ = new MockIFailoverMgr);
    meta_server_.set_peer_time_keeper(peer_time_keeper_ = new MockITimeKeeper);
    meta_server_.set_inode_removal_tracker(
        inode_removal_tracker_ = new MockIInodeRemovalTracker);
    meta_server_.set_durable_range(durable_range_ = new MockIDurableRange);
    meta_server_.set_inode_src(inode_src_ = new MockIInodeSrc);
    meta_server_.set_resync_mgr(resync_mgr_ = new MockIResyncMgr);
    meta_server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    meta_server_.set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    meta_server_.set_inode_usage(inode_usage_ = new MockIInodeUsage);
    meta_server_.set_shutdown_mgr(shutdown_mgr_ = new MockIShutdownMgr);
    meta_server_.set_store(store_ = new MockIStore);

    EXPECT_CALL(*asio_policy_, io_service())
        .WillRepeatedly(Return(&io_service_));
    EXPECT_CALL(*ms_fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(ms_req_tracker_.get()));
    EXPECT_CALL(*ms_req_tracker_, name())
        .WillRepeatedly(Return("MS"));
  }

  static ConfigMgr MakeConfig() {
    ConfigMgr ret;
    ret.set_ms1_host("192.168.1.100");
    ret.set_ms1_port(3000);
    ret.set_ms2_host("192.168.1.100");
    ret.set_ms2_port(3001);
    ret.set_ds_port(3002);
    ret.set_role("MS2");
    return ret;
  }

  FimSocketCreatedHandler GetMsConnHandler(IFimProcessor** proc) {
    FimSocketCreatedHandler ret;
    EXPECT_CALL(*state_mgr_, GetState())
        .WillOnce(Return(kStateHANotReady));
    EXPECT_CALL(*conn_, AsyncConnect("192.168.1.100", 3000, _, _, true, _, _,
                                     0))
        .WillOnce(DoAll(SaveArg<2>(proc),
                        SaveArg<3>(&ret)));

    conn_mgr_->Init();
    return ret;
  }
};

TEST_F(MSConnMgrTest, ConnectSuccess) {
  // OnMSConnected
  IFimProcessor* fim_init_proc;
  FimSocketCreatedHandler conn_ms1_handler = GetMsConnHandler(&fim_init_proc);
  EXPECT_CALL(*ha_counter_, GetCount())
      .WillOnce(Return(2));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillOnce(Return(false));
  EXPECT_CALL(*store_, GetUUID())
      .WillOnce(Return("abcded12-15b4-235b-cac2-3beb9e546014"));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(0));
  FIM_PTR<IFim> reg_fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reg_fim));

  conn_ms1_handler(ms_fim_socket_);
  ASSERT_EQ(kMSMSRegFim, reg_fim->type());
  MSMSRegFim& r_reg_fim = dynamic_cast<MSMSRegFim&>(*reg_fim);
  EXPECT_EQ(0, r_reg_fim->active);
  EXPECT_EQ(2U, r_reg_fim->ha_counter);
  EXPECT_EQ("abcded12-15b4-235b-cac2-3beb9e546014",
            std::string(r_reg_fim->uuid));
  EXPECT_TRUE(r_reg_fim->new_node);

  // MSMSRegSuccess
  EXPECT_CALL(*ms_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*state_mgr_, SwitchState(kStateResync));
  EXPECT_CALL(*tracker_mapper_, SetMSFimSocket(
      boost::static_pointer_cast<IFimSocket>(ms_fim_socket_), true));
  FimSocketCleanupCallback ms_cleanup_callback;
  EXPECT_CALL(*ms_fim_socket_, OnCleanup(_))
      .WillOnce(SaveArg<0>(&ms_cleanup_callback));
  EXPECT_CALL(*ms_fim_socket_, SetFimProcessor(ms_fim_processor_));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillOnce(Return(false));
  EXPECT_CALL(*ha_counter_, Elect(1, true))
      .WillOnce(Return(false));  // Standby
  EXPECT_CALL(*resync_mgr_, RequestResync());
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*topology_mgr_, AddMS(_, _))
      .WillOnce(Return(true));

  FIM_PTR<MSMSRegSuccessFim> fim = MSMSRegSuccessFim::MakePtr();
  std::strncpy((*fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014",
               sizeof((*fim)->uuid));
  (*fim)->ha_counter = 1;
  (*fim)->active = 1;
  (*fim)->new_node = '\1';
  fim_init_proc->Process(fim, ms_fim_socket_);

  // Trigger cleanup
  EXPECT_CALL(*peer_time_keeper_, Stop());
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*durable_range_, SetConservative(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateStandby));
  DeadlineTimer* timer1 = new DeadlineTimer(io_service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer1));
  EXPECT_CALL(*failover_mgr_, Start(_));
  IOHandler reconn_timeout_handler;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer1, 5, _))
      .WillOnce(SaveArg<2>(&reconn_timeout_handler));

  ms_cleanup_callback();

  // If still failover, won't reconnect
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateFailover));
  DeadlineTimer* timer2 = new DeadlineTimer(io_service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer2));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer2, 5, _));

  reconn_timeout_handler(boost::system::error_code());

  // If done failover, reconnect
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateStandby));
  EXPECT_CALL(*conn_, AsyncConnect("192.168.1.100", 3000, _, _, true, _, _, 0));

  reconn_timeout_handler(boost::system::error_code());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSConnMgrTest, ConnectRejected) {
  // OnMSConnected
  IFimProcessor* fim_init_proc;
  FimSocketCreatedHandler conn_ms1_handler = GetMsConnHandler(&fim_init_proc);
  EXPECT_CALL(*ha_counter_, GetCount())
      .WillOnce(Return(2));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillOnce(Return(true));
  EXPECT_CALL(*store_, GetUUID())
      .WillOnce(Return("abcded12-15b4-235b-cac2-3beb9e546014"));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(30));
  FIM_PTR<IFim> reg_fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reg_fim));

  conn_ms1_handler(ms_fim_socket_);
  ASSERT_EQ(kMSMSRegFim, reg_fim->type());
  MSMSRegFim& r_reg_fim = dynamic_cast<MSMSRegFim&>(*reg_fim);
  EXPECT_EQ(1, r_reg_fim->active);
  EXPECT_EQ(2U, r_reg_fim->ha_counter);
  EXPECT_EQ("abcded12-15b4-235b-cac2-3beb9e546014",
            std::string(r_reg_fim->uuid));
  EXPECT_FALSE(r_reg_fim->new_node);

  // MSRegRejected
  EXPECT_CALL(*ms_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*ms_fim_socket_, Shutdown());
  IOHandler reconn_timeout_handler;
  DeadlineTimer* timer = new DeadlineTimer(io_service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 5, _))
      .WillOnce(SaveArg<2>(&reconn_timeout_handler));

  fim_init_proc->Process(MSRegRejectedFim::MakePtr(), ms_fim_socket_);
}

TEST_F(MSConnMgrTest, ConnectElected) {
  // OnMSConnected
  IFimProcessor* fim_init_proc;
  FimSocketCreatedHandler conn_ms1_handler = GetMsConnHandler(&fim_init_proc);
  EXPECT_CALL(*ha_counter_, GetCount())
      .WillOnce(Return(2));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillOnce(Return(true));
  EXPECT_CALL(*store_, GetUUID())
      .WillOnce(Return("abcded12-15b4-235b-cac2-3beb9e546014"));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(0));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));

  conn_ms1_handler(ms_fim_socket_);

  EXPECT_CALL(*ms_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*state_mgr_, SwitchState(kStateResync));
  EXPECT_CALL(*tracker_mapper_, SetMSFimSocket(
      boost::static_pointer_cast<IFimSocket>(ms_fim_socket_), true));
  FimSocketCleanupCallback ms_cleanup_callback;
  EXPECT_CALL(*ms_fim_socket_, OnCleanup(_))
      .WillOnce(SaveArg<0>(&ms_cleanup_callback));
  EXPECT_CALL(*ms_fim_socket_, SetFimProcessor(ms_fim_processor_));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillOnce(Return(false));
  EXPECT_CALL(*ha_counter_, Elect(1, false))
      .WillOnce(Return(true));  // Active
  EXPECT_CALL(meta_server_, StartServerActivated());
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*topology_mgr_, AddMS(_, _))
      .WillOnce(Return(true));

  // MSMSRegSuccess
  FIM_PTR<MSMSRegSuccessFim> fim = MSMSRegSuccessFim::MakePtr();
  std::strncpy((*fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014",
               sizeof((*fim)->uuid));
  (*fim)->ha_counter = 1;
  (*fim)->active = 0;
  (*fim)->new_node = '\0';
  fim_init_proc->Process(fim, ms_fim_socket_);
}

TEST_F(MSConnMgrTest, ConnectUUIDRejected) {
  // OnMSConnected
  IFimProcessor* fim_init_proc;
  FimSocketCreatedHandler conn_ms1_handler = GetMsConnHandler(&fim_init_proc);
  EXPECT_CALL(*ha_counter_, GetCount())
      .WillOnce(Return(2));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillOnce(Return(true));
  EXPECT_CALL(*store_, GetUUID())
      .WillOnce(Return("abcded12-15b4-235b-cac2-3beb9e546014"));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(30));
  FIM_PTR<IFim> reg_fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reg_fim));
  conn_ms1_handler(ms_fim_socket_);

  // Reject UUID
  FIM_PTR<IFim> reject_fim;
  EXPECT_CALL(*topology_mgr_, AddMS(_, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*ms_fim_socket_, Shutdown());
  IOHandler reconn_timeout_handler;
  DeadlineTimer* timer = new DeadlineTimer(io_service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 5, _))
      .WillOnce(SaveArg<2>(&reconn_timeout_handler));

  FIM_PTR<MSMSRegSuccessFim> fim = MSMSRegSuccessFim::MakePtr();
  std::strncpy((*fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014",
               sizeof((*fim)->uuid));
  (*fim)->ha_counter = 1;
  (*fim)->active = 0;
  (*fim)->new_node = '\0';
  fim_init_proc->Process(fim, ms_fim_socket_);
}

TEST_F(MSConnMgrTest, CleanupFC) {
  boost::shared_ptr<MockIFimSocket> fc_socket(new MockIFimSocket);
  boost::shared_ptr<IFimSocket> fc_isocket = fc_socket;
  MockIReqTracker fc_req_tracker;
  EXPECT_CALL(*fc_socket, GetReqTracker())
      .WillOnce(Return(&fc_req_tracker));
  EXPECT_CALL(fc_req_tracker, peer_client_num())
      .WillOnce(Return(5));
  boost::unordered_set<InodeNum> inodes_opened;
  inodes_opened.insert(InodeNum(1));
  inodes_opened.insert(InodeNum(55));
  EXPECT_CALL(*inode_usage_, GetFCOpened(5))
      .WillOnce(Return(inodes_opened));
  FIM_PTR<IFim> req1, req2;
  EXPECT_CALL(*fc_fim_processor_, Process(_, fc_isocket))
      .WillOnce(DoAll(SaveArg<0>(&req1),
                      Return(true)))
      .WillOnce(DoAll(SaveArg<0>(&req2),
                      Return(true)));
  EXPECT_CALL(*topology_mgr_, SetFCTerminating(5));
  DeadlineTimer* timer = new DeadlineTimer(io_service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  IOHandler fc_timeout_handler;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 70, _))
      .WillOnce(SaveArg<2>(&fc_timeout_handler));

  conn_mgr_->CleanupFC(fc_socket);
  boost::unordered_set<InodeNum> inodes_found;
  ASSERT_EQ(kReleaseFim, req1->type());
  ReleaseFim& r_req1 = dynamic_cast<ReleaseFim&>(*req1);
  inodes_found.insert(r_req1->inode);
  ASSERT_EQ(kReleaseFim, req2->type());
  ReleaseFim& r_req2 = dynamic_cast<ReleaseFim&>(*req2);
  inodes_found.insert(r_req2->inode);
  EXPECT_EQ(inodes_opened, inodes_found);

  // Timeout
  EXPECT_CALL(*topology_mgr_, RemoveFC(5));
  EXPECT_CALL(*topology_mgr_, AnnounceFC(5, false));

  fc_timeout_handler(boost::system::error_code());
}

TEST_F(MSConnMgrTest, CleanupDS) {
  // Upon DS disconnection
  EXPECT_CALL(*topology_mgr_, RemoveDS(1, 0, _))
      .WillOnce(SetArgPointee<2>(true));
  EXPECT_CALL(*topology_mgr_, AnnounceDS(1, 0, false, true));
  EXPECT_CALL(*topology_mgr_, GetDSGState(1, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      Return(kDSGDegraded)));
  EXPECT_CALL(*startup_mgr_, set_dsg_degraded(1, true, 0));

  conn_mgr_->CleanupDS(1, 0);
}

TEST_F(MSConnMgrTest, Suicide) {
  // Upon DS disconnection causing failure
  EXPECT_CALL(*topology_mgr_, RemoveDS(1, 0, _))
      .WillOnce(SetArgPointee<2>(true));
  EXPECT_CALL(*topology_mgr_, AnnounceDS(1, 0, false, true));
  EXPECT_CALL(*topology_mgr_, GetDSGState(1, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      Return(kDSGFailed)));
  DeadlineTimer* timer1 = new DeadlineTimer(io_service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer1));
  IOHandler timeout_handler;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer1, 10.0, _))
      .WillOnce(SaveArg<2>(&timeout_handler));

  conn_mgr_->CleanupDS(1, 0);

  // Upon timeout, call shutdown
  EXPECT_CALL(*shutdown_mgr_, Shutdown());

  timeout_handler(boost::system::error_code());
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
