/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/fim_processors.hpp"

#include <stdint.h>  // IWYU pragma: keep

#include <cstring>
#include <string>
#include <vector>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "admin_info.hpp"
#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"
#include "shutdown_mgr_mock.hpp"
#include "thread_fim_processor_mock.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "util.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/conn_mgr_mock.hpp"
#include "server/ms/dsg_op_state_mock.hpp"
#include "server/ms/failover_mgr_mock.hpp"
#include "server/ms/resync_mgr_mock.hpp"
#include "server/ms/startup_mgr_mock.hpp"
#include "server/ms/stat_keeper_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/store_mock.hpp"
#include "server/ms/topology_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StartsWith;
using ::testing::StrEq;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class MSFimProcessorsTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<MockBaseMetaServer> ms_;
  // The testee
  boost::scoped_ptr<IFimProcessor> init_fim_proc_;
  boost::scoped_ptr<IFimProcessor> ms_ctrl_fim_proc_;
  boost::scoped_ptr<IFimProcessor> ds_ctrl_fim_proc_;
  // Server
  MockITrackerMapper* tracker_mapper_;
  MockIStore* store_;
  MockITopologyMgr* topology_mgr_;
  IFimProcessor* ms_fim_proc_;
  IFimProcessor* ds_fim_proc_;
  IFimProcessor* fc_fim_proc_;
  IFimProcessor* admin_fim_proc_;
  boost::shared_ptr<MockIReqTracker> fc_req_tracker_;
  boost::shared_ptr<MockIFimSocket> fc_fim_socket_;
  boost::shared_ptr<MockIReqTracker> ds_req_tracker_;
  boost::shared_ptr<MockIFimSocket> ds_fim_socket_;
  boost::shared_ptr<MockIReqTracker> ms_req_tracker_;
  boost::shared_ptr<MockIFimSocket> ms_fim_socket_;
  MockIStateMgr* state_mgr_;
  MockIStartupMgr* startup_mgr_;
  MockIThreadFimProcessor* failover_proc_;
  MockIFailoverMgr* failover_mgr_;
  MockIResyncMgr* resync_mgr_;
  MockIDSGOpStateMgr* dsg_op_state_mgr_;
  MockIAsioPolicy* asio_policy_;
  MockIAsioPolicy* fc_asio_policy_;
  MockITimeKeeper* peer_time_keeper_;
  MockIHACounter* ha_counter_;
  MockIInodeRemovalTracker* inode_removal_tracker_;
  MockIDurableRange* durable_range_;
  MockIStatKeeper* stat_keeper_;
  MockIConnMgr* conn_mgr_;
  MockIShutdownMgr* shutdown_mgr_;
  MockIThreadGroup* thread_group_;

  static ConfigMgr MakeConfigMgr() {
    ConfigMgr config_items;
    config_items.set_ms1_host("192.168.1.100");
    config_items.set_ms1_port(5000);
    config_items.set_ms2_host("192.168.1.101");
    config_items.set_ms2_port(5001);
    config_items.set_heartbeat_interval(1);
    return config_items;
  }

  MSFimProcessorsTest()
      : ms_(new MockBaseMetaServer(MakeConfigMgr())),
        init_fim_proc_(MakeInitFimProcessor(ms_.get())),
        ms_ctrl_fim_proc_(MakeMSCtrlFimProcessor(ms_.get())),
        ds_ctrl_fim_proc_(MakeDSCtrlFimProcessor(ms_.get())),
        fc_req_tracker_(new MockIReqTracker),
        fc_fim_socket_(new MockIFimSocket),
        ds_req_tracker_(new MockIReqTracker),
        ds_fim_socket_(new MockIFimSocket),
        ms_req_tracker_(new MockIReqTracker),
        ms_fim_socket_(new MockIFimSocket) {
    ms_->set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    ms_->set_store(store_ = new MockIStore);
    ms_->set_ms_fim_processor(ms_fim_proc_ = new MockIFimProcessor);
    ms_->set_ds_fim_processor(ds_fim_proc_ = new MockIFimProcessor);
    ms_->set_fc_fim_processor(fc_fim_proc_ = new MockIFimProcessor);
    ms_->set_admin_fim_processor(admin_fim_proc_ = new MockIFimProcessor);
    ms_->set_state_mgr(state_mgr_ = new MockIStateMgr);
    ms_->set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    ms_->set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    ms_->set_fc_asio_policy(fc_asio_policy_ = new MockIAsioPolicy);
    ms_->set_startup_mgr(startup_mgr_ = new MockIStartupMgr);
    ms_->set_failover_processor(failover_proc_ = new MockIThreadFimProcessor);
    ms_->set_failover_mgr(failover_mgr_ = new MockIFailoverMgr);
    ms_->set_resync_mgr(resync_mgr_ = new MockIResyncMgr);
    ms_->set_dsg_op_state_mgr(dsg_op_state_mgr_ = new MockIDSGOpStateMgr);
    ms_->set_peer_time_keeper(peer_time_keeper_ = new MockITimeKeeper);
    ms_->set_ha_counter(ha_counter_ = new MockIHACounter);
    ms_->set_inode_removal_tracker(
        inode_removal_tracker_ = new MockIInodeRemovalTracker);
    ms_->set_durable_range(durable_range_ = new MockIDurableRange);
    ms_->set_stat_keeper(stat_keeper_ = new MockIStatKeeper);
    ms_->set_conn_mgr(conn_mgr_ = new MockIConnMgr);
    ms_->set_shutdown_mgr(shutdown_mgr_ = new MockIShutdownMgr);
    ms_->set_thread_group(thread_group_ = new MockIThreadGroup);
    EXPECT_CALL(*fc_fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(fc_req_tracker_.get()));
    EXPECT_CALL(*fc_req_tracker_, name())
        .WillRepeatedly(Return("FC"));
    EXPECT_CALL(*ds_fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(ds_req_tracker_.get()));
    EXPECT_CALL(*ds_req_tracker_, name())
        .WillRepeatedly(Return("DS"));
    EXPECT_CALL(*ms_fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(ms_req_tracker_.get()));
    EXPECT_CALL(*ms_req_tracker_, name())
        .WillRepeatedly(Return("MS"));
    EXPECT_CALL(*tracker_mapper_, GetMSTracker())
        .WillRepeatedly(Return(ms_req_tracker_));
  }

  ~MSFimProcessorsTest() {
    Mock::VerifyAndClear(tracker_mapper_);
  }
};

TEST_F(MSFimProcessorsTest, InitProcCLIRegNewConnect) {
  FIM_PTR<FCMSRegFim> fcms_regfim = FCMSRegFim::MakePtr();
  (*fcms_regfim)->is_reconnect = 0;
  (*fcms_regfim)->client_num = 0;
  (*fcms_regfim)->type = 'A';
  (*fcms_regfim)->force_start = '\x1';
  EXPECT_CALL(*state_mgr_, SwitchState(kStateActive));
  EXPECT_CALL(*ha_counter_, SetActive());
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*durable_range_, SetConservative(true));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateHANotReady))
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*topology_mgr_, SuggestFCId(_))
      .WillRepeatedly(DoAll(SetArgPointee<0>(5),
                            Return(true)));
  EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(5))
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
  EXPECT_CALL(*tracker_mapper_, SetAdminFimSocket(
      boost::static_pointer_cast<IFimSocket>(fc_fim_socket_), 5, true));
  EXPECT_CALL(*fc_fim_socket_, SetFimProcessor(admin_fim_proc_));
  FIM_PTR<IFim> fc_fim;
  EXPECT_CALL(*fc_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fc_fim));
  EXPECT_CALL(*topology_mgr_, SendAllDSInfo(
      boost::static_pointer_cast<IFimSocket>(fc_fim_socket_)));
  EXPECT_CALL(*fc_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));

  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket_));
}

TEST_F(MSFimProcessorsTest, InitProcFCRegNewConnect) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  FIM_PTR<FCMSRegFim> fcms_regfim = FCMSRegFim::MakePtr();
  (*fcms_regfim)->is_reconnect = 0;
  (*fcms_regfim)->client_num = 0;
  (*fcms_regfim)->type = 'F';
  (*fcms_regfim)->pid = 1000;
  (*fcms_regfim)->force_start = '\x0';
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*topology_mgr_, SuggestFCId(_))
      .WillRepeatedly(DoAll(SetArgPointee<0>(5),
                            Return(true)));
  EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(5))
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
  EXPECT_CALL(*topology_mgr_, AddFC(5, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*fc_fim_socket_, Migrate(fc_asio_policy_));
  EXPECT_CALL(*tracker_mapper_, SetFCFimSocket(
      boost::static_pointer_cast<IFimSocket>(fc_fim_socket_), 5, true));
  FIM_PTR<IFim> fc_fim;
  EXPECT_CALL(*fc_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fc_fim));
  EXPECT_CALL(*fc_fim_socket_, SetFimProcessor(fc_fim_proc_));
  EXPECT_CALL(*topology_mgr_, SendAllDSInfo(
      boost::static_pointer_cast<IFimSocket>(fc_fim_socket_)));
  EXPECT_CALL(*topology_mgr_, AnnounceFC(5, true));
  FimSocketCleanupCallback cleanup_callback;
  EXPECT_CALL(*fc_fim_socket_, OnCleanup(_))
      .WillOnce(SaveArg<0>(&cleanup_callback));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StrEq("Connection from FC 127.0.0.1:1234 accepted"), _));
  IOService io_service;
  TcpSocket socket(io_service);
  EXPECT_CALL(*fc_fim_socket_, socket())
      .WillRepeatedly(Return(&socket));
  TcpEndpoint endpoint(boost::asio::ip::address_v4(0x12121212), 1234U);
  EXPECT_CALL(*asio_policy_, GetRemoteEndpoint(&socket))
      .WillRepeatedly(Return(endpoint));
  EXPECT_CALL(*fc_fim_socket_, name())
      .WillOnce(Return(std::string("FC")));
  EXPECT_CALL(*fc_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket_));
  FCMSRegSuccessFim& success_fim = dynamic_cast<FCMSRegSuccessFim&>(*fc_fim);
  EXPECT_EQ(5U, success_fim->client_num);

  // Cleanup rescheduled
  boost::shared_ptr<IFimSocket> fc_ifim_socket = fc_fim_socket_;
  EXPECT_CALL(*thread_group_, SocketPending(fc_ifim_socket))
      .WillOnce(Return(true));
  EXPECT_CALL(*fc_fim_socket_, asio_policy())
      .WillOnce(Return(fc_asio_policy_));
  DeadlineTimer* timer = new DeadlineTimer(io_service);
  EXPECT_CALL(*fc_asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  IOHandler timer_callback;
  EXPECT_CALL(*fc_asio_policy_, SetDeadlineTimer(timer, 5.0, _))
      .WillOnce(SaveArg<2>(&timer_callback));

  cleanup_callback();

  // Cleanup triggered
  EXPECT_CALL(*thread_group_, SocketPending(fc_ifim_socket))
      .WillOnce(Return(false));
  EXPECT_CALL(*conn_mgr_, CleanupFC(fc_ifim_socket));

  timer_callback(boost::system::error_code());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcFCRegServerNotReady) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  FIM_PTR<FCMSRegFim> fcms_regfim = FCMSRegFim::MakePtr();
  (*fcms_regfim)->is_reconnect = 0;
  (*fcms_regfim)->client_num = 0;
  (*fcms_regfim)->type = 'F';
  (*fcms_regfim)->pid = 1000;
  (*fcms_regfim)->force_start = '\x0';
  boost::shared_ptr<MockIFimSocket> fc_fim_socket(new MockIFimSocket);
  FIM_PTR<IFim> fim;

  // Inactive case
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateStandby));
  EXPECT_CALL(*fc_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));
  EXPECT_CALL(*fc_fim_socket, name());
  EXPECT_CALL(*fc_fim_socket, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StartsWith("Cannot accept connection from"), _));

  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket));
  EXPECT_TRUE(dynamic_cast<MSRegRejectedFim*>(fim.get()));
}

TEST_F(MSFimProcessorsTest, InitProcFCRegServerShuttingDown) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  FIM_PTR<FCMSRegFim> fcms_regfim = FCMSRegFim::MakePtr();
  (*fcms_regfim)->is_reconnect = 0;
  (*fcms_regfim)->client_num = 0;
  (*fcms_regfim)->type = 'F';
  (*fcms_regfim)->pid = 1000;
  (*fcms_regfim)->force_start = '\x0';
  FIM_PTR<IFim> fim;

  // Inactive case
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateShuttingDown));
  EXPECT_CALL(*fc_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));
  EXPECT_CALL(*fc_fim_socket_, name());
  EXPECT_CALL(*fc_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StartsWith("Cannot accept connection from"), _));

  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket_));
  EXPECT_TRUE(dynamic_cast<MSRegRejectedFim*>(fim.get()));
}

TEST_F(MSFimProcessorsTest, InitProcFCRegGetIdFailures) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  FIM_PTR<FCMSRegFim> fcms_regfim = FCMSRegFim::MakePtr();
  (*fcms_regfim)->is_reconnect = 0;
  (*fcms_regfim)->client_num = 0;
  (*fcms_regfim)->type = 'F';
  (*fcms_regfim)->force_start = '\x0';
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));

  // No ID case
  EXPECT_CALL(*topology_mgr_, SuggestFCId(_))
      .WillOnce(Return(false));
  EXPECT_CALL(*fc_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Server),
                   StartsWith("Failed to accept FC connection. "
                              "Cannot allocate Id"), _));
  EXPECT_CALL(*fc_fim_socket_, name())
      .WillOnce(Return(std::string()));
  EXPECT_CALL(*fc_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket_));
  EXPECT_TRUE(dynamic_cast<MSRegRejectedFim*>(fim.get()));

  EXPECT_CALL(*topology_mgr_, SuggestFCId(_))
      .WillRepeatedly(DoAll(SetArgPointee<0>(5),
                            Return(true)));

  // Used Fim socket case
  EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(5))
      .WillOnce(Return(fc_fim_socket_));
  EXPECT_CALL(*fc_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Server),
                   StartsWith("Failed to accept FC connection. "
                              "Cannot allocate Id"), _));
  EXPECT_CALL(*fc_fim_socket_, name())
      .WillOnce(Return(std::string()));
  EXPECT_CALL(*fc_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));

  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket_));

  EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(5))
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));

  // AddFC() returns false
  IOService io_service;
  TcpSocket socket(io_service);
  EXPECT_CALL(*fc_fim_socket_, socket())
      .WillRepeatedly(Return(&socket));
  TcpEndpoint endpoint(boost::asio::ip::address_v4(0x12121212), 1234U);
  EXPECT_CALL(*asio_policy_, GetRemoteEndpoint(&socket))
      .WillRepeatedly(Return(endpoint));
  EXPECT_CALL(*topology_mgr_, AddFC(5, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*fc_fim_socket_, WriteMsg(_));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Server),
                   StartsWith("Failed to add FC"), _));
  EXPECT_CALL(*fc_fim_socket_, name())
      .WillOnce(Return(std::string()));
  EXPECT_CALL(*fc_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket_));

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcDSRegGrouped) {
  FIM_PTR<DSMSRegFim> fim = DSMSRegFim::MakePtr();
  std::memcpy((*fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014", 37U);
  (*fim)->ip = IPToInt("10.1.1.2");
  (*fim)->port = 3000;
  (*fim)->pid = 1234;
  (*fim)->is_grouped = 1;
  (*fim)->ds_group = 0;
  (*fim)->ds_role = 2;
  (*fim)->is_reconnect = 0;
  (*fim)->opt_resync = '\1';
  (*fim)->distressed = '\1';

  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*topology_mgr_, GetDSGState(0, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      Return(kDSGPending)));
  EXPECT_CALL(*startup_mgr_, dsg_degraded(0, _))
      .WillOnce(DoAll(SetArgPointee<1>(1),
                      Return(true)));
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 2))
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
  NodeInfo server_addr;
  EXPECT_CALL(*topology_mgr_, AddDS(0, 2, _, true, _))
      .WillOnce(DoAll(SaveArg<2>(&server_addr),
                      SetArgPointee<4>(false),
                      Return(true)));
  EXPECT_CALL(*topology_mgr_, SetDSGDistressed(0, true));
  EXPECT_CALL(*tracker_mapper_, SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(ds_fim_socket_), 0, 2, true));
  FimSocketCleanupCallback cleanup_callback;
  EXPECT_CALL(*ds_fim_socket_, OnCleanup(_))
      .WillOnce(SaveArg<0>(&cleanup_callback));
  EXPECT_CALL(*ds_fim_socket_, name())
      .WillOnce(Return(std::string("DS")));
  EXPECT_CALL(*ds_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*ds_fim_socket_, SetFimProcessor(
      static_cast<IFimProcessor*>(ds_fim_proc_)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*ds_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  EXPECT_CALL(*topology_mgr_, AnnounceDS(0, 2, true, false));
  EXPECT_CALL(*topology_mgr_, AllDSReady())
      .WillOnce(Return(false));

  EXPECT_TRUE(init_fim_proc_->Process(fim, ds_fim_socket_));
  EXPECT_EQ(IPToInt("10.1.1.2"), server_addr.ip);
  EXPECT_EQ(3000U, server_addr.port);
  DSMSRegSuccessFim& success_fim = static_cast<DSMSRegSuccessFim&>(*reply);
  EXPECT_EQ(0U, success_fim->ds_group);
  EXPECT_EQ(2U, success_fim->ds_role);

  // Cleanup
  EXPECT_CALL(*conn_mgr_, CleanupDS(0, 2));

  cleanup_callback();

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcDSRegUngrouped) {
  FIM_PTR<DSMSRegFim> fim = DSMSRegFim::MakePtr();
  std::memcpy((*fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014", 37U);
  (*fim)->ip = IPToInt("10.1.1.2");
  (*fim)->port = 3000;
  (*fim)->pid = 1234;
  (*fim)->is_grouped = 0;
  (*fim)->ds_group = 0;
  (*fim)->ds_role = 0;
  (*fim)->is_reconnect = 0;
  (*fim)->opt_resync = '\0';
  (*fim)->distressed = '\0';

  // Failed suggesting
  EXPECT_CALL(*topology_mgr_, SuggestDSRole(_, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*ds_fim_socket_, WriteMsg(_));

  EXPECT_TRUE(init_fim_proc_->Process(fim, ds_fim_socket_));

  // Successful call
  EXPECT_CALL(*topology_mgr_, SuggestDSRole(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(0),
                      SetArgPointee<1>(2),
                      Return(true)));
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 2))
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
  NodeInfo server_addr;
  EXPECT_CALL(*topology_mgr_, AddDS(0, 2, _, false, _))
      .WillOnce(DoAll(SaveArg<2>(&server_addr),
                      SetArgPointee<4>(true),
                      Return(true)));
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*tracker_mapper_, SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(ds_fim_socket_), 0, 2, true));
  EXPECT_CALL(*ds_fim_socket_, OnCleanup(_));
  EXPECT_CALL(*ds_fim_socket_, name())
      .WillOnce(Return(std::string("DS")));
  EXPECT_CALL(*ds_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*ds_fim_socket_, SetFimProcessor(
      static_cast<IFimProcessor*>(ds_fim_proc_)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*ds_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  EXPECT_CALL(*topology_mgr_, AnnounceDS(0, 2, true, true));
  EXPECT_CALL(*topology_mgr_, AllDSReady())
      .WillOnce(Return(true));
  EXPECT_CALL(*stat_keeper_, Run());

  EXPECT_TRUE(init_fim_proc_->Process(fim, ds_fim_socket_));
  EXPECT_EQ(IPToInt("10.1.1.2"), server_addr.ip);
  EXPECT_EQ(3000U, server_addr.port);
  DSMSRegSuccessFim& success_fim = static_cast<DSMSRegSuccessFim&>(*reply);
  EXPECT_EQ(0U, success_fim->ds_group);
  EXPECT_EQ(2U, success_fim->ds_role);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcDSRegServerNotReady) {
  FIM_PTR<DSMSRegFim> fim = DSMSRegFim::MakePtr();
  std::memcpy((*fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014", 37U);
  (*fim)->ip = IPToInt("10.1.1.2");
  (*fim)->port = 3000;
  (*fim)->pid = 1234;
  (*fim)->is_grouped = 1;
  (*fim)->ds_group = 0;
  (*fim)->ds_role = 2;
  (*fim)->is_reconnect = 0;
  (*fim)->opt_resync = '\0';
  (*fim)->distressed = '\0';
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());

  // Unsuccessful call 1: not active
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillOnce(Return(false));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateStandby));
  EXPECT_CALL(*ds_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*ds_fim_socket_, name());
  EXPECT_CALL(*ds_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StartsWith("Cannot accept connection from"), _));

  EXPECT_TRUE(init_fim_proc_->Process(fim, ds_fim_socket_));
  Mock::VerifyAndClear(&mock_log_callback);

  // Unsuccessful call 2: Previously failed
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*topology_mgr_, GetDSGState(0, _))
      .WillRepeatedly(Return(kDSGPending));
  EXPECT_CALL(*startup_mgr_, dsg_degraded(0, _))
      .WillOnce(DoAll(SetArgPointee<1>(2),
                      Return(true)));
  EXPECT_CALL(*ds_fim_socket_, WriteMsg(_));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Server),
                   StrEq("Cannot accept previously failed DS 0-2 "
                         "until DSG is started"), _));

  EXPECT_TRUE(init_fim_proc_->Process(fim, ds_fim_socket_));
  Mock::VerifyAndClear(&mock_log_callback);

  // Unsuccessful call 3: already assigned
  EXPECT_CALL(*topology_mgr_, GetDSGState(0, _))
      .WillRepeatedly(Return(kDSGDegraded));
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 2))
      .WillOnce(Return(ds_fim_socket_));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Server),
                   StrEq("DS 0-2 already occupied"), _));
  EXPECT_CALL(*ds_fim_socket_, WriteMsg(_));
  EXPECT_TRUE(init_fim_proc_->Process(fim, ds_fim_socket_));

  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 2))
      .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));

  // Unsuccessful call 4: AddDS returns false
  EXPECT_CALL(*topology_mgr_, AddDS(0, 2, _, false, _))
      .WillOnce(Return(false));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Server),
                   StrEq("Internal inconsistency with DS 0-2"),
                   _));
  EXPECT_CALL(*ds_fim_socket_, WriteMsg(_));
  EXPECT_TRUE(init_fim_proc_->Process(fim, ds_fim_socket_));

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcMSReg) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateHANotReady));
  EXPECT_CALL(*ha_counter_, GetCount()).WillOnce(Return(1));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*conn_mgr_, InitMSConn(
      boost::static_pointer_cast<IFimSocket>(ms_fim_socket_), 0, 1));
  EXPECT_CALL(*ms_fim_socket_, name())
      .WillRepeatedly(Return(std::string("MS")));
  EXPECT_CALL(*store_, GetUUID())
      .WillOnce(Return("aabbccdd-15b4-235b-cac2-3beb9e546014"));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(0));
  EXPECT_CALL(*topology_mgr_, AddMS(_, _))
      .WillOnce(Return(true));

  FIM_PTR<MSMSRegFim> reg_fim = MSMSRegFim::MakePtr();
  std::memcpy((*reg_fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014", 37U);
  (*reg_fim)->ha_counter = 0;
  (*reg_fim)->active = 1;
  (*reg_fim)->new_node = '\1';
  EXPECT_TRUE(init_fim_proc_->Process(reg_fim, ms_fim_socket_));

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcMSRegRejectFailover) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateFailover));
  FIM_PTR<IFim> result;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&result));
  EXPECT_CALL(*topology_mgr_, AddMS(_, _))
      .WillOnce(Return(true));

  FIM_PTR<MSMSRegFim> reg_fim = MSMSRegFim::MakePtr();
  std::memcpy((*reg_fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014", 37U);
  (*reg_fim)->ha_counter = 0;
  (*reg_fim)->active = 0;
  (*reg_fim)->new_node = '\0';
  EXPECT_TRUE(init_fim_proc_->Process(reg_fim, ms_fim_socket_));
  EXPECT_EQ(kMSRegRejectedFim, result->type());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcMSRegRejectActive) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  FIM_PTR<IFim> result;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&result));
  EXPECT_CALL(*topology_mgr_, AddMS(_, _))
      .WillOnce(Return(true));

  FIM_PTR<MSMSRegFim> reg_fim = MSMSRegFim::MakePtr();
  std::memcpy((*reg_fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014", 37U);
  (*reg_fim)->ha_counter = 0;
  (*reg_fim)->active = 1;
  (*reg_fim)->new_node = '\0';
  EXPECT_TRUE(init_fim_proc_->Process(reg_fim, ms_fim_socket_));
  EXPECT_EQ(kMSRegRejectedFim, result->type());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcMSInvalidUUID) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*ms_fim_socket_, Shutdown());
  EXPECT_CALL(*topology_mgr_, AddMS(_, _))
      .WillOnce(Return(false));

  FIM_PTR<MSMSRegFim> reg_fim = MSMSRegFim::MakePtr();
  std::memcpy((*reg_fim)->uuid, "abcded12-15b4-235b-cac2-3beb9e546014", 37U);
  (*reg_fim)->ha_counter = 0;
  (*reg_fim)->active = 1;
  (*reg_fim)->new_node = '\0';
  EXPECT_TRUE(init_fim_proc_->Process(reg_fim, ms_fim_socket_));

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InitProcFailover) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  FIM_PTR<FCMSRegFim> fcms_regfim = FCMSRegFim::MakePtr();
  (*fcms_regfim)->is_reconnect = 1;
  (*fcms_regfim)->client_num = 5;
  (*fcms_regfim)->type = 'F';
  (*fcms_regfim)->force_start = '\x0';

  IOService io_service;
  TcpSocket socket(io_service);
  EXPECT_CALL(*fc_fim_socket_, socket())
      .WillRepeatedly(Return(&socket));
  TcpEndpoint endpoint(boost::asio::ip::address_v4(0x12121212), 1234U);
  EXPECT_CALL(*asio_policy_, GetRemoteEndpoint(&socket))
      .WillRepeatedly(Return(endpoint));

  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateFailover));
  EXPECT_CALL(*topology_mgr_, AddFC(5, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*fc_fim_socket_, Migrate(fc_asio_policy_));
  EXPECT_CALL(*tracker_mapper_, SetFCFimSocket(
      boost::static_pointer_cast<IFimSocket>(fc_fim_socket_), 5, true));
  EXPECT_CALL(*fc_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*fc_fim_socket_, SetFimProcessor(failover_proc_));
  EXPECT_CALL(*topology_mgr_, SendAllDSInfo(
      boost::static_pointer_cast<IFimSocket>(fc_fim_socket_)));
  EXPECT_CALL(*topology_mgr_, AnnounceFC(5, true));
  FimSocketCleanupCallback cleanup_callback;
  EXPECT_CALL(*fc_fim_socket_, OnCleanup(_))
      .WillOnce(SaveArg<0>(&cleanup_callback));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StrEq("Connection from FC 127.0.0.1:1234 accepted"), _));
  EXPECT_CALL(*fc_fim_socket_, name())
      .WillOnce(Return(std::string("FC")));
  EXPECT_CALL(*fc_fim_socket_, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));

  EXPECT_TRUE(init_fim_proc_->Process(fcms_regfim, fc_fim_socket_));
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, InterMSProc) {
  // Add DS
  NodeInfo addr;
  EXPECT_CALL(*topology_mgr_, AddDS(12, 3, _, false, _))
      .WillOnce(DoAll(SaveArg<2>(&addr),
                      Return(true)));

  FIM_PTR<TopologyChangeFim> fim(new TopologyChangeFim);
  std::memset((*fim)->uuid, '\0', sizeof((*fim)->uuid));
  (*fim)->ds_group = 12;
  (*fim)->ds_role = 3;
  (*fim)->ip = 0x12345678;
  (*fim)->port = 0x90ab;
  (*fim)->type = 'D';
  (*fim)->joined = '\x01';
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
  EXPECT_EQ(0x12345678U, addr.ip);
  EXPECT_EQ(0x90abU, addr.port);

  // Remove DS
  EXPECT_CALL(*topology_mgr_, RemoveDS(12, 3, _));

  (*fim)->joined = '\x00';
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  // Add FC
  EXPECT_CALL(*topology_mgr_, AddFC(123, _))
      .WillOnce(Return(true));

  (*fim)->type = 'F';
  (*fim)->client_num = 123;
  (*fim)->joined = '\x01';
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  // Remove FC
  EXPECT_CALL(*topology_mgr_, RemoveFC(123));

  (*fim)->joined = '\x00';
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
}

TEST_F(MSFimProcessorsTest, MSDSDSGStateChangeAckDegraded) {
  EXPECT_CALL(*topology_mgr_, AckDSGStateChange(
      42U, boost::static_pointer_cast<IFimSocket>(ms_fim_socket_), _, _))
      .WillOnce(DoAll(SetArgPointee<2>(2),
                      SetArgPointee<3>(kDSGOutdated),
                      Return(true)));

  FIM_PTR<DSGStateChangeAckFim> fim(new DSGStateChangeAckFim);
  (*fim)->state_change_id = 42;
  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  EXPECT_CALL(*topology_mgr_, AckDSGStateChange(
      42U, boost::static_pointer_cast<IFimSocket>(ms_fim_socket_), _, _))
      .WillOnce(DoAll(SetArgPointee<2>(2),
                      SetArgPointee<3>(kDSGDegraded),
                      Return(true)));
  EXPECT_CALL(*topology_mgr_, GetDSGState(2, _))
      .WillOnce(DoAll(SetArgPointee<1>(3),
                      Return(kDSGDegraded)));
  boost::shared_ptr<MockIReqTracker> ds_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(2, 3))
      .WillOnce(Return(ds_tracker));
  EXPECT_CALL(*ds_tracker, RedirectRequests(_));

  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, MSDSDSGStateChangeAckRecovering) {
  EXPECT_CALL(*topology_mgr_, AckDSGStateChange(
      42U, boost::static_pointer_cast<IFimSocket>(ms_fim_socket_), _, _))
      .WillOnce(DoAll(SetArgPointee<2>(2),
                      SetArgPointee<3>(kDSGRecovering),
                      Return(true)));
  boost::shared_ptr<MockIFimSocket> ds_fim_socket(new MockIFimSocket);
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(2, r))
        .WillOnce(Return(ds_fim_socket));
  FIM_PTR<IFim> resync_fim;
  EXPECT_CALL(*ds_fim_socket, WriteMsg(_)).Times(kNumDSPerGroup)
      .WillRepeatedly(SaveArg<0>(&resync_fim));

  FIM_PTR<DSGStateChangeAckFim> fim(new DSGStateChangeAckFim);
  (*fim)->state_change_id = 42;
  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
  EXPECT_EQ(kDSResyncReqFim, resync_fim->type());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, MSDSDSResyncEnd) {
  boost::shared_ptr<MockIFimSocket> ds_fim_socket(new MockIFimSocket);
  boost::shared_ptr<IFimSocket> ifs = ds_fim_socket;

  // Cannot find DS role: just return
  EXPECT_CALL(*tracker_mapper_, FindDSRole(ifs, _, _))
      .WillOnce(Return(false));

  FIM_PTR<DSResyncEndFim> fim = DSResyncEndFim::MakePtr();
  (*fim)->end_type = 1;
  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ds_fim_socket));

  // Can find DS role, DSRecovered failed: return
  EXPECT_CALL(*tracker_mapper_, FindDSRole(ifs, _, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      SetArgPointee<2>(2),
                      Return(true)));
  EXPECT_CALL(*topology_mgr_, DSRecovered(0, 2, 1))
      .WillOnce(Return(false));

  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ds_fim_socket));

  // Can find DS role, DSRecovered success: Announce state
  EXPECT_CALL(*tracker_mapper_, FindDSRole(ifs, _, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      SetArgPointee<2>(2),
                      Return(true)));
  EXPECT_CALL(*topology_mgr_, DSRecovered(0, 2, 1))
      .WillOnce(Return(true));
  EXPECT_CALL(*startup_mgr_, set_dsg_degraded(0, false, 2));
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*topology_mgr_, AnnounceDSGState(0));

  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ds_fim_socket));
}

TEST_F(MSFimProcessorsTest, MSDSDSResyncPhaseInodeList) {
  boost::shared_ptr<MockIFimSocket> ds_fim_socket(new MockIFimSocket);
  boost::shared_ptr<IFimSocket> ifs = ds_fim_socket;

  // Cannot find DS role: just return
  EXPECT_CALL(*tracker_mapper_, FindDSRole(ifs, _, _))
      .WillOnce(Return(false));

  FIM_PTR<DSResyncPhaseInodeListFim> fim =
      DSResyncPhaseInodeListFim::MakePtr(2 * sizeof(InodeNum));
  InodeNum* inodes = reinterpret_cast<InodeNum*>(fim->tail_buf());
  inodes[0] = 42;
  inodes[1] = 43;
  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ds_fim_socket));

  // Can find DS role, incorrect state: just return
  EXPECT_CALL(*tracker_mapper_, FindDSRole(ifs, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<1>(0),
                            SetArgPointee<2>(2),
                            Return(true)));
  EXPECT_CALL(*topology_mgr_, GetDSGState(0, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      Return(kDSGRecovering)));

  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ds_fim_socket));

  // Can find DS role, state is resync, set resyncing inodes
  EXPECT_CALL(*topology_mgr_, GetDSGState(0, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      Return(kDSGResync)));
  EXPECT_CALL(*dsg_op_state_mgr_, SetDsgInodesResyncing(
      0, ElementsAre(42, 43)));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateStandalone));
  boost::function<void()> callback;
  EXPECT_CALL(*dsg_op_state_mgr_, OnInodesCompleteOp(_, _))
      .WillOnce(SaveArg<1>(&callback));

  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ds_fim_socket));

  // Upon callback, a SendPhaseInodeListReply is queued
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*ds_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  callback();
  EXPECT_EQ(kResultCodeReplyFim, reply->type());
}

TEST_F(MSFimProcessorsTest, MSDSDSResyncPhaseInodeListHA) {
  boost::shared_ptr<MockIFimSocket> ds_fim_socket(new MockIFimSocket);
  boost::shared_ptr<IFimSocket> ifs = ds_fim_socket;

  FIM_PTR<DSResyncPhaseInodeListFim> fim =
      DSResyncPhaseInodeListFim::MakePtr(2 * sizeof(InodeNum));
  InodeNum* inodes = reinterpret_cast<InodeNum*>(fim->tail_buf());
  inodes[0] = 42;
  inodes[1] = 43;

  // In HA mode, will make a replication and wait for reply
  EXPECT_CALL(*tracker_mapper_, FindDSRole(ifs, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<1>(0),
                            SetArgPointee<2>(2),
                            Return(true)));
  EXPECT_CALL(*topology_mgr_, GetDSGState(0, _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      Return(kDSGResync)));
  EXPECT_CALL(*dsg_op_state_mgr_, SetDsgInodesResyncing(
      0, ElementsAre(42, 43)));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  boost::shared_ptr<IReqEntry> repl_entry;
  EXPECT_CALL(*ms_req_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&repl_entry),
                      Return(true)));

  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ds_fim_socket));

  // Upon reply, follow non-HA path
  boost::function<void()> callback;
  EXPECT_CALL(*dsg_op_state_mgr_, OnInodesCompleteOp(_, _))
      .WillOnce(SaveArg<1>(&callback));

  FIM_PTR<ResultCodeReplyFim> cs_reply = ResultCodeReplyFim::MakePtr();
  (*cs_reply)->err_no = 0;
  repl_entry->SetReply(cs_reply, 42);

  // Upon callback, a SendPhaseInodeListReply is queued
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*ds_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  callback();
  EXPECT_EQ(kResultCodeReplyFim, reply->type());
}

TEST_F(MSFimProcessorsTest, MSResyncReqCombined) {
  MSStateChangeCallback callback;
  EXPECT_CALL(*state_mgr_, OnState(kStateResync, _))
      .WillOnce(SaveArg<1>(&callback));

  FIM_PTR<MSResyncReqFim> fim(new MSResyncReqFim);
  std::memset(fim->body(), 0, fim->body_size());
  (*fim)->first = 1;
  (*fim)->last = 'O';
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  std::vector<InodeNum> removed;
  EXPECT_CALL(*resync_mgr_, SendAllResync(true, removed));

  callback();
}

TEST_F(MSFimProcessorsTest, MSResyncReqSplit) {
  // First part
  MSStateChangeCallback callback;
  EXPECT_CALL(*state_mgr_, OnState(kStateResync, _))
      .WillOnce(SaveArg<1>(&callback));

  FIM_PTR<MSResyncReqFim> fim(new MSResyncReqFim(sizeof(InodeNum)));
  std::memset(fim->body(), 0, fim->body_size());
  (*fim)->first = 2;
  *reinterpret_cast<InodeNum*>(fim->tail_buf()) = 3;
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  // Callback, nothing observable
  callback();

  // Second part
  EXPECT_CALL(*state_mgr_, OnState(kStateResync, _))
      .WillOnce(SaveArg<1>(&callback));

  std::memset(fim->body(), 0, fim->body_size());
  (*fim)->last = 'F';
  *reinterpret_cast<InodeNum*>(fim->tail_buf()) = 7;
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  // Callback, observe ResyncAll called with combined info
  std::vector<InodeNum> removed;
  removed.push_back(3);
  removed.push_back(7);
  EXPECT_CALL(*resync_mgr_, SendAllResync(false, removed));

  callback();
}

TEST_F(MSFimProcessorsTest, MSResyncEnd) {
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*peer_time_keeper_, Start());
  EXPECT_CALL(*state_mgr_, SwitchState(kStateStandby));
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(false));
  EXPECT_CALL(*durable_range_, SetConservative(false));
  EXPECT_CALL(*store_, SetLastResyncDirTimes());
  EXPECT_CALL(*ha_counter_, PersistCount(kPersistActualHACount));
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  ms_ctrl_fim_proc_->Process(MSResyncEndFim::MakePtr(), ms_fim_socket_);
  Sleep(0.01)();

  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSFimProcessorsTest, DSGStateChangeFim) {
  // Switching to degraded, will record
  EXPECT_CALL(*topology_mgr_, SetDSGState(0, 2, kDSGDegraded, 1));
  EXPECT_CALL(*startup_mgr_, set_dsg_degraded(0, true, 2));

  FIM_PTR<DSGStateChangeFim> fim(new DSGStateChangeFim);
  (*fim)->ds_group = 0;
  (*fim)->failed = 2;
  (*fim)->state = kDSGDegraded;
  (*fim)->state_change_id = 1;
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  // Switching to ready, will remove record
  EXPECT_CALL(*topology_mgr_, SetDSGState(0, 2, kDSGReady, 1));
  EXPECT_CALL(*startup_mgr_, set_dsg_degraded(0, false, 2));

  (*fim)->state = kDSGReady;
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  // Switching to failed, will not touch record
  EXPECT_CALL(*topology_mgr_, SetDSGState(0, 2, kDSGFailed, 1));

  (*fim)->state = kDSGFailed;
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
}

TEST_F(MSFimProcessorsTest, MSDSDSGStateChangeAckShuttingDown) {
  EXPECT_CALL(*topology_mgr_, AckDSGStateChange(
      42U, boost::static_pointer_cast<IFimSocket>(ms_fim_socket_), _, _))
      .WillOnce(DoAll(SetArgPointee<2>(2),
                      SetArgPointee<3>(kDSGShuttingDown),
                      Return(true)));
  EXPECT_CALL(*shutdown_mgr_, Shutdown());

  FIM_PTR<DSGStateChangeAckFim> fim(new DSGStateChangeAckFim);
  (*fim)->state_change_id = 42;
  EXPECT_TRUE(ds_ctrl_fim_proc_->Process(fim, ms_fim_socket_));

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSFimProcessorsTest, SysHalt) {
  FIM_PTR<SysHaltFim> fim(new SysHaltFim);
  EXPECT_CALL(*shutdown_mgr_, Shutdown());

  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
}

TEST_F(MSFimProcessorsTest, SysShutdownReq) {
  FIM_PTR<SysShutdownReqFim> fim(new SysShutdownReqFim);
  EXPECT_CALL(*shutdown_mgr_, Init(_));

  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
}

TEST_F(MSFimProcessorsTest, DSGResize) {
  FIM_PTR<DSGResizeFim> fim(new DSGResizeFim);
  (*fim)->num_groups = 3;
  EXPECT_CALL(*topology_mgr_, set_num_groups(3));
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
}

TEST_F(MSFimProcessorsTest, ConfigList) {
  FIM_PTR<ClusterConfigListReqFim> fim(new ClusterConfigListReqFim);
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
}

TEST_F(MSFimProcessorsTest, ConfigChange) {
  FIM_PTR<PeerConfigChangeReqFim> fim1 = PeerConfigChangeReqFim::MakePtr();
  strncpy((*fim1)->name, "log_severity", 13);
  strncpy((*fim1)->value, "5", 2);
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  ms_ctrl_fim_proc_->Process(fim1, ms_fim_socket_);
  EXPECT_EQ("5", ms_->configs().log_severity());

  FIM_PTR<PeerConfigChangeReqFim> fim2 = PeerConfigChangeReqFim::MakePtr();
  strncpy((*fim2)->name, "log_path", 9);
  strncpy((*fim2)->value, "/tmp", 5);
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  ms_ctrl_fim_proc_->Process(fim2, ms_fim_socket_);
  EXPECT_EQ("/tmp", ms_->configs().log_path());

  FIM_PTR<PeerConfigChangeReqFim> fim3 = PeerConfigChangeReqFim::MakePtr();
  strncpy((*fim3)->name, "num_ds_groups", 14);
  strncpy((*fim3)->value, "3", 2);
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateActive));
  EXPECT_CALL(*topology_mgr_, set_num_groups(3));
  ms_ctrl_fim_proc_->Process(fim3, ms_fim_socket_);
}

TEST_F(MSFimProcessorsTest, DSResyncPhaseInodeList) {
  FIM_PTR<DSResyncPhaseInodeListFim> fim =
      DSResyncPhaseInodeListFim::MakePtr(2 * sizeof(InodeNum));
  (*fim)->ds_group = 1;
  InodeNum* inodes = reinterpret_cast<InodeNum*>(fim->tail_buf());
  inodes[0] = 42;
  inodes[1] = 43;

  EXPECT_CALL(*dsg_op_state_mgr_, SetDsgInodesResyncing(
      1, ElementsAre(42, 43)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  EXPECT_TRUE(ms_ctrl_fim_proc_->Process(fim, ms_fim_socket_));
  EXPECT_EQ(kResultCodeReplyFim, reply->type());
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
