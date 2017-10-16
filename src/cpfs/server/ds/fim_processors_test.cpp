/* Copyright 2013 ClusterTech Ltd */
#include "server/ds/fim_processors.hpp"

#include <stdint.h>

#include <cerrno>
#include <cstring>
#include <string>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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
#include "req_completion_mock.hpp"
#include "req_tracker_mock.hpp"
#include "shutdown_mgr_mock.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ds/base_ds_mock.hpp"
#include "server/ds/conn_mgr_mock.hpp"
#include "server/ds/degrade_mock.hpp"
#include "server/ds/resync_mock.hpp"
#include "server/ds/store_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

namespace cpfs {
namespace server {
namespace ds {
namespace {

class DsFimProcessorTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<MockBaseDataServer> ds_;
  boost::scoped_ptr<IFimProcessor> ms_ctrl_fim_proc_;
  boost::scoped_ptr<IFimProcessor> init_fim_proc_;
  boost::scoped_ptr<MockIResyncFimProcessor> resync_fim_proc_;

  MockIAsioPolicy* asio_policy_;
  MockIAsioPolicy* fc_asio_policy_;
  MockIAsioPolicy* ds_asio_policy_;
  MockITrackerMapper* tracker_mapper_;
  MockIConnMgr* conn_mgr_;
  MockIStore* store_;
  MockIInodeRemovalTracker* inode_removal_tracker_;
  MockIDurableRange* durable_range_;
  MockIFimProcessor* ds_fim_processor_;
  MockIFimProcessor* fc_fim_processor_;
  MockIDegradedCache* degraded_cache_;
  MockIReqCompletionCheckerSet* req_completion_checker_set_;
  MockIResyncMgr* resync_mgr_;
  MockIThreadGroup* thread_group_;
  MockITimeKeeper* dsg_ready_time_keeper_;
  MockIShutdownMgr* shutdown_mgr_;

  static ConfigMgr MakeConfig() {
    ConfigMgr ret;
    ret.set_ds_port(0);
    ret.set_ms1_port(0);
    ret.set_ms2_port(0);
    return ret;
  }

  DsFimProcessorTest()
      : ds_(new MockBaseDataServer(MakeConfig())),
        ms_ctrl_fim_proc_(MakeMSCtrlFimProcessor(ds_.get())),
        init_fim_proc_(MakeInitFimProcessor(ds_.get())),
        resync_fim_proc_(new MockIResyncFimProcessor) {
    ds_->set_asio_policy((asio_policy_ = new MockIAsioPolicy));
    ds_->set_fc_asio_policy((fc_asio_policy_ = new MockIAsioPolicy));
    ds_->set_ds_asio_policy((ds_asio_policy_ = new MockIAsioPolicy));
    ds_->set_tracker_mapper((tracker_mapper_ = new MockITrackerMapper));
    ds_->set_conn_mgr((conn_mgr_ = new MockIConnMgr));
    ds_->set_store((store_ = new MockIStore));
    ds_->set_inode_removal_tracker(
        (inode_removal_tracker_ = new MockIInodeRemovalTracker));
    ds_->set_durable_range((durable_range_ = new MockIDurableRange));
    ds_->set_ds_fim_processor((ds_fim_processor_ = new MockIFimProcessor));
    ds_->set_fc_fim_processor((fc_fim_processor_ = new MockIFimProcessor));
    ds_->set_degraded_cache((degraded_cache_ = new MockIDegradedCache));
    ds_->set_req_completion_checker_set((req_completion_checker_set_ =
                                         new MockIReqCompletionCheckerSet));
    ds_->set_resync_fim_processor(resync_fim_proc_.get());
    ds_->set_resync_mgr(resync_mgr_ = new MockIResyncMgr);
    ds_->set_thread_group(thread_group_ = new MockIThreadGroup);
    ds_->set_dsg_ready_time_keeper(
        dsg_ready_time_keeper_ = new MockITimeKeeper);
    ds_->set_shutdown_mgr(shutdown_mgr_ = new MockIShutdownMgr);
    EXPECT_CALL(*store_, ds_group())
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*store_, ds_role())
        .WillRepeatedly(Return(1));
  }
};

TEST_F(DsFimProcessorTest, DSMSProcMSReg) {
  // Successful
  EXPECT_CALL(*store_, SetRole(0U, 2U));
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  boost::shared_ptr<IFimSocket> i_fim_socket = fim_socket;
  EXPECT_CALL(*tracker_mapper_, SetMSFimSocket(i_fim_socket, true));
  FimSocketCleanupCallback cleanup_callback;
  EXPECT_CALL(*fim_socket, OnCleanup(_))
      .WillOnce(SaveArg<0>(&cleanup_callback));
  EXPECT_CALL(*fim_socket, name());
  EXPECT_CALL(*fim_socket, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));

  FIM_PTR<DSMSRegSuccessFim> reg_success_fim =
      DSMSRegSuccessFim::MakePtr();
  (*reg_success_fim)->ds_group = 0;
  (*reg_success_fim)->ds_role = 2;
  ms_ctrl_fim_proc_->Process(reg_success_fim, fim_socket);

  // Trigger cleanup
  EXPECT_CALL(*conn_mgr_, ReconnectMS(i_fim_socket));

  cleanup_callback();

  // Rejection
  EXPECT_CALL(*conn_mgr_, DisconnectMS(i_fim_socket));

  ms_ctrl_fim_proc_->Process(MSRegRejectedFim::MakePtr(), fim_socket);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(DsFimProcessorTest, DSMSProcMSTopologyChange) {
  FIM_PTR<TopologyChangeFim> fim(new TopologyChangeFim);
  (*fim)->type = 'D';
  (*fim)->joined = '\x01';
  (*fim)->ds_group = 0;
  (*fim)->ds_role = 3;
  (*fim)->ip = 12345678;
  (*fim)->port = 5000;
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);

  // New connect
  EXPECT_CALL(*conn_mgr_, ConnectDS(12345678, 5000, 0, 3));

  ms_ctrl_fim_proc_->Process(fim, fim_socket);

  // Disconnect
  boost::shared_ptr<MockIFimSocket> ds_fim_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 3))
      .WillOnce(Return(ds_fim_socket));
  EXPECT_CALL(*ds_fim_socket, Shutdown());

  (*fim)->joined = '\x00';
  ms_ctrl_fim_proc_->Process(fim, fim_socket);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(DsFimProcessorTest, DSMSProcDSGStateChangeDegraded) {
  FIM_PTR<IFim> enqueued_fim;
  EXPECT_CALL(*thread_group_, EnqueueAll(_))
      .WillOnce(SaveArg<0>(&enqueued_fim));
  EXPECT_CALL(*dsg_ready_time_keeper_, Stop());
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*durable_range_, SetConservative(true));
  EXPECT_CALL(*degraded_cache_, SetActive(true));

  FIM_PTR<DSGStateChangeFim> fim(new DSGStateChangeFim);
  (*fim)->state_change_id = 7;
  (*fim)->state = kDSGDegraded;
  (*fim)->failed = 2;
  (*fim)->distress = 1;
  ms_ctrl_fim_proc_->Process(fim, fim_socket);
  ASSERT_EQ(kDSGDistressModeChangeFim, enqueued_fim->type());
  EXPECT_EQ(1U, static_cast<DSGDistressModeChangeFim&>(*enqueued_fim)->
            distress);
  uint64_t state_change_id;
  GroupRole failed_role;
  EXPECT_EQ(kDSGDegraded, ds_->dsg_state(&state_change_id, &failed_role));
  EXPECT_EQ(7U, state_change_id);
  EXPECT_EQ(2U, failed_role);
  DSGStateChangeAckFim& ack_fim = dynamic_cast<DSGStateChangeAckFim&>(*reply);
  EXPECT_EQ(7U, ack_fim->state_change_id);
}

TEST_F(DsFimProcessorTest, DSMSProcDSGStateChangeRecoveringOther) {
  EXPECT_CALL(*thread_group_, EnqueueAll(_));
  EXPECT_CALL(*dsg_ready_time_keeper_, Stop());
  boost::function<void()> cb;
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAllInodes(_))
      .WillOnce(SaveArg<0>(&cb));

  // Process Fim, state change immediately
  FIM_PTR<DSGStateChangeFim> fim(new DSGStateChangeFim);
  (*fim)->state_change_id = 7;
  (*fim)->state = kDSGRecovering;
  (*fim)->failed = 2;
  (*fim)->opt_resync = '\0';
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  ms_ctrl_fim_proc_->Process(fim, fim_socket);
  uint64_t state_change_id;
  GroupRole failed_role;
  EXPECT_EQ(kDSGRecovering, ds_->dsg_state(&state_change_id, &failed_role));
  EXPECT_EQ(7U, state_change_id);
  EXPECT_EQ(2U, failed_role);

  // If state changed on inode completion, only the cache is set inactive
  EXPECT_CALL(*degraded_cache_, SetActive(false));

  ds_->set_dsg_state(7, kDSGDegraded, 2);
  cb();
  EXPECT_FALSE(ds_->opt_resync());

  // Otherwise, reply is sent, expecting flag is set
  ds_->set_dsg_state(7, kDSGRecovering, 2);
  EXPECT_CALL(*degraded_cache_, SetActive(false));
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 2))
      .WillRepeatedly(Return(tracker));
  EXPECT_CALL(*tracker, SetExpectingFimSocket(true));
  IOService service;
  DeadlineTimer* timer = new DeadlineTimer(service);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  boost::function<void(const boost::system::error_code&)> expecting_cb;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 15, _))
      .WillOnce(SaveArg<2>(&expecting_cb));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket, WriteMsg(_))
     .WillOnce(SaveArg<0>(&reply));

  cb();
  DSGStateChangeAckFim& ack_fim = dynamic_cast<DSGStateChangeAckFim&>(*reply);
  EXPECT_EQ(7U, ack_fim->state_change_id);

  // When expecting timer expires, unset expecting
  EXPECT_CALL(*tracker, SetExpectingFimSocket(false));

  boost::system::error_code error;
  expecting_cb(error);

  Mock::VerifyAndClear(tracker_mapper_);
  Mock::VerifyAndClear(asio_policy_);
}

TEST_F(DsFimProcessorTest, DSMSProcDSGStateChangeRecoveringSelf) {
  EXPECT_CALL(*thread_group_, EnqueueAll(_));
  EXPECT_CALL(*dsg_ready_time_keeper_, Stop());
  boost::function<void()> cb;
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAllInodes(_))
      .WillOnce(SaveArg<0>(&cb));

  // Process Fim, state change immediately
  FIM_PTR<DSGStateChangeFim> fim(new DSGStateChangeFim);
  (*fim)->state_change_id = 7;
  (*fim)->state = kDSGRecovering;
  (*fim)->failed = 1;
  (*fim)->opt_resync = '\1';
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  ms_ctrl_fim_proc_->Process(fim, fim_socket);
  uint64_t state_change_id;
  GroupRole failed_role;
  EXPECT_EQ(kDSGRecovering, ds_->dsg_state(&state_change_id, &failed_role));
  EXPECT_EQ(7U, state_change_id);
  EXPECT_EQ(1U, failed_role);

  // If state not changed on inode completion, reply is sent, resync
  // FimProcessor is initialized
  EXPECT_CALL(*degraded_cache_, SetActive(false));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket, WriteMsg(_))
     .WillOnce(SaveArg<0>(&reply));
  ResyncCompleteHandler resync_completion_handler;
  EXPECT_CALL(*resync_fim_proc_, AsyncResync(_))
      .WillOnce(SaveArg<0>(&resync_completion_handler));

  cb();
  DSGStateChangeAckFim& ack_fim = dynamic_cast<DSGStateChangeAckFim&>(*reply);
  EXPECT_EQ(7U, ack_fim->state_change_id);
  EXPECT_TRUE(ds_->opt_resync());

  // If successful, notify MS
  FIM_PTR<IFim> notify_fim;
  boost::shared_ptr<MockIFimSocket> ms_fim_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket));
  EXPECT_CALL(*ms_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&notify_fim));

  resync_completion_handler(true);
  EXPECT_EQ(kDSResyncEndFim, notify_fim->type());

  // If failed, log error
  MockLogCallback log_cb;
  LogRoute route(log_cb.GetLogCallback());
  EXPECT_CALL(log_cb, Call(PLEVEL(error, Server),
                           StrEq("DS Resync failed"), _));

  resync_completion_handler(false);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(DsFimProcessorTest, DSMSProcDSGStateChangeFromRecovering) {
  ds_->set_dsg_state(6, kDSGRecovering, 0);

  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(false));
  EXPECT_CALL(*durable_range_, SetConservative(false));
  EXPECT_CALL(*dsg_ready_time_keeper_, Start());
  FIM_PTR<IFim> fim1, fim2;
  EXPECT_CALL(*thread_group_, EnqueueAll(_))
      .WillOnce(SaveArg<0>(&fim1))
      .WillOnce(SaveArg<0>(&fim2));
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*fim_socket, WriteMsg(_));

  FIM_PTR<DSGStateChangeFim> fim(new DSGStateChangeFim);
  (*fim)->state_change_id = 7;
  (*fim)->state = kDSGReady;
  (*fim)->failed = 0;
  (*fim)->distress = 0;
  ms_ctrl_fim_proc_->Process(fim, fim_socket);
  DSGDistressModeChangeFim& rfim1 =
      dynamic_cast<DSGDistressModeChangeFim&>(*fim1);
  EXPECT_EQ(0U, rfim1->distress);
  dynamic_cast<DeferResetFim&>(*fim2);
}

TEST_F(DsFimProcessorTest, DSMSProcDSGStateChangeShuttingDown) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);

  EXPECT_CALL(*thread_group_, EnqueueAll(_));
  EXPECT_CALL(*dsg_ready_time_keeper_, Stop());
  boost::function<void()> cb;
  EXPECT_CALL(*shutdown_mgr_, Init(_));
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAllInodes(_))
      .WillOnce(SaveArg<0>(&cb));

  ds_->set_dsg_state(6, kDSGReady, 0);
  FIM_PTR<DSGStateChangeFim> fim(new DSGStateChangeFim);
  (*fim)->state_change_id = 7;
  (*fim)->state = kDSGShuttingDown;
  (*fim)->failed = 0;
  ms_ctrl_fim_proc_->Process(fim, fim_socket);

  EXPECT_CALL(*fim_socket, WriteMsg(_));
  cb();
}

TEST_F(DsFimProcessorTest, DSMSProcDSGStateChangeUnknown) {
  EXPECT_CALL(*thread_group_, EnqueueAll(_));
  EXPECT_CALL(*dsg_ready_time_keeper_, Stop());
  MockLogCallback log_cb;
  LogRoute route(log_cb.GetLogCallback());
  EXPECT_CALL(log_cb,
      Call(PLEVEL(notice, Server),
           StrEq("DSG state change, state is now: Unknown"), _));

  FIM_PTR<DSGStateChangeFim> fim(new DSGStateChangeFim);
  (*fim)->state_change_id = 7;
  (*fim)->state = 100;  // Unknown state
  (*fim)->failed = 0;

  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  ms_ctrl_fim_proc_->Process(fim, fim_socket);
}

TEST_F(DsFimProcessorTest, DSMSProcDSGDistressModeChange) {
  FIM_PTR<DSGDistressModeChangeFim> fim(new DSGDistressModeChangeFim);
  EXPECT_CALL(*thread_group_, EnqueueAll(
      boost::static_pointer_cast<IFim>(fim)));
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  ms_ctrl_fim_proc_->Process(fim, fim_socket);
}

TEST_F(DsFimProcessorTest, DSMSSysHalt) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*shutdown_mgr_, Shutdown());

  ms_ctrl_fim_proc_->Process(SysHaltFim::MakePtr(), fim_socket);
}

TEST_F(DsFimProcessorTest, ConfigList) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  ms_ctrl_fim_proc_->Process(ClusterConfigListReqFim::MakePtr(), fim_socket);
}

TEST_F(DsFimProcessorTest, DSMSProcDSResyncReq) {
  FIM_PTR<DSResyncReqFim> fim = DSResyncReqFim::MakePtr();
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);

  // State is not recovering: just ignore
  ms_ctrl_fim_proc_->Process(fim, fim_socket);

  // State is recovering, I've failed: do nothing further
  // (already set up during state change)
  ds_->set_dsg_state(7, kDSGRecovering, 1);
  ms_ctrl_fim_proc_->Process(fim, fim_socket);

  // State is recovering, peer failed: start sending
  EXPECT_CALL(*resync_mgr_, Start(2));

  ds_->set_dsg_state(7, kDSGRecovering, 2);
  ms_ctrl_fim_proc_->Process(fim, fim_socket);
}

TEST_F(DsFimProcessorTest, InitDSProcDSReg) {
  // Test DS registers
  FIM_PTR<DSDSRegFim> reg_fim = DSDSRegFim::MakePtr();
  (*reg_fim)->ds_group = 0;
  (*reg_fim)->ds_role = 1;
  EXPECT_CALL(*tracker_mapper_, SetDSFimSocket(_, GroupId(0), GroupRole(1),
                                               true));
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*fim_socket, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*fim_socket, Migrate(ds_asio_policy_));
  EXPECT_CALL(*fim_socket, SetFimProcessor(ds_fim_processor_));

  init_fim_proc_->Process(reg_fim, fim_socket);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(DsFimProcessorTest, InitDSProcFCReg) {
  FIM_PTR<FCDSRegFim> reg_fim = FCDSRegFim::MakePtr();
  (*reg_fim)->client_num = 1;
  EXPECT_CALL(*tracker_mapper_, SetFCFimSocket(_, 1, true));
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*fim_socket, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*fim_socket, Migrate(fc_asio_policy_));
  EXPECT_CALL(*fim_socket, SetFimProcessor(fc_fim_processor_));

  init_fim_proc_->Process(reg_fim, fim_socket);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(DsFimProcessorTest, StatFS) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*store_, Stat(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(2),
                      SetArgPointee<1>(1),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  ms_ctrl_fim_proc_->Process(MSStatFSFim::MakePtr(), fim_socket);
  const MSStatFSReplyFim& rreply = static_cast<MSStatFSReplyFim&>(*reply);
  EXPECT_EQ(2U, rreply->total_space);
  EXPECT_EQ(1U, rreply->free_space);

  // Error case
  EXPECT_CALL(*store_, Stat(_, _))
      .WillOnce(Return(-EACCES));
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  ms_ctrl_fim_proc_->Process(MSStatFSFim::MakePtr(), fim_socket);
  const ResultCodeReplyFim& error = static_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(uint32_t(EACCES), error->err_no);
}

TEST_F(DsFimProcessorTest, ConfigChange) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  FIM_PTR<PeerConfigChangeReqFim> fim1 = PeerConfigChangeReqFim::MakePtr();
  strncpy((*fim1)->name, "log_severity", 13);
  strncpy((*fim1)->value, "5", 2);
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  ms_ctrl_fim_proc_->Process(fim1, fim_socket);
  EXPECT_EQ("5", ds_->configs().log_severity());

  FIM_PTR<PeerConfigChangeReqFim> fim2 = PeerConfigChangeReqFim::MakePtr();
  strncpy((*fim2)->name, "log_path", 9);
  strncpy((*fim2)->value, "/tmp", 5);
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  ms_ctrl_fim_proc_->Process(fim2, fim_socket);
  EXPECT_EQ("/tmp", ds_->configs().log_path());
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
