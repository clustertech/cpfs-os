/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/failover_mgr_impl.hpp"

#include <stdexcept>
#include <vector>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "common.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "periodic_timer_mock.hpp"
#include "req_tracker_mock.hpp"
#include "shutdown_mgr_mock.hpp"
#include "thread_fim_processor_mock.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/conn_mgr_mock.hpp"
#include "server/ms/dirty_inode_mock.hpp"
#include "server/ms/failover_mgr.hpp"
#include "server/ms/inode_src_mock.hpp"
#include "server/ms/replier_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/topology_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::DoAll;
using ::testing::Mock;
using ::testing::SaveArg;
using ::testing::_;

namespace cpfs {
namespace server {
namespace ms {
namespace {

using ::testing::Return;

class FailoverMgrTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  MockIAsioPolicy* asio_policy_;
  IOService io_service_;
  MockIStateMgr* state_;
  MockITopologyMgr* topology_mgr_;
  MockITrackerMapper* tracker_mapper_;
  boost::shared_ptr<MockIReqTracker> ds_trackers_[kNumDSPerGroup];
  MockIThreadFimProcessor* failover_proc_;
  MockIFimProcessor* fc_fim_proc_;
  MockIReplier* replier_;
  MockIInodeRemovalTracker* inode_removal_tracker_;
  MockIDurableRange* durable_range_;
  MockIInodeSrc* inode_src_;
  MockITimeKeeper* time_keeper_;
  MockIHACounter* ha_counter_;
  MockPeriodicTimerMaker periodic_timer_maker_;
  MockIConnMgr* conn_mgr_;
  MockIThreadGroup* thread_group_;
  MockIDirtyInodeMgr* dirty_inode_mgr_;
  MockIShutdownMgr* shutdown_mgr_;
  boost::scoped_ptr<IFailoverMgr> failover_mgr_;

  FailoverMgrTest()
      : failover_mgr_(MakeFailoverMgr(&server_)) {
    server_.set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    EXPECT_CALL(*asio_policy_, io_service())
        .WillRepeatedly(Return(&io_service_));
    server_.set_state_mgr(state_ = new MockIStateMgr);
    server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      ds_trackers_[r].reset(new MockIReqTracker);
      EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, r))
          .WillRepeatedly(Return(ds_trackers_[r]));
    }
    server_.set_failover_processor(
        failover_proc_ = new MockIThreadFimProcessor);
    server_.set_fc_fim_processor(fc_fim_proc_ = new MockIFimProcessor);
    server_.set_replier(replier_ = new MockIReplier);
    server_.set_inode_removal_tracker(
        inode_removal_tracker_ = new MockIInodeRemovalTracker);
    server_.set_durable_range(durable_range_ = new MockIDurableRange);
    server_.set_inode_src(inode_src_ = new MockIInodeSrc);
    server_.set_peer_time_keeper(
        time_keeper_ = new MockITimeKeeper);
    server_.set_ha_counter(ha_counter_ = new MockIHACounter);
    server_.set_conn_mgr(conn_mgr_ = new MockIConnMgr);
    server_.set_thread_group(thread_group_ = new MockIThreadGroup);
    server_.set_dirty_inode_mgr(dirty_inode_mgr_ = new MockIDirtyInodeMgr);
    server_.set_shutdown_mgr(shutdown_mgr_ = new MockIShutdownMgr);
    EXPECT_CALL(*topology_mgr_, num_groups())
        .WillRepeatedly(Return(1));
    failover_mgr_->SetPeriodicTimerMaker(periodic_timer_maker_.GetMaker());
  }

  ~FailoverMgrTest() {
    Mock::VerifyAndClear(tracker_mapper_);
  }
};

TEST_F(FailoverMgrTest, Unexpected) {
  EXPECT_THROW(failover_mgr_->AddReconfirmDone(1), std::logic_error);
}

TEST_F(FailoverMgrTest, TimeoutOnce) {
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    EXPECT_CALL(*topology_mgr_, HasDS(0, r))
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*ds_trackers_[r], SetExpectingFimSocket(true));
  }
  EXPECT_CALL(*state_, SwitchState(kStateFailover))
      .WillOnce(Return(true));
  EXPECT_CALL(*ha_counter_, SetActive());
  EXPECT_CALL(*time_keeper_, Stop());
  boost::shared_ptr<MockIPeriodicTimer> periodic_timer(new MockIPeriodicTimer);
  EXPECT_CALL(periodic_timer_maker_, Make(&io_service_, 40))
      .WillOnce(Return(periodic_timer));
  PeriodicTimerCallback callback;
  EXPECT_CALL(*periodic_timer, OnTimeout(_))
      .WillOnce(SaveArg<0>(&callback));
  EXPECT_CALL(*topology_mgr_, GetNumFCs())
      .WillRepeatedly(Return(3));

  failover_mgr_->Start(40);

  // First confirmed
  std::vector<ClientNum> clients;
  clients.push_back(1);
  clients.push_back(2);
  clients.push_back(3);
  EXPECT_CALL(*topology_mgr_, GetFCs())
      .WillRepeatedly(Return(clients));

  failover_mgr_->AddReconfirmDone(1);

  // Timeout, Reset all DS tracker as non-expecting, cleanup missing sockets
  boost::shared_ptr<MockIFimSocket> ds_sockets[kNumDSPerGroup];
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    EXPECT_CALL(*ds_trackers_[r], SetExpectingFimSocket(false));
    if (r == 0) {
      EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, r))
          .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
    } else {
      ds_sockets[r].reset(new MockIFimSocket);
      EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, r))
          .WillRepeatedly(Return(ds_sockets[r]));
    }
  }
  EXPECT_CALL(*conn_mgr_, CleanupDS(0, 0));
  boost::shared_ptr<MockIFimSocket> fc_sockets[3];
  for (ClientNum n = 1; n <= 3; ++n) {
    if (n == 3) {
      EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(n))
          .WillRepeatedly(Return(boost::shared_ptr<IFimSocket>()));
    } else {
      fc_sockets[n].reset(new MockIFimSocket);
      EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(n))
          .WillRepeatedly(Return(fc_sockets[n]));
    }
  }
  EXPECT_CALL(*tracker_mapper_, SetFCFimSocket(_, 3, true));
  EXPECT_CALL(*tracker_mapper_,
              SetFCFimSocket(boost::shared_ptr<IFimSocket>(), 3, true));
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  boost::shared_ptr<IReqTracker> itracker = tracker;
  EXPECT_CALL(*tracker_mapper_, GetFCTracker(3))
      .WillOnce(Return(tracker));
  EXPECT_CALL(*conn_mgr_, CleanupFC(_));

  EXPECT_TRUE(callback());

  // Second confirmed, switch all states
  clients.clear();
  clients.push_back(1);
  clients.push_back(2);
  EXPECT_CALL(*topology_mgr_, GetFCs())
      .WillRepeatedly(Return(clients));
  EXPECT_CALL(*inode_src_, SetupAllocation());
  EXPECT_CALL(*state_, SwitchState(kStateActive))
      .WillOnce(Return(true));
  EXPECT_CALL(*dirty_inode_mgr_, ClearCleaning());
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*ha_counter_, ConfirmActive());
  ServiceTask task;
  EXPECT_CALL(*asio_policy_, Post(_))
      .WillOnce(SaveArg<0>(&task));
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*durable_range_, SetConservative(true));
  for (ClientNum n = 1; n <= 2; ++n) {
    EXPECT_CALL(*fc_sockets[n], SetFimProcessor(fc_fim_proc_));
    EXPECT_CALL(*fc_sockets[n], WriteMsg(_));
  }
  EXPECT_CALL(server_, PrepareActivate());

  failover_mgr_->AddReconfirmDone(2);

  // The above routine sets up a task.  If run, will redo callbacks
  EXPECT_CALL(*replier_, RedoCallbacks());

  task();

  Mock::VerifyAndClear(&periodic_timer_maker_);
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(FailoverMgrTest, NoFC) {
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_CALL(*topology_mgr_, HasDS(0, r))
        .WillRepeatedly(Return(false));
  EXPECT_CALL(*state_, SwitchState(kStateFailover))
      .WillOnce(Return(true));
  EXPECT_CALL(*dirty_inode_mgr_, ClearCleaning());
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*inode_src_, SetupAllocation());
  EXPECT_CALL(*ha_counter_, SetActive());
  EXPECT_CALL(*time_keeper_, Stop());
  boost::shared_ptr<MockIPeriodicTimer> periodic_timer(new MockIPeriodicTimer);
  EXPECT_CALL(periodic_timer_maker_, Make(&io_service_, 40))
      .WillOnce(Return(periodic_timer));
  PeriodicTimerCallback callback;
  EXPECT_CALL(*periodic_timer, OnTimeout(_))
      .WillOnce(SaveArg<0>(&callback));
  EXPECT_CALL(*topology_mgr_, GetNumFCs())
      .WillRepeatedly(Return(0));
  EXPECT_CALL(*state_, SwitchState(kStateActive))
      .WillOnce(Return(true));
  EXPECT_CALL(*ha_counter_, ConfirmActive());
  ServiceTask task;
  EXPECT_CALL(*asio_policy_, Post(_))
      .WillOnce(SaveArg<0>(&task));
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*durable_range_, SetConservative(true));
  EXPECT_CALL(*topology_mgr_, GetFCs())
      .WillRepeatedly(Return(std::vector<ClientNum>()));
  EXPECT_CALL(server_, PrepareActivate());

  failover_mgr_->Start(40);

  // The above sets up a task to call RedoCallback
  EXPECT_CALL(*replier_, RedoCallbacks());

  task();

  // Timeout, Reset all DS tracker as non-expecting
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_CALL(*ds_trackers_[r], SetExpectingFimSocket(false));

  EXPECT_FALSE(callback());

  Mock::VerifyAndClear(&periodic_timer_maker_);
}

TEST_F(FailoverMgrTest, ResyncAbort) {
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    EXPECT_CALL(*topology_mgr_, HasDS(0, r))
        .WillRepeatedly(Return(false));
  EXPECT_CALL(*state_, SwitchState(kStateFailover))
      .WillOnce(Return(false));
  EXPECT_CALL(*state_, GetState())
      .WillOnce(Return(kStateResync));
  EXPECT_CALL(*shutdown_mgr_, Shutdown());
  failover_mgr_->Start(40);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
