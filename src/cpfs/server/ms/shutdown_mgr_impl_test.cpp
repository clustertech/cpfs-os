/* Copyright 2014 ClusterTech Ltd */
#include "server/ms/shutdown_mgr_impl.hpp"

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "fim_socket_mock.hpp"
#include "io_service_runner_mock.hpp"
#include "periodic_timer_mock.hpp"
#include "shutdown_mgr.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/topology_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class MSShutdownMgrTest : public ::testing::Test {
 protected:
  IOService service_;
  MockIAsioPolicy* asio_policy_;
  MockBaseMetaServer server_;
  MockIHACounter* ha_counter_;
  MockIStateMgr* state_mgr_;
  MockIThreadGroup* thread_group_;
  MockIOServiceRunner* io_service_runner_;
  MockITopologyMgr* topology_mgr_;
  MockITrackerMapper* tracker_mapper_;
  boost::shared_ptr<MockIFimSocket> ms_socket_;
  boost::scoped_ptr<IShutdownMgr> mgr_;
  MockPeriodicTimerMaker timer_maker_;
  boost::shared_ptr<MockIPeriodicTimer> timer_;

  MSShutdownMgrTest()
      : ms_socket_(new MockIFimSocket),
        timer_(new MockIPeriodicTimer) {
    server_.set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    EXPECT_CALL(*asio_policy_, io_service())
        .WillRepeatedly(Return(&service_));
    server_.set_ha_counter(ha_counter_ = new MockIHACounter);
    server_.set_state_mgr(state_mgr_ = new MockIStateMgr);
    server_.set_thread_group(thread_group_ = new MockIThreadGroup);
    server_.set_io_service_runner(io_service_runner_ = new MockIOServiceRunner);
    server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    server_.configs().set_ms2_host("0.0.0.0");  // HA mode

    mgr_.reset(MakeShutdownMgr(&server_, timer_maker_.GetMaker()));
    mgr_->SetAsioPolicy(asio_policy_);
  }
};

TEST_F(MSShutdownMgrTest, ActiveShutdown) {
  EXPECT_CALL(*ha_counter_, IsActive())
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*topology_mgr_, AnnounceShutdown());
  EXPECT_CALL(*thread_group_, Stop(-1));
  EXPECT_CALL(*state_mgr_, SwitchState(kStateShuttingDown));
  DeadlineTimer* timer = new DeadlineTimer(service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 100, _));

  EXPECT_TRUE(mgr_->Init(100));

  // Shutdown
  EXPECT_CALL(*topology_mgr_, AnnounceHalt());
  EXPECT_CALL(timer_maker_, Make(_, _))
      .WillOnce(Return(timer_));
  boost::function<bool()> callback;
  EXPECT_CALL(*timer_, OnTimeout(_))
      .WillOnce(SaveArg<0>(&callback));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 0, _));

  EXPECT_TRUE(mgr_->Shutdown());

  // Wait until all Fims sent
  EXPECT_CALL(*tracker_mapper_, HasPendingWrite())
      .WillOnce(Return(true));

  callback();

  EXPECT_CALL(*tracker_mapper_, HasPendingWrite())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_socket_));

  callback();

  EXPECT_CALL(*tracker_mapper_, HasPendingWrite())
      .WillOnce(Return(false));
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));
  EXPECT_CALL(*io_service_runner_, Stop());

  callback();

  Mock::VerifyAndClear(tracker_mapper_);

  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 0, _));
}

TEST_F(MSShutdownMgrTest, StandbyShutdown) {
  EXPECT_CALL(*ha_counter_, IsActive())
        .WillOnce(Return(false));
  EXPECT_CALL(*io_service_runner_, Stop());

  mgr_->Shutdown();
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
