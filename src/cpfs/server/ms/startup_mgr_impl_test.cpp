/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/startup_mgr_impl.hpp"

#include <stdexcept>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "common.hpp"
#include "periodic_timer_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/startup_mgr.hpp"
#include "server/ms/stat_keeper_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/topology_mock.hpp"
#include "server/server_info_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class StartupMgrTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  MockIAsioPolicy* asio_policy_;
  MockIStateMgr* state_mgr_;
  MockIServerInfo* server_info_;
  MockITopologyMgr* topology_mgr_;
  MockIStatKeeper* stat_keeper_;
  MockPeriodicTimerMaker periodic_timer_maker_;
  boost::scoped_ptr<IStartupMgr> startup_mgr_;
  IOService io_service_;

  StartupMgrTest() {
    server_.set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    server_.set_state_mgr(state_mgr_ = new MockIStateMgr);
    server_.set_server_info(server_info_ = new MockIServerInfo);
    server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    server_.set_stat_keeper(stat_keeper_ = new MockIStatKeeper);
    startup_mgr_.reset(MakeStartupMgr(&server_));
    startup_mgr_->SetPeriodicTimerMaker(periodic_timer_maker_.GetMaker());
  }
};

TEST_F(StartupMgrTest, Empty) {
  EXPECT_CALL(*server_info_, Get("swait", ""))
      .WillOnce(Return(""));
  EXPECT_CALL(*asio_policy_, io_service())
      .WillOnce(Return(&io_service_));
  boost::shared_ptr<MockIPeriodicTimer> periodic_timer(new MockIPeriodicTimer);
  EXPECT_CALL(periodic_timer_maker_, Make(&io_service_, _))
      .WillOnce(Return(periodic_timer));
  EXPECT_CALL(*periodic_timer, OnTimeout(_));

  startup_mgr_->Init();
  GroupRole failed;
  EXPECT_FALSE(startup_mgr_->dsg_degraded(1, &failed));

  EXPECT_CALL(*server_info_, Set("swait", ""));

  startup_mgr_->set_dsg_degraded(0, false, 0);

  Mock::VerifyAndClear(&periodic_timer_maker_);
}

TEST_F(StartupMgrTest, Degraded) {
  EXPECT_CALL(*server_info_, Get("swait", ""))
      .WillOnce(Return("1-2,2-0"));
  EXPECT_CALL(*topology_mgr_, SetDSLost(1, 2));
  EXPECT_CALL(*topology_mgr_, SetDSLost(2, 0));
  EXPECT_CALL(*asio_policy_, io_service())
      .WillOnce(Return(&io_service_));
  boost::shared_ptr<MockIPeriodicTimer> periodic_timer(new MockIPeriodicTimer);
  EXPECT_CALL(periodic_timer_maker_, Make(&io_service_, _))
      .WillOnce(Return(periodic_timer));
  EXPECT_CALL(*periodic_timer, OnTimeout(_));

  startup_mgr_->Init();
  GroupRole failed;
  EXPECT_TRUE(startup_mgr_->dsg_degraded(1, &failed));
  EXPECT_EQ(2U, failed);
  EXPECT_TRUE(startup_mgr_->dsg_degraded(2, &failed));
  EXPECT_EQ(0U, failed);
  EXPECT_FALSE(startup_mgr_->dsg_degraded(0, &failed));

  EXPECT_CALL(*server_info_, Set("swait", "1-2,2-0"));

  startup_mgr_->set_dsg_degraded(1, true, 2);

  EXPECT_CALL(*server_info_, Set("swait", "2-0"));

  startup_mgr_->set_dsg_degraded(1, false, 0);

  EXPECT_CALL(*server_info_, Set("swait", ""));

  startup_mgr_->set_dsg_degraded(2, false, 0);

  EXPECT_CALL(*server_info_, Set("swait", "3-1"));

  startup_mgr_->set_dsg_degraded(3, true, 1);

  Mock::VerifyAndClear(&periodic_timer_maker_);
}

TEST_F(StartupMgrTest, ForceStart) {
  // Get timeout callback
  EXPECT_CALL(*server_info_, Get("swait", ""))
      .WillOnce(Return(""));
  EXPECT_CALL(*asio_policy_, io_service())
      .WillOnce(Return(&io_service_));
  boost::shared_ptr<MockIPeriodicTimer> periodic_timer(new MockIPeriodicTimer);
  EXPECT_CALL(periodic_timer_maker_, Make(&io_service_, 300))
      .WillOnce(Return(periodic_timer));
  PeriodicTimerCallback timeout;
  EXPECT_CALL(*periodic_timer, OnTimeout(_))
      .WillOnce(SaveArg<0>(&timeout));

  startup_mgr_->Init();

  // If not active, stop
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateStandby));

  EXPECT_FALSE(timeout());

  // If pending, just return
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateHANotReady));

  EXPECT_TRUE(timeout());

  // Otherwise (active), if all DSG already ready, stop
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*topology_mgr_, AllDSReady())
      .WillOnce(Return(true));

  EXPECT_FALSE(timeout());

  // Otherwise (some DSG not ready), if some DSG not sufficient, return
  EXPECT_CALL(*topology_mgr_, AllDSReady())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*topology_mgr_, AllDSGStartable())
      .WillOnce(Return(false));

  EXPECT_TRUE(timeout());

  // Otherwise (all DSG sufficient), record
  EXPECT_CALL(*topology_mgr_, AllDSGStartable())
      .WillRepeatedly(Return(true));

  EXPECT_TRUE(timeout());

  // If happens once more, will trigger force start.  If failed, will retry
  EXPECT_CALL(*topology_mgr_, ForceStartDSG())
      .WillOnce(Return(false));

  EXPECT_TRUE(timeout());

  // If force start successful, will stop
  EXPECT_CALL(*topology_mgr_, ForceStartDSG())
      .WillOnce(Return(true));
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*stat_keeper_, Run());

  EXPECT_FALSE(timeout());

  Mock::VerifyAndClear(&periodic_timer_maker_);
}

TEST_F(StartupMgrTest, Error) {
  EXPECT_CALL(*server_info_, Get("swait", ""))
      .WillOnce(Return("0"));

  EXPECT_THROW(startup_mgr_->Init(), std::runtime_error);

  EXPECT_CALL(*server_info_, Get("swait", ""))
      .WillOnce(Return("0-1-2"));

  EXPECT_THROW(startup_mgr_->Init(), std::runtime_error);

  EXPECT_CALL(*server_info_, Get("swait", ""))
      .WillOnce(Return("a-1"));

  EXPECT_THROW(startup_mgr_->Init(), std::runtime_error);
}

TEST_F(StartupMgrTest, Reset) {
  // Init
  {
    EXPECT_CALL(*server_info_, Get("swait", ""))
        .WillOnce(Return(""));
    EXPECT_CALL(*asio_policy_, io_service())
        .WillOnce(Return(&io_service_));
    boost::shared_ptr<MockIPeriodicTimer> periodic_timer(
        new MockIPeriodicTimer);
    EXPECT_CALL(periodic_timer_maker_, Make(&io_service_, _))
        .WillOnce(Return(periodic_timer));
    EXPECT_CALL(*periodic_timer, OnTimeout(_));

    startup_mgr_->Init();

    GroupRole failed;
    EXPECT_FALSE(startup_mgr_->dsg_degraded(1, &failed));
    EXPECT_FALSE(startup_mgr_->dsg_degraded(0, &failed));
    Mock::VerifyAndClear(&periodic_timer_maker_);
  }

  // Reset
  EXPECT_CALL(*server_info_, Set("swait", "1-3,0-2"));
  EXPECT_CALL(*server_info_, Get("swait", ""))
      .WillOnce(Return("1-3,0-2"));
  EXPECT_CALL(*asio_policy_, io_service())
      .WillOnce(Return(&io_service_));
  boost::shared_ptr<MockIPeriodicTimer> periodic_timer(new MockIPeriodicTimer);
  EXPECT_CALL(periodic_timer_maker_, Make(&io_service_, _))
      .WillOnce(Return(periodic_timer));
  EXPECT_CALL(*periodic_timer, OnTimeout(_));

  EXPECT_CALL(*topology_mgr_, SetDSLost(1, 3));
  EXPECT_CALL(*topology_mgr_, SetDSLost(0, 2));

  startup_mgr_->Reset("1-3,0-2");

  GroupRole failed;
  EXPECT_TRUE(startup_mgr_->dsg_degraded(0, &failed));
  EXPECT_EQ(2U, failed);
  EXPECT_TRUE(startup_mgr_->dsg_degraded(1, &failed));
  EXPECT_EQ(3U, failed);
  Mock::VerifyAndClear(&periodic_timer_maker_);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
