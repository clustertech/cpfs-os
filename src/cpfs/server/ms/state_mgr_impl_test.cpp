/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/state_mgr_impl.hpp"

#include <sys/stat.h>

#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "common.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "server/ms/state_mgr.hpp"

using ::testing::_;
using ::testing::MockFunction;
using ::testing::StartsWith;
using ::testing::StrEq;
using ::testing::Return;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/state_mgr_test_XXXXXX";

class MSStateMgrTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;

  MSStateMgrTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()) {}
};

TEST_F(MSStateMgrTest, MS1Elect) {
  boost::scoped_ptr<IHACounter> counter(MakeHACounter(data_path_, "MS1"));
  EXPECT_EQ(1U, counter->GetCount());
  EXPECT_TRUE(counter->Elect(0, false));
  EXPECT_TRUE(counter->IsActive());
  EXPECT_EQ(1U, counter->GetCount());
  counter->ConfirmActive();
  EXPECT_EQ(1U, counter->GetCount());
  counter->SetActive();
  counter->ConfirmActive();
  EXPECT_EQ(3U, counter->GetCount());
  // Force it to be another value temporarily
  counter->PersistCount(1);
  boost::scoped_ptr<IHACounter> counter2(MakeHACounter(data_path_, "MS1"));
  EXPECT_EQ(1U, counter2->GetCount());
  // Revert it
  counter->PersistCount();
  counter2.reset(MakeHACounter(data_path_, "MS1"));
  EXPECT_EQ(3U, counter2->GetCount());
  // Reload
  counter.reset(MakeHACounter(data_path_, "MS1"));
  EXPECT_EQ(3U, counter->GetCount());
  // peer active: always lose
  EXPECT_FALSE(counter->Elect(2, true));
  EXPECT_EQ(1U, counter->GetCount());
  // Error case: No permission
  counter->SetActive();
  chmod(data_path_, 0);
  EXPECT_THROW(counter->ConfirmActive(), std::runtime_error);
}

TEST_F(MSStateMgrTest, MS2Elect) {
  boost::scoped_ptr<IHACounter> counter(MakeHACounter(data_path_, "MS2"));
  EXPECT_EQ(0U, counter->GetCount());
  EXPECT_FALSE(counter->Elect(1, false));
  EXPECT_FALSE(counter->IsActive());
  counter->SetActive();
  counter->ConfirmActive();
  EXPECT_EQ(2U, counter->GetCount());
  // Reload
  counter.reset(MakeHACounter(data_path_, "MS1"));
  EXPECT_EQ(2U, counter->GetCount());
  // Error case: Tie
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Server),
                   StartsWith("HA tie occurs"), _));
  EXPECT_FALSE(counter->Elect(2, false));
  // Error case: No permission
  chmod(data_path_, 0);
  counter->SetActive();
  EXPECT_THROW(counter->ConfirmActive(), std::runtime_error);
}

TEST_F(MSStateMgrTest, SwitchState) {
  IOService io_service;
  boost::scoped_ptr<IStateMgr> state_mgr(MakeStateMgr());
  // Standby
  EXPECT_TRUE(state_mgr->SwitchState(kStateStandby));
  EXPECT_EQ(kStateStandby, state_mgr->GetState());
  // Failover
  EXPECT_TRUE(state_mgr->SwitchState(kStateFailover));
  EXPECT_EQ(kStateFailover, state_mgr->GetState());
  // Resync: Both Resync and the Failover states are sticky
  // Resync state is queued until failover completes
  EXPECT_FALSE(state_mgr->SwitchState(kStateResync));
  MockFunction<void()> callback1;
  state_mgr->OnState(kStateResync,
                     boost::bind(GetMockCall(callback1), &callback1));
  // Failover completed
  EXPECT_CALL(callback1, Call());
  EXPECT_TRUE(state_mgr->SwitchState(kStateActive));
  EXPECT_EQ(kStateResync, state_mgr->GetState());
  // Resync completed
  EXPECT_TRUE(state_mgr->SwitchState(kStateActive));
  EXPECT_EQ(kStateActive, state_mgr->GetState());
  // Already active, callback is called immediately
  MockFunction<void()> callback2;
  EXPECT_CALL(callback2, Call());
  state_mgr->OnState(kStateActive,
                     boost::bind(GetMockCall(callback2), &callback2));
  EXPECT_EQ(kStateActive, state_mgr->GetState());

  // Unknown state
  {
    MockLogCallback mock_log_callback;
    LogRoute log_route(mock_log_callback.GetLogCallback());
    EXPECT_CALL(mock_log_callback,
                Call(PLEVEL(notice, Server),
                     StrEq("Server changed to state: Unknown"), _));
    EXPECT_TRUE(state_mgr->SwitchState(MSState(1000)));
  }

  // ShuttingDown: State is finalized
  EXPECT_TRUE(state_mgr->SwitchState(kStateShuttingDown));
  EXPECT_EQ(kStateShuttingDown, state_mgr->GetState());
  EXPECT_FALSE(state_mgr->SwitchState(kStateActive));
  EXPECT_EQ(kStateShuttingDown, state_mgr->GetState());
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
