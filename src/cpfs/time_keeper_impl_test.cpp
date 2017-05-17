/* Copyright 2014 ClusterTech Ltd */
#include "time_keeper_impl.hpp"

#include <inttypes.h>

#include <sys/xattr.h>

#include <cstdio>
#include <ctime>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "time_keeper.hpp"

using ::testing::_;
using ::testing::Return;
using ::testing::StartsWith;

namespace cpfs {
namespace {

const char* kDataPath = "/tmp/time_keeper_test_XXXXXX";

class TimeKeeperTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  boost::scoped_ptr<ITimeKeeper> time_keeper_;

  TimeKeeperTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        time_keeper_(MakeTimeKeeper(data_path_, "user.mt", 1)) {}
};

TEST_F(TimeKeeperTest, Update) {
  EXPECT_EQ(0U, time_keeper_->GetLastUpdate());
  uint64_t start_time = std::time(0);
  EXPECT_FALSE(time_keeper_->Update());
  time_keeper_->Start();
  EXPECT_TRUE(time_keeper_->Update());
  EXPECT_GE(time_keeper_->GetLastUpdate(), start_time);

  // Sleep 1 second
  Sleep(1)();
  EXPECT_TRUE(time_keeper_->Update());
  EXPECT_GE(time_keeper_->GetLastUpdate(), start_time + 1);

  // Stop
  time_keeper_->Stop();
  EXPECT_FALSE(time_keeper_->Update());

  // Destruct and reload
  time_keeper_.reset(MakeTimeKeeper(data_path_, "user.mt", 1));
  EXPECT_GE(time_keeper_->GetLastUpdate(), start_time + 1);
}

TEST_F(TimeKeeperTest, ClockSetBack) {
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  char buf[21];

  uint64_t time_past = std::time(0) + 60;
  int ret = std::snprintf(buf, sizeof(buf), "%" PRIu64, time_past);
  lsetxattr(data_path_, "user.mt", buf, ret, 0);
  time_keeper_.reset(MakeTimeKeeper(data_path_, "user.mt", 1));
  time_keeper_->Start();

  EXPECT_CALL(callback, Call(PLEVEL(warning, Server),
                             StartsWith("System clock has been set back"), _));
  EXPECT_TRUE(time_keeper_->Update());
  EXPECT_NE(0U, time_keeper_->GetLastUpdate());
}

}  // namespace
}  // namespace cpfs
