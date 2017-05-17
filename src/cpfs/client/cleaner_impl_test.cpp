/* Copyright 2013 ClusterTech Ltd */
#include "client/cleaner_impl.hpp"

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "io_service_runner_mock.hpp"
#include "periodic_timer_mock.hpp"
#include "client/base_client.hpp"
#include "client/cache_mock.hpp"
#include "client/cleaner.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace client {
namespace {

TEST(ClientCleanerTest, BaseTest) {
  // Construction
  BaseFSClient client;
  MockIOServiceRunner* io_service_runner = new MockIOServiceRunner;
  client.set_service_runner(io_service_runner);
  EXPECT_CALL(*io_service_runner, AddService(_));
  MockICacheMgr* cache_mgr = new MockICacheMgr;
  client.set_cache_mgr(cache_mgr);
  MockPeriodicTimerMaker timer_maker;
  boost::shared_ptr<MockIPeriodicTimer> timer(new MockIPeriodicTimer);
  EXPECT_CALL(timer_maker, Make(_, 60.0))
      .WillOnce(Return(timer));
  boost::function<bool(void)> callback;
  EXPECT_CALL(*timer, OnTimeout(_))
      .WillOnce(SaveArg<0>(&callback));

  boost::scoped_ptr<ICleaner> client_cleaner(MakeCleaner(&client));
  client_cleaner->SetPeriodicTimerMaker(timer_maker.GetMaker());
  client_cleaner->Init();

  // Iteration
  EXPECT_CALL(*cache_mgr, CleanMgr(3600));

  EXPECT_TRUE(callback());

  Mock::VerifyAndClear(&timer_maker);
}

}  // namespace
}  // namespace client
}  // namespace cpfs
