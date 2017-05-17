/* Copyright 2013 ClusterTech Ltd */
#include "server/ds/cleaner_impl.hpp"

#include <ctime>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "dsg_state.hpp"
#include "finfo.hpp"  // IWYU pragma: keep (need by gtest over store.hpp)
#include "io_service_runner_mock.hpp"
#include "periodic_timer_mock.hpp"
#include "time_keeper_mock.hpp"
#include "server/ds/base_ds_mock.hpp"
#include "server/ds/cleaner.hpp"
#include "server/ds/store_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::SaveArg;
using ::testing::Return;

namespace cpfs {
namespace server {
namespace ds {
namespace {

TEST(DSCleanerTest, BaseTest) {
  // Construction
  MockBaseDataServer server;
  MockIOServiceRunner* io_service_runner;
  server.set_io_service_runner(io_service_runner = new MockIOServiceRunner);
  MockIStore* store;
  server.set_store(store = new MockIStore);
  MockPeriodicTimerMaker timer_maker;
  boost::shared_ptr<MockIPeriodicTimer> timer(new MockIPeriodicTimer);
  EXPECT_CALL(timer_maker, Make(_, kDSCleanerIterationTime))
      .WillOnce(Return(timer));
  boost::function<bool(void)> callback;
  EXPECT_CALL(*timer, OnTimeout(_))
      .WillOnce(SaveArg<0>(&callback));
  EXPECT_CALL(*io_service_runner, AddService(_));
  MockITimeKeeper* time_keeper;
  server.set_dsg_ready_time_keeper(time_keeper = new MockITimeKeeper);
  MockIDurableRange* durable_range;
  server.set_durable_range(durable_range = new MockIDurableRange);
  server.set_dsg_state(1, kDSGReady, 0);
  MockIInodeRemovalTracker* inode_removal_tracker;
  server.set_inode_removal_tracker(inode_removal_tracker
                                   = new MockIInodeRemovalTracker);

  boost::scoped_ptr<ICleaner> cleaner(MakeCleaner(&server));
  cleaner->SetPeriodicTimerMaker(timer_maker.GetMaker());
  cleaner->SetMinInodeRemoveTime(0);
  cleaner->Init();
  cleaner->RemoveInodeLater(123);

  // Iteration
  EXPECT_CALL(*time_keeper, GetLastUpdate())
      .WillOnce(Return(std::time(0) + kReplAndIODelay + 5));
  EXPECT_CALL(*store, FreeData(123, true));
  EXPECT_CALL(*durable_range, Latch());
  EXPECT_CALL(*inode_removal_tracker, ExpireRemoved(60));

  EXPECT_TRUE(callback());

  // Shutdown
  EXPECT_CALL(*store, FreeData(456, true));

  cleaner->SetMinInodeRemoveTime(1);
  cleaner->RemoveInodeLater(456);
  cleaner->Shutdown();

  Mock::VerifyAndClear(&timer_maker);
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
