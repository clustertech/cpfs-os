/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/cleaner_impl.hpp"

#include <ctime>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim_socket_mock.hpp"
#include "io_service_runner_mock.hpp"
#include "periodic_timer_mock.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ccache_tracker_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/cleaner.hpp"
#include "server/ms/replier_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/ugid_handler_mock.hpp"
#include "server/reply_set_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::SaveArg;
using ::testing::Return;

namespace cpfs {
namespace server {
namespace ms {
namespace {

TEST(MSCleanerTest, BaseTest) {
  // Construction
  MockBaseMetaServer server;
  MockIOServiceRunner* io_service_runner = new MockIOServiceRunner;
  server.set_io_service_runner(io_service_runner);
  MockICCacheTracker* cache_tracker = new MockICCacheTracker;
  server.set_cache_tracker(cache_tracker);
  MockITrackerMapper* tracker_mapper = new MockITrackerMapper;
  server.set_tracker_mapper(tracker_mapper);
  MockIReplySet* recent_reply_set = new MockIReplySet;
  server.set_recent_reply_set(recent_reply_set);
  MockIUgidHandler* ugid_handler = new MockIUgidHandler;
  server.set_ugid_handler(ugid_handler);
  MockIReplier* replier = new MockIReplier;
  server.set_replier(replier);
  MockIInodeRemovalTracker* inode_removal_tracker =
      new MockIInodeRemovalTracker;
  server.set_inode_removal_tracker(inode_removal_tracker);
  MockITimeKeeper* time_keeper = new MockITimeKeeper;
  server.set_peer_time_keeper(time_keeper);
  MockIStateMgr* state_mgr = new MockIStateMgr;
  server.set_state_mgr(state_mgr);
  MockIDurableRange* durable_range = new MockIDurableRange;
  server.set_durable_range(durable_range);

  MockPeriodicTimerMaker timer_maker;
  boost::shared_ptr<MockIPeriodicTimer> timer(new MockIPeriodicTimer);
  EXPECT_CALL(timer_maker, Make(_, kMSCleanerIterationTime))
      .WillOnce(Return(timer));
  boost::function<bool(void)> callback;
  EXPECT_CALL(*timer, OnTimeout(_))
      .WillOnce(SaveArg<0>(&callback));
  EXPECT_CALL(*io_service_runner, AddService(_));

  boost::scoped_ptr<ICleaner> cleaner(MakeCleaner(&server));
  cleaner->SetPeriodicTimerMaker(timer_maker.GetMaker());
  cleaner->Init();

  // Iteration
  EXPECT_CALL(*cache_tracker, ExpireCache(3600, _));
  boost::shared_ptr<MockIFimSocket> ms_fim_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper, GetMSFimSocket())
      .WillOnce(Return(ms_fim_socket));
  EXPECT_CALL(*recent_reply_set, ExpireReplies(kMSReplySetMaxAge));
  EXPECT_CALL(*ugid_handler, Clean());
  EXPECT_CALL(*replier, ExpireCallbacks(60));
  EXPECT_CALL(*inode_removal_tracker, ExpireRemoved(60));
  EXPECT_CALL(*time_keeper, GetLastUpdate())
      .WillOnce(Return(std::time(0) + 300));
  EXPECT_CALL(*state_mgr, GetState())
      .WillOnce(Return(kStateStandby));
  EXPECT_CALL(*durable_range, Latch());

  EXPECT_TRUE(callback());

  Mock::VerifyAndClear(&timer_maker);
  Mock::VerifyAndClear(tracker_mapper);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
