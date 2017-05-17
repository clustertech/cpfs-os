/* Copyright 2014 ClusterTech Ltd */
#include "client/shutdown_mgr_impl.hpp"

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "service_mock.hpp"
#include "shutdown_mgr.hpp"
#include "tracker_mapper_mock.hpp"
#include "client/base_client.hpp"
#include "client/cache_mock.hpp"

using ::testing::_;
using ::testing::Return;

namespace cpfs {
namespace client {
namespace {

TEST(ClientShutdownMgrTest, Shutdown) {
  IOService service;
  BaseFSClient client;
  MockIAsioPolicy asio_policy;
  MockITrackerMapper* tracker_mapper;
  MockICacheMgr* cache_mgr;
  MockIService* fso_runner;
  boost::scoped_ptr<IShutdownMgr> mgr;

  EXPECT_CALL(asio_policy, io_service())
      .WillRepeatedly(Return(&service));

  client.set_tracker_mapper(tracker_mapper = new MockITrackerMapper);
  mgr.reset(MakeShutdownMgr(&client));
  mgr->SetAsioPolicy(&asio_policy);
  client.set_cache_mgr(cache_mgr = new MockICacheMgr);
  client.set_runner(fso_runner = new MockIService);

  DeadlineTimer* timer = new DeadlineTimer(service);
  EXPECT_CALL(asio_policy, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(asio_policy, SetDeadlineTimer(timer, 10, _));
  EXPECT_CALL(*tracker_mapper, Plug(false));
  EXPECT_CALL(*cache_mgr, Shutdown());

  EXPECT_TRUE(mgr->Init(10));

  EXPECT_CALL(asio_policy, SetDeadlineTimer(timer, 0, _));
  EXPECT_CALL(*tracker_mapper, Shutdown());
  EXPECT_CALL(*fso_runner, Shutdown());

  EXPECT_TRUE(mgr->Shutdown());

  EXPECT_CALL(asio_policy, SetDeadlineTimer(timer, 0, _));
}

}  // namespace
}  // namespace client
}  // namespace cpfs
