/* Copyright 2014 ClusterTech Ltd */
#include "server/ds/shutdown_mgr_impl.hpp"

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "io_service_runner_mock.hpp"
#include "shutdown_mgr.hpp"
#include "server/ds/base_ds_mock.hpp"
#include "server/ds/cleaner_mock.hpp"
#include "server/thread_group_mock.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

using ::testing::_;
using ::testing::Return;

class DSShutdownMgrTest : public ::testing::Test {
 protected:
  IOService service_;
  MockIAsioPolicy asio_policy_;
  MockBaseDataServer server_;
  MockIOServiceRunner* io_service_runner_;
  MockICleaner* cleaner_;
  MockIThreadGroup* thread_group_;
  boost::scoped_ptr<IShutdownMgr> mgr_;

  DSShutdownMgrTest() {
    server_.set_io_service_runner(io_service_runner_ = new MockIOServiceRunner);
    mgr_.reset(MakeShutdownMgr(&server_));
    EXPECT_CALL(asio_policy_, io_service())
        .WillRepeatedly(Return(&service_));
    mgr_->SetAsioPolicy(&asio_policy_);
    server_.set_cleaner(cleaner_ = new MockICleaner);
    server_.set_thread_group(thread_group_ = new MockIThreadGroup);
  }
};

TEST_F(DSShutdownMgrTest, Shutdown) {
  DeadlineTimer* timer = new DeadlineTimer(service_);
  EXPECT_CALL(asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(asio_policy_, SetDeadlineTimer(timer, 10, _));

  EXPECT_TRUE(mgr_->Init(10));

  // Timeout occurs
  EXPECT_CALL(asio_policy_, SetDeadlineTimer(timer, 0, _));
  EXPECT_CALL(*io_service_runner_, Stop());
  EXPECT_CALL(*thread_group_, Stop(-1));
  EXPECT_CALL(*cleaner_, Shutdown());

  EXPECT_TRUE(mgr_->Shutdown());

  EXPECT_CALL(asio_policy_, SetDeadlineTimer(timer, 0, _));
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
