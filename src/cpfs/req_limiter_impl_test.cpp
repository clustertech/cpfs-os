/* Copyright 2013 ClusterTech Ltd */
#include "req_limiter_impl.hpp"

#include <stdint.h>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "req_entry_mock.hpp"
#include "req_limiter.hpp"
#include "req_tracker_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace {

class ReqLimiterTest : public ::testing::Test {
 protected:
  boost::shared_ptr<MockIReqTracker> req_tracker_;
  boost::scoped_ptr<IReqLimiter> req_limiter_;

  ReqLimiterTest()
      : req_tracker_(new MockIReqTracker),
        req_limiter_(MakeReqLimiter(req_tracker_, 1024 * 1024)) {}

 public:
  void DoSend(uint64_t size,
              boost::shared_ptr<MockIReqEntry> entry,
              ReqAckCallback* cb_ret,
              bool* completed_ret = 0) {
    FIM_PTR<IFim> req = WriteFim::MakePtr(size);
    EXPECT_CALL(*entry, request())
        .WillRepeatedly(Return(req));
    EXPECT_CALL(*req_tracker_, AddRequestEntry(
        boost::static_pointer_cast<IReqEntry>(entry), _))
        .WillOnce(Return(true));
    EXPECT_CALL(*entry, OnAck(_, false))
        .WillOnce(SaveArg<0>(cb_ret));

    EXPECT_TRUE(req_limiter_->Send(entry));
    if (completed_ret)
      *completed_ret = true;
  }
};

TEST_F(ReqLimiterTest, SendAck) {
  boost::shared_ptr<MockIReqEntry> entry1(new MockIReqEntry);
  ReqAckCallback cb1;
  DoSend(512 * 1024, entry1, &cb1);
  boost::shared_ptr<MockIReqEntry> entry2(new MockIReqEntry);
  ReqAckCallback cb2;
  DoSend(256 * 1024, entry2, &cb2);
  cb2(entry2);
  cb1(entry1);

  Mock::VerifyAndClear(entry1.get());
  Mock::VerifyAndClear(entry2.get());
  Mock::VerifyAndClear(req_tracker_.get());
}

TEST_F(ReqLimiterTest, SendWaitAck) {
  // Large entry block the way for next
  boost::shared_ptr<MockIReqEntry> entry1(new MockIReqEntry);
  ReqAckCallback cb1;
  DoSend(512 * 1024, entry1, &cb1);

  // Large enough to block, also block future sends
  boost::shared_ptr<MockIReqEntry> entry2(new MockIReqEntry);
  ReqAckCallback cb2;
  bool completed2 = false;
  boost::thread th2(boost::bind(&ReqLimiterTest::DoSend, this,
                                512 * 1024, entry2, &cb2, &completed2));
  Sleep(0.01)();

  // Even small Send will block now
  boost::shared_ptr<MockIReqEntry> entry3(new MockIReqEntry);
  ReqAckCallback cb3;
  bool completed3 = false;
  boost::thread th3(boost::bind(&ReqLimiterTest::DoSend, this,
                                256 * 1024, entry3, &cb3, &completed3));
  Sleep(0.01)();

  EXPECT_FALSE(completed2);
  EXPECT_FALSE(completed3);
  cb1(entry1);
  Sleep(0.05)();
  EXPECT_TRUE(completed2);
  EXPECT_TRUE(completed3);
  cb2(entry2);
  cb2(entry3);

  Mock::VerifyAndClear(entry1.get());
  Mock::VerifyAndClear(req_tracker_.get());
}

TEST_F(ReqLimiterTest, SendError) {
  // AddRequestEntry failed case
  boost::shared_ptr<MockIReqEntry> entry(new MockIReqEntry);
  FIM_PTR<IFim> req = WriteFim::MakePtr(1);
  EXPECT_CALL(*entry, request())
      .WillRepeatedly(Return(req));
  EXPECT_CALL(*req_tracker_, AddRequestEntry(
      boost::static_pointer_cast<IReqEntry>(entry), _))
      .WillOnce(Return(false));

  EXPECT_FALSE(req_limiter_->Send(entry));

  // Tracker already gone case
  req_tracker_.reset();
  EXPECT_FALSE(req_limiter_->Send(entry));
}

}  // namespace
}  // namespace cpfs
