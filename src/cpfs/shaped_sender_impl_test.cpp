/* Copyright 2013 ClusterTech Ltd */
#include "shaped_sender_impl.hpp"

#include <stdexcept>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim.hpp"
#include "fims.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace {

TEST(ShapedSender, NormalAPI) {
  // ctor
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  boost::scoped_ptr<IShapedSender> sender(kShapedSenderMaker(tracker, 2));

  // WaitAllReplied when empty
  sender->WaitAllReplied();

  // Add a couple of requests to make it full
  boost::shared_ptr<IReqEntry> entry1, entry2, entry3;
  EXPECT_CALL(*tracker, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry1),
                      Return(true)))
      .WillOnce(DoAll(SaveArg<0>(&entry2),
                      Return(true)));

  FIM_PTR<IFim> fim1 = DSResyncReqFim::MakePtr();
  sender->SendFim(fim1);
  EXPECT_EQ(fim1, entry1->request());
  FIM_PTR<IFim> fim2 = DSResyncReqFim::MakePtr();
  sender->SendFim(fim2);

  // Further requests need waiting
  FIM_PTR<ResultCodeReplyFim> reply1 = ResultCodeReplyFim::MakePtr();
  (*reply1)->err_no = 0;
  entry1->SetReply(reply1, 1);
  EXPECT_CALL(*tracker, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry3),
                      Return(true)));

  FIM_PTR<IFim> fim3 = DSResyncReqFim::MakePtr();
  sender->SendFim(fim3);

  // Can wait for the rest to complete
  entry2->SetReply(reply1, 1);
  entry3->SetReply(reply1, 1);

  sender->WaitAllReplied();
}

TEST(ShapedSender, ErrorAddEntry) {
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  boost::shared_ptr<IReqEntry> entry1;
  EXPECT_CALL(*tracker, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry1),
                      Return(false)));

  boost::scoped_ptr<IShapedSender> sender(kShapedSenderMaker(tracker, 2));
  FIM_PTR<IFim> fim1 = DSResyncReqFim::MakePtr();
  EXPECT_THROW(sender->SendFim(fim1), std::runtime_error);
}

TEST(ShapedSender, ErrorWaitReply) {
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  boost::shared_ptr<IReqEntry> entry1, entry2;
  EXPECT_CALL(*tracker, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry1),
                      Return(true)));

  boost::scoped_ptr<IShapedSender> sender(kShapedSenderMaker(tracker, 2));
  FIM_PTR<IFim> fim1 = DSResyncReqFim::MakePtr();
  sender->SendFim(fim1);
  entry1->set_reply_error(true);
  EXPECT_THROW(sender->WaitAllReplied(), std::runtime_error);
}

TEST(ShapedSender, ErrorReplyType) {
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  boost::shared_ptr<IReqEntry> entry1;
  EXPECT_CALL(*tracker, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry1),
                      Return(true)));

  boost::scoped_ptr<IShapedSender> sender(kShapedSenderMaker(tracker, 2));
  FIM_PTR<IFim> fim1 = DSResyncReqFim::MakePtr();
  sender->SendFim(fim1);
  entry1->SetReply(DataReplyFim::MakePtr(), 0);
  EXPECT_THROW(sender->WaitAllReplied(), std::runtime_error);
}

TEST(ShapedSender, ErrorInReply) {
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  boost::shared_ptr<IReqEntry> entry1;
  EXPECT_CALL(*tracker, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry1),
                      Return(true)));

  boost::scoped_ptr<IShapedSender> sender(kShapedSenderMaker(tracker, 2));
  FIM_PTR<IFim> fim1 = DSResyncReqFim::MakePtr();
  sender->SendFim(fim1);
  FIM_PTR<ResultCodeReplyFim> reply = ResultCodeReplyFim::MakePtr();
  (*reply)->err_no = 1;
  entry1->SetReply(reply, 0);
  EXPECT_THROW(sender->WaitAllReplied(), std::runtime_error);
}

}  // namespace
}  // namespace cpfs
