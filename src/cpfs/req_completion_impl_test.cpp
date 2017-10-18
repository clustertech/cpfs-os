/* Copyright 2013 ClusterTech Ltd */
#include "req_completion_impl.hpp"

#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "req_completion.hpp"
#include "req_entry_mock.hpp"

namespace cpfs {
namespace {

using ::testing::_;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;

TEST(ReqCompletionTest, BasicRun) {
  // Create the completion checker, register a request entry
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  ReqAckCallback callback;
  boost::shared_ptr<MockIReqEntry> req_entry(new MockIReqEntry);
  {
    boost::shared_ptr<IReqCompletionChecker> checker =
        checker_set->Get(3);
    callback = checker_set->GetReqAckCallback(3, fim_socket);
    checker->RegisterReq(req_entry);
  }
  boost::weak_ptr<IReqCompletionChecker> w_checker(checker_set->Get(3));

  // Add a callback to be called when all completes
  MockFunction<void()> cb;
  EXPECT_CALL(cb, Call());

  checker_set->Get(3)->OnCompleteAll(boost::bind(GetMockCall(cb), &cb));
  EXPECT_FALSE(w_checker.expired());

  // Call the completion callback
  FIM_PTR<IFim> req(new GetattrFim);
  FIM_PTR<IFim> res;
  req->set_req_id(1000);
  EXPECT_CALL(*req_entry, req_id())
      .WillOnce(Return(1000));
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&res));

  callback(req_entry);
  EXPECT_TRUE(w_checker.expired());
  EXPECT_EQ(1000U, res->req_id());
  EXPECT_EQ(true, res->is_final());
  EXPECT_TRUE(dynamic_cast<FinalReplyFim*>(res.get()));

  // Also try creating a new standalone checker
  EXPECT_TRUE(boost::scoped_ptr<IReqCompletionChecker>(
      MakeReqCompletionChecker()));
}

TEST(ReqCompletionTest, NonePending) {
  MockFunction<void()> cb;
  EXPECT_CALL(cb, Call());

  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  checker_set->Get(3)->OnCompleteAll(boost::bind(GetMockCall(cb), &cb));
}

TEST(ReqCompletionTest, NoRequestFound) {
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  boost::shared_ptr<IReqCompletionChecker> checker =
      checker_set->Get(3);

  // Simple case, complete unregistered request, nothing should happen
  checker->CompleteReq(boost::make_shared<MockIReqEntry>());

  // Checker already cleaned
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  ReqAckCallback callback = checker_set->GetReqAckCallback(4, fim_socket);
  ReqAckCallback callback2 = checker_set->GetReqAckCallback(4, fim_socket);
  FIM_PTR<IFim> req(new GetattrFim);
  FIM_PTR<IFim> res;
  req->set_req_id(1000);
  boost::shared_ptr<MockIReqEntry> req_entry_1(new MockIReqEntry);
  boost::shared_ptr<MockIReqEntry> req_entry_2(new MockIReqEntry);
  EXPECT_CALL(*req_entry_1, req_id())
      .WillOnce(Return(1000));
  EXPECT_CALL(*req_entry_2, req_id())
      .WillOnce(Return(1000));
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .Times(2);
  callback(req_entry_1);
  callback2(req_entry_2);
}

TEST(ReqCompletionTest, OnComplete) {
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  ReqAckCallback callback1, callback2;

  // Before any entry is registered: immediately call the callback
  MockFunction<void()> cb;
  EXPECT_CALL(cb, Call());

  checker_set->OnCompleteAll(3, boost::bind(GetMockCall(cb), &cb));

  // Create two entries and retry
  boost::shared_ptr<MockIReqEntry> req_entry1(new MockIReqEntry);
  boost::shared_ptr<MockIReqEntry> req_entry2(new MockIReqEntry);
  {
    boost::shared_ptr<IReqCompletionChecker> checker = checker_set->Get(3);
    callback1 = checker_set->GetReqAckCallback(
        3, boost::shared_ptr<IFimSocket>());
    checker->RegisterReq(req_entry1);
    callback2 = checker_set->GetReqAckCallback(
        3, boost::shared_ptr<IFimSocket>());
    checker->RegisterReq(req_entry2);
  }
  checker_set->OnCompleteAll(3, boost::bind(GetMockCall(cb), &cb));
  callback1(req_entry1);
  Sleep(0.02)();

  // On completion, call the callback
  EXPECT_CALL(cb, Call());

  callback2(req_entry2);
}

TEST(ReqCompletionTest, OnCompleteAllGlobal) {
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  ReqAckCallback callback1, callback2;

  // Before any entry is registered: immediately call the callback
  MockFunction<void()> cb;
  EXPECT_CALL(cb, Call());
  checker_set->OnCompleteAllGlobal(boost::bind(GetMockCall(cb), &cb));

  // Create two entries and retry
  boost::shared_ptr<MockIReqEntry> req_entry1(new MockIReqEntry);
  boost::shared_ptr<MockIReqEntry> req_entry2(new MockIReqEntry);
  {
    boost::shared_ptr<IReqCompletionChecker> checker1 = checker_set->Get(3);
    callback1 = checker_set->GetReqAckCallback(
        3, boost::shared_ptr<IFimSocket>());
    checker1->RegisterReq(req_entry1);
    boost::shared_ptr<IReqCompletionChecker> checker2 = checker_set->Get(4);
    callback2 = checker_set->GetReqAckCallback(
        4, boost::shared_ptr<IFimSocket>());
    checker2->RegisterReq(req_entry2);
  }
  checker_set->OnCompleteAllGlobal(boost::bind(GetMockCall(cb), &cb));
  callback1(req_entry1);
  Sleep(0.02)();

  // On completion, call the callback
  EXPECT_CALL(cb, Call());

  callback2(req_entry2);
}

TEST(ReqCompletionTest, OnCompleteAllSubset) {
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  ReqAckCallback callback1, callback2, callback3;

  // Before any entry is registered: immediately call the callback
  MockFunction<void()> cb;
  EXPECT_CALL(cb, Call());

  std::vector<InodeNum> subset;
  subset.push_back(2);
  subset.push_back(3);
  subset.push_back(4);
  checker_set->OnCompleteAllSubset(subset, boost::bind(GetMockCall(cb), &cb));

  // Create two entries and retry
  boost::shared_ptr<MockIReqEntry> req_entry1(new MockIReqEntry);
  boost::shared_ptr<MockIReqEntry> req_entry2(new MockIReqEntry);
  boost::shared_ptr<MockIReqEntry> req_entry3(new MockIReqEntry);
  {
    boost::shared_ptr<IReqCompletionChecker> checker1 = checker_set->Get(3);
    callback1 = checker_set->GetReqAckCallback(
        3, boost::shared_ptr<IFimSocket>());
    checker1->RegisterReq(req_entry1);
    boost::shared_ptr<IReqCompletionChecker> checker2 = checker_set->Get(4);
    callback2 = checker_set->GetReqAckCallback(
        4, boost::shared_ptr<IFimSocket>());
    checker2->RegisterReq(req_entry2);
    boost::shared_ptr<IReqCompletionChecker> checker3 = checker_set->Get(5);
    callback3 = checker_set->GetReqAckCallback(
        5, boost::shared_ptr<IFimSocket>());
    checker3->RegisterReq(req_entry3);
  }
  checker_set->OnCompleteAllSubset(subset, boost::bind(GetMockCall(cb), &cb));
  callback1(req_entry1);
  Sleep(0.02)();

  // On completion, call the callback, even though inode 3 is not yet completed
  EXPECT_CALL(cb, Call());

  callback2(req_entry2);
}

}  // namespace
}  // namespace cpfs
