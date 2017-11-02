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
#include "mock_actions.hpp"
#include "req_completion.hpp"

namespace cpfs {
namespace {

using ::testing::_;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;

TEST(ReqCompletionTest, CheckerBasic) {
  boost::scoped_ptr<IReqCompletionChecker> checker(MakeReqCompletionChecker());
  // Call the completion callback once completed
  MockFunction<void()> cb;
  int op;
  checker->RegisterOp(&op);
  checker->OnCompleteAll(boost::bind(GetMockCall(cb), &cb));

  EXPECT_CALL(cb, Call());

  checker->CompleteOp(&op);

  // Will immediately call if nothing is pending
  EXPECT_CALL(cb, Call());

  checker->OnCompleteAll(boost::bind(GetMockCall(cb), &cb));
}

TEST(ReqCompletionTest, CheckerInterleave) {
  boost::scoped_ptr<IReqCompletionChecker> checker(MakeReqCompletionChecker());
  int op, op2;
  checker->RegisterOp(&op);
  MockFunction<void()> cb;
  checker->OnCompleteAll(boost::bind(GetMockCall(cb), &cb));
  checker->RegisterOp(&op2);
  MockFunction<void()> cb2;
  checker->OnCompleteAll(boost::bind(GetMockCall(cb2), &cb2));

  // First completion
  EXPECT_CALL(cb, Call());

  checker->CompleteOp(&op);
  checker->CompleteOp(&op);  // No effect if completed again
  Mock::VerifyAndClear(&cb);

  // Second completion
  EXPECT_CALL(cb2, Call());

  checker->CompleteOp(&op2);
}

TEST(ReqCompletionTest, CheckerSet) {
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  // Call the completion callback once completed
  int op;
  checker_set->Get(3)->RegisterOp(&op);
  boost::weak_ptr<IReqCompletionChecker> w_checker(checker_set->Get(3));
  MockFunction<void()> cb;
  checker_set->OnCompleteAll(3, boost::bind(GetMockCall(cb), &cb));
  EXPECT_FALSE(w_checker.expired());

  EXPECT_CALL(cb, Call());

  checker_set->CompleteOp(3, &op);
  EXPECT_TRUE(w_checker.expired());

  // Will immediately call if nothing is pending
  EXPECT_CALL(cb, Call());

  checker_set->OnCompleteAll(3, boost::bind(GetMockCall(cb), &cb));
}

TEST(ReqCompletionTest, OnCompleteAllGlobal) {
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  MockFunction<void()> cb;
  EXPECT_CALL(cb, Call());

  checker_set->OnCompleteAllGlobal(boost::bind(GetMockCall(cb), &cb));

  int op, op2;
  checker_set->Get(3)->RegisterOp(&op);
  checker_set->Get(4)->RegisterOp(&op2);
  checker_set->OnCompleteAllGlobal(boost::bind(GetMockCall(cb), &cb));

  checker_set->CompleteOp(3, &op);
  checker_set->CompleteOp(3, &op);

  EXPECT_CALL(cb, Call());

  checker_set->CompleteOp(4, &op2);
}

TEST(ReqCompletionTest, OnCompleteAllSubset) {
  boost::scoped_ptr<IReqCompletionCheckerSet> checker_set(
      MakeReqCompletionCheckerSet());
  // Before any entry is registered: immediately call the callback
  MockFunction<void()> cb;
  EXPECT_CALL(cb, Call());

  std::vector<InodeNum> subset;
  subset.push_back(2);
  subset.push_back(3);
  subset.push_back(4);
  checker_set->OnCompleteAllSubset(subset, boost::bind(GetMockCall(cb), &cb));

  int op1, op2, op3;
  checker_set->Get(3)->RegisterOp(&op1);
  checker_set->Get(4)->RegisterOp(&op2);
  checker_set->Get(5)->RegisterOp(&op3);
  checker_set->OnCompleteAllSubset(subset, boost::bind(GetMockCall(cb), &cb));

  checker_set->CompleteOp(3, &op1);

  EXPECT_CALL(cb, Call());

  checker_set->CompleteOp(4, &op2);
}

}  // namespace
}  // namespace cpfs
