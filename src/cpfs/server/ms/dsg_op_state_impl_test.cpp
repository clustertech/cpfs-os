/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/dsg_op_state_impl.hpp"

#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "common.hpp"
#include "mock_actions.hpp"
#include "op_completion_mock.hpp"
#include "server/ms/dsg_op_state.hpp"

using ::testing::_;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class DSGOpStateMgrTest : public ::testing::Test {
 protected:
  MockIOpCompletionCheckerSet* checker_set_;
  boost::shared_ptr<MockIOpCompletionChecker> checker_;
  boost::scoped_ptr<IDSGOpStateMgr> mgr_;

  DSGOpStateMgrTest() : checker_(new MockIOpCompletionChecker) {
    mgr_.reset(
        MakeDSGOpStateMgr(checker_set_ = new MockIOpCompletionCheckerSet));
    EXPECT_CALL(*checker_set_, Get(_))
        .WillRepeatedly(Return(checker_));
  }
};

TEST_F(DSGOpStateMgrTest, InodeOp) {
  int op;
  EXPECT_CALL(*checker_, RegisterOp(&op));

  mgr_->RegisterInodeOp(42, &op);

  EXPECT_CALL(*checker_set_, CompleteOp(42, &op));

  mgr_->CompleteInodeOp(42, &op);
}

TEST_F(DSGOpStateMgrTest, Completion) {
  MockFunction<void()> cb;
  std::vector<InodeNum> inodes;
  inodes.push_back(42);
  inodes.push_back(43);
  OpCompletionCallback callback;
  EXPECT_CALL(*checker_set_, OnCompleteAllSubset(inodes, _))
      .WillOnce(SaveArg<1>(&callback));

  mgr_->OnInodesCompleteOp(inodes, boost::bind(GetMockCall(cb), &cb));

  EXPECT_CALL(cb, Call());

  callback();
}

TEST_F(DSGOpStateMgrTest, Resyncing) {
  std::vector<InodeNum> resyncing;
  resyncing.push_back(2);
  resyncing.push_back(3);
  mgr_->SetDsgInodesResyncing(1, resyncing);
  {
    boost::shared_lock<boost::shared_mutex> lock;
    mgr_->ReadLock(1, &lock);
    EXPECT_TRUE(mgr_->is_dsg_inode_resyncing(1, 2));
    EXPECT_FALSE(mgr_->is_dsg_inode_resyncing(1, 4));
  }
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
