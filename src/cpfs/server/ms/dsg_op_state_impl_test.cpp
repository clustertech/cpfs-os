/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/dsg_op_state_impl.hpp"

#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>

#include <gtest/gtest.h>

#include "common.hpp"
#include "op_completion_mock.hpp"
#include "server/ms/dsg_op_state.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

class DSGOpStateMgrTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<IDSGOpStateMgr> mgr_;
  MockIOpCompletionCheckerSet* checker_set_;

  DSGOpStateMgrTest() : mgr_(MakeDSGOpStateMgr()) {
    mgr_->set_completion_checker_set(
        checker_set_ = new MockIOpCompletionCheckerSet);
    EXPECT_EQ(mgr_->completion_checker_set(), checker_set_);
  }
};

TEST_F(DSGOpStateMgrTest, Resyncing) {
  std::vector<InodeNum> resyncing;
  resyncing.push_back(2);
  resyncing.push_back(3);
  mgr_->set_dsg_inodes_resyncing(1, resyncing);
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
