/* Copyright 2013 ClusterTech Ltd */
#include "client/inode_usage_impl.hpp"

#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gtest/gtest.h>

#include "common.hpp"
#include "mock_actions.hpp"
#include "client/inode_usage.hpp"

namespace cpfs {
namespace client {
namespace {

class InodeUsageSetTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<IInodeUsageSet> inode_usage_set_;

  InodeUsageSetTest()
      : inode_usage_set_(MakeInodeUsageSet()) {}
};

TEST_F(InodeUsageSetTest, OpenForRead) {
  {
    boost::scoped_ptr<IInodeUsageGuard> guard;
    inode_usage_set_->UpdateOpened(2, false, &guard);
    boost::scoped_ptr<IInodeUsageGuard> guard2;
    inode_usage_set_->UpdateOpened(3, false, &guard2);
  }
  inode_usage_set_->UpdateOpened(2, false);
  bool clean;
  {
    boost::scoped_ptr<IInodeUsageGuard> guard;
    EXPECT_EQ(0, inode_usage_set_->UpdateClosed(3, false, &clean, &guard));
    EXPECT_TRUE(clean);
    boost::scoped_ptr<IInodeUsageGuard> guard2;
    EXPECT_EQ(2, inode_usage_set_->UpdateClosed(2, false, &clean, &guard2));
    EXPECT_TRUE(clean);
  }
  EXPECT_EQ(0, inode_usage_set_->UpdateClosed(2, false, &clean));
  // Error case: Close an not yet opened inode
  EXPECT_THROW(inode_usage_set_->UpdateClosed(1000, false, &clean),
               std::runtime_error);
}

TEST_F(InodeUsageSetTest, OpenForWrite) {
  inode_usage_set_->UpdateOpened(2, true);
  inode_usage_set_->UpdateOpened(2, true);
  inode_usage_set_->UpdateOpened(3, true);
  inode_usage_set_->UpdateOpened(4, true);
  inode_usage_set_->UpdateOpened(4, true);
  inode_usage_set_->UpdateOpened(4, false);
  bool clean;
  EXPECT_EQ(0, inode_usage_set_->UpdateClosed(3, true, &clean));
  EXPECT_TRUE(clean);
  inode_usage_set_->SetDirty(2);
  EXPECT_EQ(kClientAccessUnchanged, inode_usage_set_->UpdateClosed(2, true,
                                                                   &clean));
  EXPECT_FALSE(clean);
  EXPECT_EQ(0, inode_usage_set_->UpdateClosed(2, true, &clean));
  EXPECT_FALSE(clean);
  EXPECT_EQ(kClientAccessUnchanged, inode_usage_set_->UpdateClosed(4, true,
                                                                   &clean));
  EXPECT_EQ(1, inode_usage_set_->UpdateClosed(4, true, &clean));
  EXPECT_TRUE(clean);
  EXPECT_EQ(0, inode_usage_set_->UpdateClosed(4, false, &clean));
  // Error case: Close for write when opened for read
  inode_usage_set_->UpdateOpened(5, false);
  EXPECT_THROW(inode_usage_set_->UpdateClosed(5, true, &clean),
               std::runtime_error);
}

void DoSetDirty(IInodeUsageSet* inode_usage_set, InodeNum inode, bool* flag) {
  inode_usage_set->SetDirty(inode);
  *flag = true;
}

TEST_F(InodeUsageSetTest, OpenForLockedSetattr) {
  {
    boost::scoped_ptr<IInodeUsageGuard> guard;
    EXPECT_TRUE(inode_usage_set_->StartLockedSetattr(1, &guard));
  }
  {
    inode_usage_set_->UpdateOpened(2, true);
    boost::scoped_ptr<IInodeUsageGuard> guard;
    EXPECT_TRUE(inode_usage_set_->StartLockedSetattr(2, &guard));
    EXPECT_TRUE(inode_usage_set_->StartLockedSetattr(2, 0));
    EXPECT_TRUE(guard);
    bool clean;
    EXPECT_EQ(0, inode_usage_set_->UpdateClosed(2, true, &clean));
    EXPECT_TRUE(clean);
    inode_usage_set_->StopLockedSetattr(2);
  }
  {
    inode_usage_set_->UpdateOpened(3, true);
    inode_usage_set_->SetDirty(3);
    boost::scoped_ptr<IInodeUsageGuard> guard;
    EXPECT_FALSE(inode_usage_set_->StartLockedSetattr(3, &guard));
    EXPECT_FALSE(guard);
    bool clean;
    EXPECT_EQ(0, inode_usage_set_->UpdateClosed(3, true, &clean));
    EXPECT_FALSE(clean);
  }
  {
    boost::scoped_ptr<IInodeUsageGuard> guard;
    EXPECT_TRUE(inode_usage_set_->StartLockedSetattr(4, &guard));
    ASSERT_TRUE(guard);
    inode_usage_set_->UpdateOpened(4, true);
    bool flag = false;
    boost::thread thr(boost::bind(DoSetDirty, inode_usage_set_.get(),
                                  4, &flag));
    Sleep(0.2)();
    EXPECT_FALSE(flag);
    guard.reset();
    Sleep(0.2)();
    EXPECT_TRUE(flag);
    bool clean;
    EXPECT_EQ(0, inode_usage_set_->UpdateClosed(4, true, &clean));
    EXPECT_FALSE(clean);
  }
}

}  // namespace
}  // namespace client
}  // namespace cpfs
