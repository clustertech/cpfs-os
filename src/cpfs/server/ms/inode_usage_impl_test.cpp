/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/inode_usage_impl.hpp"

#include <utility>

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "server/ms/inode_usage.hpp"

using ::testing::Contains;
using ::testing::Not;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class MSInodeUsageTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<IInodeUsage> inode_usage_;

  MSInodeUsageTest() : inode_usage_(MakeInodeUsage()) {}
};

TEST_F(MSInodeUsageTest, MSOpenClose) {
  inode_usage_->SetFCOpened(1, 5, kInodeReadAccess);
  inode_usage_->SetFCOpened(1, 6, kInodeWriteAccess);
  inode_usage_->SetFCOpened(1, 10, kInodeWriteAccess);
  EXPECT_TRUE(inode_usage_->SetFCClosed(1, 6, true));
  EXPECT_TRUE(inode_usage_->SetFCClosed(1, 10, false));
  EXPECT_TRUE(inode_usage_->IsOpened(5));
  EXPECT_TRUE(inode_usage_->IsOpened(6));
  EXPECT_FALSE(inode_usage_->IsOpened(6, true));
  EXPECT_FALSE(inode_usage_->IsOpened(10));

  inode_usage_->SetFCOpened(2, 5);
  inode_usage_->SetFCOpened(2, 6);
  inode_usage_->SetFCOpened(2, 10);
  inode_usage_->SetFCOpened(2, 10);

  inode_usage_->SetFCOpened(1, 6, kInodeWriteAccess);
  EXPECT_TRUE(inode_usage_->IsOpened(6, true));

  ClientInodeMap inodes_opened = inode_usage_->client_opened();
  EXPECT_THAT(inodes_opened[1], Contains(std::make_pair(5, kInodeReadAccess)));
  EXPECT_THAT(inodes_opened[1], Contains(std::make_pair(6, kInodeWriteAccess)));
  EXPECT_THAT(inodes_opened[2], Contains(std::make_pair(5, kInodeReadAccess)));
  EXPECT_THAT(inodes_opened[2], Contains(std::make_pair(6, kInodeReadAccess)));
  EXPECT_THAT(inodes_opened[2], Contains(std::make_pair(10, kInodeReadAccess)));

  // Inode 10 is opened by FC 2
  EXPECT_TRUE(inode_usage_->IsOpened(10));

  boost::unordered_set<InodeNum> o1 = inode_usage_->GetFCOpened(1);
  EXPECT_EQ(2U, o1.size());
  EXPECT_TRUE(o1.find(5) != o1.end());
  EXPECT_TRUE(o1.find(6) != o1.end());
  EXPECT_TRUE(o1.find(10) == o1.end());  // Removed
  EXPECT_TRUE(o1.find(1000) == o1.end());  // Does not exist

  boost::unordered_set<InodeNum> o2 = inode_usage_->GetFCOpened(2);
  EXPECT_EQ(3U, o2.size());
  EXPECT_TRUE(o2.find(5) != o2.end());
  EXPECT_TRUE(o2.find(6) != o2.end());
  EXPECT_TRUE(o2.find(10) != o2.end());
  EXPECT_TRUE(o2.find(1000) == o2.end());  // Does not exist

  boost::unordered_set<InodeNum> o3 = inode_usage_->GetFCOpened(3);
  EXPECT_EQ(0U, o3.size());

  // Close Inode 10
  EXPECT_FALSE(inode_usage_->SetFCClosed(2, 10));
  EXPECT_FALSE(inode_usage_->IsOpened(10));

  // Check open of an not yet opened inode
  EXPECT_THAT(inode_usage_->GetFCOpened(1), Not(Contains(100)));
  EXPECT_THAT(inode_usage_->GetFCOpened(3), Not(Contains(100)));
  inode_usage_->SetFCOpened(1, 100);
  EXPECT_THAT(inode_usage_->GetFCOpened(1), Contains(100));

  // Close an not yet opened inode
  EXPECT_FALSE(inode_usage_->SetFCClosed(100, 1000));

  // Closing all opened by FC 1
  EXPECT_FALSE(inode_usage_->SetFCClosed(1, 5));
  EXPECT_TRUE(inode_usage_->SetFCClosed(1, 6));
  EXPECT_FALSE(inode_usage_->SetFCClosed(1, 100));

  // Swap
  inode_usage_->SetFCOpened(4, 3);
  inode_usage_->SetFCOpened(4, 70);
  InodeAccessMap client_inodes;
  client_inodes[1] = kInodeReadAccess;
  client_inodes[100] = kInodeWriteAccess;
  client_inodes[2000] = kInodeReadAccess;
  inode_usage_->SwapClientOpened(4, &client_inodes);
  EXPECT_THAT(inode_usage_->GetFCOpened(4), Contains(1));
  EXPECT_THAT(inode_usage_->GetFCOpened(4), Contains(100));
  EXPECT_THAT(inode_usage_->GetFCOpened(4), Contains(2000));
  EXPECT_TRUE(inode_usage_->IsOpened(1));
  EXPECT_FALSE(inode_usage_->IsOpened(1, true));
  EXPECT_TRUE(inode_usage_->IsOpened(100));
  EXPECT_TRUE(inode_usage_->IsOpened(100, true));
  EXPECT_TRUE(inode_usage_->IsOpened(2000));
  EXPECT_THAT(inode_usage_->GetFCOpened(4), Not(Contains(3)));
  EXPECT_THAT(inode_usage_->GetFCOpened(4), Not(Contains(70)));
}

TEST_F(MSInodeUsageTest, MSIsSoleWriter) {
  // Initial
  EXPECT_TRUE(inode_usage_->IsSoleWriter(1, 5));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(1, 6));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(2, 5));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(2, 6));

  // Read and write access from 1
  inode_usage_->SetFCOpened(1, 5, kInodeReadAccess);
  inode_usage_->SetFCOpened(1, 6, kInodeWriteAccess);

  EXPECT_TRUE(inode_usage_->IsSoleWriter(1, 5));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(1, 6));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(2, 5));
  EXPECT_FALSE(inode_usage_->IsSoleWriter(2, 6));

  // Write access from 2
  inode_usage_->SetFCOpened(2, 5, kInodeWriteAccess);
  inode_usage_->SetFCOpened(2, 6, kInodeWriteAccess);

  EXPECT_FALSE(inode_usage_->IsSoleWriter(1, 5));
  EXPECT_FALSE(inode_usage_->IsSoleWriter(1, 6));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(2, 5));
  EXPECT_FALSE(inode_usage_->IsSoleWriter(2, 6));

  // Release write access from 1
  inode_usage_->SetFCClosed(1, 5, false);
  inode_usage_->SetFCClosed(1, 6, true);

  EXPECT_FALSE(inode_usage_->IsSoleWriter(1, 5));
  EXPECT_FALSE(inode_usage_->IsSoleWriter(1, 6));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(2, 5));
  EXPECT_TRUE(inode_usage_->IsSoleWriter(2, 6));
}

TEST_F(MSInodeUsageTest, MSUnlink) {
  EXPECT_FALSE(inode_usage_->IsPendingUnlink(5));
  inode_usage_->AddPendingUnlink(5);
  EXPECT_TRUE(inode_usage_->IsPendingUnlink(5));
  inode_usage_->RemovePendingUnlink(5);
  EXPECT_FALSE(inode_usage_->IsPendingUnlink(5));

  inode_usage_->AddPendingUnlink(6);
  inode_usage_->AddPendingUnlink(6);
  EXPECT_TRUE(inode_usage_->IsPendingUnlink(6));
  inode_usage_->RemovePendingUnlink(6);
  EXPECT_FALSE(inode_usage_->IsPendingUnlink(6));

  inode_usage_->AddPendingUnlink(7);
  inode_usage_->AddPendingUnlink(8);
  boost::unordered_set<InodeNum> pending_inodes =
      inode_usage_->pending_unlink();

  EXPECT_EQ(2U, pending_inodes.size());
  EXPECT_TRUE(pending_inodes.find(7) != pending_inodes.end());
  EXPECT_TRUE(pending_inodes.find(8) != pending_inodes.end());

  // Clear and add multiple inodes
  boost::unordered_set<InodeNum> new_pending_inodes;
  new_pending_inodes.insert(2);
  new_pending_inodes.insert(50);
  new_pending_inodes.insert(300);
  inode_usage_->ClearPendingUnlink();
  inode_usage_->AddPendingUnlinks(new_pending_inodes);

  pending_inodes = inode_usage_->pending_unlink();
  ASSERT_EQ(3U, pending_inodes.size());
  EXPECT_TRUE(pending_inodes.find(2) != pending_inodes.end());
  EXPECT_TRUE(pending_inodes.find(50) != pending_inodes.end());
  EXPECT_TRUE(pending_inodes.find(300) != pending_inodes.end());
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
