/* Copyright 2013 ClusterTech Ltd */
#include "server/inode_removal_tracker_impl.hpp"

#include <sys/stat.h>

#include <stdexcept>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "mock_actions.hpp"
#include "server/inode_removal_tracker.hpp"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;

namespace cpfs {
namespace server {
namespace {

const char* kDataPath = "/tmp/inode_removal_tracker_test_XXXXXX";

class InodeRemovalTrackerTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  boost::scoped_ptr<IInodeRemovalTracker> inode_removal_tracker_;

  InodeRemovalTrackerTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        inode_removal_tracker_(MakeInodeRemovalTracker(data_path_)) {}
};

TEST_F(InodeRemovalTrackerTest, RecordRemoved) {
  std::vector<InodeNum> inodes = inode_removal_tracker_->GetRemovedInodes();
  EXPECT_TRUE(inodes.empty());

  inode_removal_tracker_->SetPersistRemoved(false);
  inode_removal_tracker_->RecordRemoved(5);
  inode_removal_tracker_->RecordRemoved(100);

  inode_removal_tracker_->SetPersistRemoved(true);
  inode_removal_tracker_->RecordRemoved(200);
  inode_removal_tracker_->RecordRemoved(30);
  inodes = inode_removal_tracker_->GetRemovedInodes();
  EXPECT_EQ(4U, inodes.size());
  EXPECT_THAT(inodes, ElementsAre(5, 100, 200, 30));
  inode_removal_tracker_->SetPersistRemoved(false);
}

TEST_F(InodeRemovalTrackerTest, RecordError) {
  // Cannot create resync record directory
  chmod(data_path_, 0555);
  EXPECT_THROW(
      inode_removal_tracker_->SetPersistRemoved(true), std::runtime_error);

  // Cannot write resync record file
  chmod(data_path_, 0755);
  std::string rec_path = std::string(data_path_) + "/r";
  mkdir(rec_path.c_str(), 0555);
  EXPECT_THROW(
      inode_removal_tracker_->SetPersistRemoved(true), std::runtime_error);
}

TEST_F(InodeRemovalTrackerTest, ExpireRemoved) {
  inode_removal_tracker_->SetPersistRemoved(false);
  inode_removal_tracker_->RecordRemoved(5);
  inode_removal_tracker_->RecordRemoved(100);
  inode_removal_tracker_->SetPersistRemoved(true);
  inode_removal_tracker_->RecordRemoved(300);
  inode_removal_tracker_->ExpireRemoved(60);
  EXPECT_EQ(3U, inode_removal_tracker_->GetRemovedInodes().size());

  inode_removal_tracker_->SetPersistRemoved(false);
  inode_removal_tracker_->RecordRemoved(200);
  inode_removal_tracker_->ExpireRemoved(0);
  inode_removal_tracker_->SetPersistRemoved(true);
  EXPECT_EQ(0U, inode_removal_tracker_->GetRemovedInodes().size());
}

}  // namespace
}  // namespace server
}  // namespace cpfs
