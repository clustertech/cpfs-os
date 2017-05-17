/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/inode_src_impl.hpp"

#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>

#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "common.hpp"
#include "mock_actions.hpp"
#include "store_util.hpp"
#include "server/ms/inode_src.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/inode_src_test_XXXXXX";

class InodeSrcTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  std::string data_path_;
  boost::scoped_ptr<IInodeSrc> inode_src_;

  InodeSrcTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        inode_src_(MakeInodeSrc(data_path_)) {
    // TODO(Joseph): WIP for #13801
    for (unsigned d_idx = 0; d_idx < kNumBaseDir; ++d_idx) {
      char buf[4];
      std::snprintf(buf, sizeof(buf), "%03x", d_idx);
      mkdir((std::string(data_path_) + "/" + std::string(buf)).c_str(), 0777);
    }
  }

  void UseInode(InodeNum inode) {
    std::ofstream((GetRoot() + "/" + GetInodeStr(inode)).c_str());
  }

  void RemoveInode(InodeNum inode) {
    unlink((GetRoot() + "/" + GetInodeStr(inode)).c_str());
  }

  // TODO(Joseph): WIP for #13801
  std::string GetRoot() {
    return std::string(data_path_) + "/000";
  }
};

TEST_F(InodeSrcTest, InitError) {
  // Errors during initialization
  EXPECT_EQ(0, symlink("10000000000000000", (GetRoot() + "/c").c_str()));
  EXPECT_THROW(inode_src_->Init(), std::runtime_error);
  EXPECT_EQ(0, unlink((GetRoot() + "/c").c_str()));
  EXPECT_EQ(0, symlink("hello", (GetRoot() + "/c").c_str()));
  EXPECT_THROW(inode_src_->Init(), std::runtime_error);
  EXPECT_EQ(0, unlink((GetRoot() + "/c").c_str()));
  EXPECT_EQ(0, rmdir(GetRoot().c_str()));
  EXPECT_THROW(inode_src_->Init(), std::runtime_error);
}

TEST_F(InodeSrcTest, API) {
  // Base usage and allocate API
  inode_src_->Init();
  EXPECT_EQ(1U, inode_src_->GetLastUsed()[0]);
  UseInode(2U);
  inode_src_->NotifyUsed(2U);
  EXPECT_EQ(2U, inode_src_->GetLastUsed()[0]);
  inode_src_->SetupAllocation();
  EXPECT_EQ(3U, inode_src_->Allocate(1, true));
  EXPECT_EQ(4U, inode_src_->Allocate(1, true));
  EXPECT_EQ(2U, inode_src_->GetLastUsed()[0]);
  inode_src_->NotifyUsed(4U);
  EXPECT_EQ(4U, inode_src_->GetLastUsed()[0]);
  inode_src_->NotifyUsed(3U);
  EXPECT_EQ(4U, inode_src_->GetLastUsed()[0]);
  std::vector<InodeNum> to_set;
  to_set.push_back(10U);
  inode_src_->SetLastUsed(to_set);
  EXPECT_EQ(10U, inode_src_->GetLastUsed()[0]);
  // TODO(Joseph): WIP for #13801. Dummy test at this moment
  inode_src_->Allocate(1, false);
}

TEST_F(InodeSrcTest, LazyPersist) {
  // After jumpy usage, get correct value
  inode_src_->Init();
  UseInode(4U);
  inode_src_->NotifyUsed(4U);
  UseInode(2U);
  inode_src_->NotifyUsed(2U);
  inode_src_.reset(MakeInodeSrc(data_path_));
  inode_src_->Init();
  inode_src_->SetupAllocation();
  inode_src_.reset(MakeInodeSrc(data_path_));
  inode_src_->Init();
  inode_src_->SetupAllocation();
  EXPECT_EQ(4U, inode_src_->GetLastUsed()[0]);

  // After long usage, get correct value
  for (uint64_t inode = 5; inode < 2100; ++inode) {
    UseInode(inode);
    inode_src_->NotifyUsed(inode);
  }
  inode_src_.reset(MakeInodeSrc(data_path_));
  inode_src_->Init();
  inode_src_->SetupAllocation();
  EXPECT_EQ(2099U, inode_src_->GetLastUsed()[0]);
}

TEST_F(InodeSrcTest, PersistRemoved) {
  // After removal, new inode source get correct last_used
  inode_src_->Init();
  UseInode(4U);
  inode_src_->NotifyUsed(4U);
  RemoveInode(3U);
  inode_src_->NotifyRemoved(3U);
  RemoveInode(2U);
  inode_src_->NotifyRemoved(2U);
  inode_src_.reset(MakeInodeSrc(data_path_));
  inode_src_->Init();
  inode_src_->SetupAllocation();
  EXPECT_EQ(4U, inode_src_->GetLastUsed()[0]);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
