/* Copyright 2013 ClusterTech Ltd */
#include "server/durable_range_impl.hpp"

#include <sys/stat.h>

#include <stdexcept>
#include <string>
#include <vector>

#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "mock_actions.hpp"
#include "server/durable_range.hpp"

using ::testing::ElementsAre;

namespace cpfs {
namespace server {
namespace {

const char* kDataPath = "/tmp/durable_range_test_XXXXXX";

class DurableRangeTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  std::string path_;
  boost::scoped_ptr<IDurableRange> dr_;

  DurableRangeTest()
      : data_path_mgr_(kDataPath),
        path_(std::string(data_path_mgr_.GetPath()) + "/drfile") {
    MakeNewRange();
  }

  void MakeNewRange() {
    dr_.reset(MakeDurableRange(path_.c_str(), 4));
  }
};

TEST_F(DurableRangeTest, BaseAPI) {
  // Empty load
  dr_->SetConservative(true);
  EXPECT_FALSE(dr_->Load());
  EXPECT_EQ(std::vector<InodeNum>(), dr_->Get());
  dr_->Add(1);
  dr_->Add(2);
  dr_->Add(17);
  dr_->Add(18);
  dr_->Add(60);
  EXPECT_THAT(dr_->Get(), ElementsAre(0, 16, 48));

  // Latch, add and get again
  dr_->Latch();
  dr_->Add(3);
  dr_->Add(61);
  dr_->Add(65);
  EXPECT_THAT(dr_->Get(), ElementsAre(0, 16, 48, 64));

  // Latch again, old ranges are removed
  dr_->Latch();
  EXPECT_THAT(dr_->Get(), ElementsAre(0, 48, 64));

  // Add some new ranges and reload
  dr_->Add(81);
  MakeNewRange();
  EXPECT_EQ(std::vector<InodeNum>(), dr_->Get());
  EXPECT_TRUE(dr_->Load());
  EXPECT_THAT(dr_->Get(), ElementsAre(0, 48, 64, 80));

  // Also try clear
  dr_->Clear();
  EXPECT_EQ(std::vector<InodeNum>(), dr_->Get());
  MakeNewRange();
  EXPECT_FALSE(dr_->Load());
  EXPECT_EQ(std::vector<InodeNum>(), dr_->Get());
}

TEST_F(DurableRangeTest, LargeFile) {
  std::vector<InodeNum> expected;
  // Generate a file of more than 512 entries without threading.  This
  // takes quite some time (7s in initial test)
  for (int i = 0; i <= 512 * 16; i += 16 * 4) {
    dr_->Add(i, i + 16, i + 32, i + 48);
    for (int j = 0; j < 4; ++j)
      expected.push_back(i + j * 16);
  }
  EXPECT_EQ(expected, dr_->Get());
  dr_->Latch();
  MakeNewRange();
  EXPECT_TRUE(dr_->Load());
  EXPECT_EQ(expected, dr_->Get());
}

void ThreadWork(IDurableRange* dr, int start, int end) {
  for (int i = start * 16; i <= end * 16; i += 16 * 4) {
    if (i == 64 * 16)
      dr->Latch();
    dr->Add(i, i + 16, i + 32, i + 48);
  }
}

TEST_F(DurableRangeTest, ThreadedAdd) {
  // Same as LargeFile, but do it concurrently in 4 threads to
  // exercise piggybacking.  Also add a Latch() call within
  // ThreadWork() to ensure correct interaction.
  std::vector<boost::shared_ptr<boost::thread> > threads;
  threads.push_back(
      boost::make_shared<boost::thread>(ThreadWork, dr_.get(), 0, 127));
  threads.push_back(
      boost::make_shared<boost::thread>(ThreadWork, dr_.get(), 128, 255));
  threads.push_back(
      boost::make_shared<boost::thread>(ThreadWork, dr_.get(), 256, 383));
  threads.push_back(
      boost::make_shared<boost::thread>(ThreadWork, dr_.get(), 384, 513));
  for (unsigned i = 0; i < threads.size(); ++i)
    threads[i]->join();
  std::vector<InodeNum> expected;
  for (int i = 0; i < (512 + 4) * 16; i += 16)
      expected.push_back(i);
  EXPECT_EQ(expected, dr_->Get());
  MakeNewRange();
  EXPECT_TRUE(dr_->Load());
  EXPECT_EQ(expected, dr_->Get());
  EXPECT_EQ(expected.size(), dr_->Get().size());
}

TEST_F(DurableRangeTest, Errors) {
  mkdir(path_.c_str(), 0777);
  // Test open(read) error
  EXPECT_THROW(dr_->Load(), std::runtime_error);
  // Test open(append) error
  EXPECT_THROW(dr_->Add(1), std::runtime_error);
  // Test open(write) error
  chmod(data_path_mgr_.GetPath(), 0);
  EXPECT_THROW(dr_->Latch(), std::runtime_error);
  // Test rename error
  chmod(data_path_mgr_.GetPath(), 0755);
  EXPECT_THROW(dr_->Latch(), std::runtime_error);
}

}  // namespace
}  // namespace server
}  // namespace cpfs
