/* Copyright 2013 ClusterTech Ltd */

#include "server/ds/degrade_impl.hpp"

#include <cstring>
#include <stdexcept>
#include <vector>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "server/ds/degrade.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

class DegradeTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<IDegradedCache> cache_;
  boost::scoped_ptr<IDataRecoveryMgr> recovery_mgr_;

  DegradeTest() : cache_(MakeDegradedCache(4)),
                  recovery_mgr_(MakeDataRecoveryMgr()) {}
};

TEST_F(DegradeTest, DegradedCacheBasic) {
  EXPECT_FALSE(cache_->IsActive());
  EXPECT_FALSE(cache_->GetHandle(103, 0));
  cache_->FreeInode(103);  // Won't die
  cache_->SetActive(true);
  EXPECT_TRUE(cache_->IsActive());
  char* data;
  {
    boost::shared_ptr<IDegradedCacheHandle> handle = cache_->GetHandle(103, 0);
    EXPECT_FALSE(handle->initialized());
    handle->Initialize();
    EXPECT_TRUE(handle->initialized());
    EXPECT_FALSE(handle->data());
    char buf[10];
    handle->Read(2, buf, 10);
    for (unsigned i = 0; i < 10; ++i)
      EXPECT_EQ('\0', buf[i]);
    handle->Allocate();
    data = handle->data();
    EXPECT_TRUE(data);
    for (unsigned i = 0; i < kSegmentSize; ++i)
      data[i] = char(i);
    handle->Read(2, buf, 10);
    for (unsigned i = 0; i < 10; ++i)
      EXPECT_EQ(char(i + 2), buf[i]);
  }
  EXPECT_EQ(data, cache_->GetHandle(103, 0)->data());
  cache_->FreeInode(103);
  EXPECT_FALSE(cache_->GetHandle(103, 0)->initialized());
  cache_->SetActive(false);
  EXPECT_FALSE(cache_->GetHandle(103, 0));
  EXPECT_FALSE(cache_->IsActive());
}

TEST_F(DegradeTest, DegradedCacheTruncate) {
  // Won't fail if called when inactive
  cache_->Truncate(103, 9 * kSegmentSize + 1);

  // Initialize
  cache_->SetActive(true);
  cache_->GetHandle(103, kSegmentSize)->Allocate();
  cache_->GetHandle(103, 3 * kSegmentSize)->Allocate();
  cache_->GetHandle(103, 5 * kSegmentSize)->Initialize();
  cache_->GetHandle(103, 7 * kSegmentSize)->Initialize();

  // Truncate after end
  cache_->Truncate(103, 9 * kSegmentSize + 1);
  EXPECT_TRUE(cache_->GetHandle(103, 7 * kSegmentSize)->initialized());

  // Truncate at odd size without data
  cache_->Truncate(103, 5 * kSegmentSize + 1);
  EXPECT_TRUE(cache_->GetHandle(103, 5 * kSegmentSize));
  EXPECT_TRUE(cache_->GetHandle(103, 5 * kSegmentSize)->initialized());
  EXPECT_FALSE(cache_->GetHandle(103, 7 * kSegmentSize)->initialized());

  // Truncate at boundary
  cache_->Truncate(103, 5 * kSegmentSize);
  EXPECT_FALSE(cache_->GetHandle(103, 5 * kSegmentSize)->initialized());
  EXPECT_FALSE(cache_->GetHandle(103, 7 * kSegmentSize)->initialized());

  // Truncate at odd size with data
  char* data = cache_->GetHandle(103, 3 * kSegmentSize)->data();
  cache_->Truncate(103, 3 * kSegmentSize + 4);
  ASSERT_EQ(data, cache_->GetHandle(103, 3 * kSegmentSize)->data());
  EXPECT_FALSE(cache_->GetHandle(103, 5 * kSegmentSize)->initialized());

  // Effect on other inodes
  cache_->GetHandle(104, 3 * kSegmentSize)->Allocate();
  cache_->Truncate(103, 3 * kSegmentSize);
  EXPECT_FALSE(cache_->GetHandle(103, 3 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(104, 3 * kSegmentSize)->initialized());
}

TEST_F(DegradeTest, DegradedCacheEviction) {
  cache_->SetActive(true);
  cache_->GetHandle(103, kSegmentSize)->Initialize();
  cache_->GetHandle(103, 3 * kSegmentSize)->Allocate();
  cache_->GetHandle(103, 5 * kSegmentSize)->Allocate();
  cache_->GetHandle(103, 7 * kSegmentSize)->Allocate();
  cache_->GetHandle(103, 9 * kSegmentSize)->Allocate();
  EXPECT_TRUE(cache_->GetHandle(103, kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 3 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 5 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 7 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 9 * kSegmentSize)->initialized());
  boost::shared_ptr<IDegradedCacheHandle> handle1 =
      cache_->GetHandle(104, kSegmentSize);
  char cs_buf[3];
  handle1->Write(0, "abc", 3, cs_buf);  // Also allocate
  EXPECT_EQ(0, std::memcmp("abc", cs_buf, 3));
  {
    // Recursion
    EXPECT_EQ(handle1, cache_->GetHandle(104, kSegmentSize));
  }
  handle1->RevertWrite(0, cs_buf, 3);
  EXPECT_FALSE(cache_->GetHandle(103, kSegmentSize)->initialized());
  EXPECT_FALSE(cache_->GetHandle(103, 3 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 5 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 7 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 9 * kSegmentSize)->initialized());
  // Try allocate another handle.  Test whether recursion works: if it
  // doesn't, the handle1 might seem to have been released and oldest
  // and is evicted.  The correct behavior is to treat it as still in
  // use, and evict 103 - 5 instead.
  cache_->GetHandle(104, 3 * kSegmentSize)->Allocate();
  EXPECT_FALSE(cache_->GetHandle(103, kSegmentSize)->initialized());
  EXPECT_FALSE(cache_->GetHandle(103, 3 * kSegmentSize)->initialized());
  EXPECT_FALSE(cache_->GetHandle(103, 5 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 7 * kSegmentSize)->initialized());
  EXPECT_TRUE(cache_->GetHandle(103, 9 * kSegmentSize)->initialized());
  boost::shared_ptr<IDegradedCacheHandle> handle2 =
      cache_->GetHandle(104, 3 * kSegmentSize);
  boost::shared_ptr<IDegradedCacheHandle> handle3 =
      cache_->GetHandle(103, 7 * kSegmentSize);
  boost::shared_ptr<IDegradedCacheHandle> handle4 =
      cache_->GetHandle(103, 9 * kSegmentSize);
  EXPECT_THROW(cache_->GetHandle(103, kSegmentSize)->Allocate(),
               std::runtime_error);
}

TEST_F(DegradeTest, DegradedCacheDeactivate) {
  // Simple deactivation
  cache_->SetActive(true);
  cache_->SetActive(false);

  // Repeated, no effect
  cache_->SetActive(false);

  cache_->SetActive(true);
  boost::shared_ptr<IDegradedCacheHandle> handle = cache_->GetHandle(103, 0);
  boost::thread deactivate_thread(
      boost::bind(&IDegradedCache::SetActive, cache_.get(), false));

  // Won't deactivate now
  Sleep(0.1)();
  EXPECT_EQ(handle, cache_->GetHandle(103, 0));

  // Deactivate once handle is removed
  handle.reset();
  Sleep(0.1)();
  EXPECT_FALSE(cache_->GetHandle(103, 0));
}

TEST_F(DegradeTest, DataRecoveryGetHandler) {
  EXPECT_FALSE(recovery_mgr_->GetHandle(100, 5, false));
  {
    boost::shared_ptr<IDataRecoveryHandle> handle =
        recovery_mgr_->GetHandle(100, 5, true);
    EXPECT_FALSE(handle->SetStarted());
    EXPECT_TRUE(handle->SetStarted());
    EXPECT_FALSE(recovery_mgr_->GetHandle(100, 6, false));
    EXPECT_FALSE(recovery_mgr_->GetHandle(101, 5, false));
  }
  EXPECT_TRUE(recovery_mgr_->GetHandle(100, 5, false)->SetStarted());
  recovery_mgr_->DropHandle(100, 5);
  EXPECT_FALSE(recovery_mgr_->GetHandle(100, 5, false));
}

TEST_F(DegradeTest, DataRecoveryQueue) {
  boost::shared_ptr<IDataRecoveryHandle> handle =
      recovery_mgr_->GetHandle(100, 5, true);
  boost::shared_ptr<IFimSocket> socket_ret;
  EXPECT_FALSE(handle->UnqueueFim(&socket_ret));
  FIM_PTR<DataReplyFim> fim1(new DataReplyFim);
  boost::shared_ptr<IFimSocket> socket1(new MockIFimSocket);
  handle->QueueFim(fim1, socket1);
  FIM_PTR<DataReplyFim> fim2(new DataReplyFim);
  boost::shared_ptr<IFimSocket> socket2(new MockIFimSocket);
  handle->QueueFim(fim2, socket2);
  EXPECT_EQ(fim1, handle->UnqueueFim(&socket_ret));
  EXPECT_EQ(socket1, socket_ret);
  EXPECT_EQ(fim2, handle->UnqueueFim(&socket_ret));
  EXPECT_EQ(socket2, socket_ret);
  EXPECT_FALSE(handle->UnqueueFim(&socket_ret));
}

TEST_F(DegradeTest, DataRecoveryData) {
  boost::shared_ptr<IDataRecoveryHandle> handle =
      recovery_mgr_->GetHandle(100, 5, true);
  std::vector<boost::shared_ptr<IFimSocket> > sockets;
  for (unsigned i = 0; i < kNumDSPerGroup - 2; ++i)
    sockets.push_back(boost::shared_ptr<IFimSocket>(new MockIFimSocket));
  char data[kSegmentSize];
  for (unsigned i = 0; i < kSegmentSize; ++i)
    data[i] = char(i % 256);
  for (unsigned i = 0; i < kNumDSPerGroup - 3; ++i)
    EXPECT_FALSE(handle->AddData(data, sockets[i]));
  EXPECT_TRUE(handle->DataSent(sockets[0]));
  EXPECT_FALSE(handle->DataSent(sockets[kNumDSPerGroup - 3]));
  EXPECT_TRUE(handle->AddData(0, sockets[kNumDSPerGroup - 3]));
  EXPECT_TRUE(handle->DataSent(sockets[0]));
  EXPECT_TRUE(handle->DataSent(sockets[kNumDSPerGroup - 3]));
  EXPECT_EQ(bool(kNumDSPerGroup % 2), handle->RecoverData(data));
  char data2[kSegmentSize];
  for (unsigned i = 0; i < kSegmentSize; ++i)
    data2[i] = 0;
  EXPECT_EQ(!(kNumDSPerGroup % 2), handle->RecoverData(data2));
  for (unsigned i = 0; i < kSegmentSize; ++i) {
    EXPECT_TRUE(data[i] == 0 || data2[i] == 0);
    EXPECT_TRUE(data[i] == char(i % 256) || data2[i] == char(i % 256));
  }
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
