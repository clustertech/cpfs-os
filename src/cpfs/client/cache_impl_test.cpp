/* Copyright 2013 ClusterTech Ltd */
#include "client/cache_impl.hpp"

#include <fuse/fuse_lowlevel.h>  // IWYU pragma: keep

#include <sys/types.h>  // IWYU pragma: keep

#include <cstddef>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/lock_guard.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "fuseobj_mock.hpp"
#include "mock_actions.hpp"
#include "mutex_util.hpp"
#include "client/cache.hpp"

using ::testing::AtMost;
using ::testing::DoAll;
using ::testing::InvokeWithoutArgs;
using ::testing::Mock;
using ::testing::Return;
using ::testing::StartsWith;
using ::testing::Throw;

namespace cpfs {
namespace client {

// Explicit instantiate class template to better detect missing coverage
template
class CacheInvPolicy<MockFuseMethodPolicy>;

namespace {

class ClientCacheMgrTest : public ::testing::Test {
 public:
  ClientCacheMgrTest()
      : chan_(0),
        fuse_methods_(new MockFuseMethodPolicy),
        cache_mgr_(MakeCacheMgr(
            chan_,
            new CacheInvPolicy<MockFuseMethodPolicy>(fuse_methods_.get()))) {
    cache_mgr_->Init();
  }
  ~ClientCacheMgrTest() {
    cache_mgr_->Shutdown();
  }
 protected:
  fuse_chan* chan_;
  boost::shared_ptr<MockFuseMethodPolicy> fuse_methods_;
  boost::scoped_ptr<ICacheMgr> cache_mgr_;
};

// TODO(Joseph): This dependency will be removed
TEST_F(ClientCacheMgrTest, NullCachePolicy) {
  boost::scoped_ptr<NullCachePolicy> policy(new NullCachePolicy);
  EXPECT_EQ(0, policy->NotifyInvalInode(NULL, 0, 0, 0));
  EXPECT_EQ(0, policy->NotifyInvalEntry(NULL, 0, 0, 0));
}

TEST_F(ClientCacheMgrTest, SetFuseChannel) {
  cache_mgr_->Shutdown();
  cache_mgr_.reset(
      MakeCacheMgr(
          chan_,
          new CacheInvPolicy<MockFuseMethodPolicy>(fuse_methods_.get())));
  cache_mgr_->SetFuseChannel(0);
  // No Init() call, test shutdown without Init()
}

TEST_F(ClientCacheMgrTest, InvalInodePages) {
  // The first InvalidateInode() call will cause the cache manager
  // thread to sleep, so that the next two InvalidateInode() calls
  // will always be done when that thread is waiting, thus combining
  // them to at most a single further call to NotifyInvalInode.
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, 0, 0))
      .Times(AtMost(2))
      .WillRepeatedly(DoAll(InvokeWithoutArgs(Sleep(0.01)),
                            Return(0)));

  cache_mgr_->InvalidateInode(InodeNum(1234), true);
  cache_mgr_->InvalidateInode(InodeNum(1234), true);
  cache_mgr_->InvalidateInode(InodeNum(1234), true);
  Sleep(0.1)();
}

TEST_F(ClientCacheMgrTest, InvalInodeSingleWatcher) {
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, -1, 0));

  boost::shared_ptr<ICacheInvRecord> rec = cache_mgr_->StartLookup();
  cache_mgr_->InvalidateInode(InodeNum(1234), false);
  Sleep(0.005)();
  {
    boost::lock_guard<MUTEX_TYPE> lock(*(rec->GetMutex()));
    EXPECT_TRUE(rec->InodeInvalidated(1234, false));
    EXPECT_FALSE(rec->InodeInvalidated(1234, true));
    EXPECT_FALSE(rec->InodeInvalidated(1235, false));
    EXPECT_FALSE(rec->InodeInvalidated(1235, true));
    // Can still add entry with lock if true passed
    cache_mgr_->AddEntry(1234, "hello", true);
  }
  Mock::VerifyAndClear(&*fuse_methods_);
  rec.reset();

  // Run the IR removal path
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, -1, 0));

  cache_mgr_->InvalidateInode(InodeNum(1234), false);
  Sleep(0.005)();
}

TEST_F(ClientCacheMgrTest, InvalInodeMultiWatcher) {
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, -1, 0));

  boost::shared_ptr<ICacheInvRecord> rec = cache_mgr_->StartLookup();
  cache_mgr_->InvalidateInode(InodeNum(1234), false);
  Sleep(0.005)();
  Mock::VerifyAndClear(&*fuse_methods_);

  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1235, -1, 0));

  boost::shared_ptr<ICacheInvRecord> rec2 = cache_mgr_->StartLookup();
  cache_mgr_->InvalidateInode(InodeNum(1235), false);
  Sleep(0.005)();

  {
    boost::lock_guard<MUTEX_TYPE> lock(*(rec->GetMutex()));
    EXPECT_TRUE(rec->InodeInvalidated(1234, false));
    EXPECT_TRUE(rec->InodeInvalidated(1235, false));
  }
  {
    boost::lock_guard<MUTEX_TYPE> lock(*(rec2->GetMutex()));
    EXPECT_FALSE(rec2->InodeInvalidated(1234, false));
    EXPECT_TRUE(rec2->InodeInvalidated(1235, false));
  }
}

TEST_F(ClientCacheMgrTest, InvalAllSingleWatcher) {
  boost::shared_ptr<ICacheInvRecord> rec = cache_mgr_->StartLookup();
  cache_mgr_->InvalidateAll();
  Sleep(0.005)();
  {
    boost::lock_guard<MUTEX_TYPE> lock(*(rec->GetMutex()));
    EXPECT_TRUE(rec->InodeInvalidated(1234, false));
    EXPECT_TRUE(rec->InodeInvalidated(1234, true));
    EXPECT_TRUE(rec->InodeInvalidated(1235, false));
    EXPECT_TRUE(rec->InodeInvalidated(1235, true));
  }
}

TEST_F(ClientCacheMgrTest, InvalInodeEntry) {
  cache_mgr_->RegisterInode(1234);
  cache_mgr_->AddEntry(1234, "hello.world");
  cache_mgr_->AddEntry(1234, "42");
  cache_mgr_->AddEntry(1234, "42");
  cache_mgr_->AddEntry(1235, "foo.bar");
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, -1, 0));

  cache_mgr_->InvalidateInode(InodeNum(1234), false);
  Sleep(0.005)();
  Mock::VerifyAndClear(&*fuse_methods_);

  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, 0, 0));
  EXPECT_CALL(*fuse_methods_, NotifyInvalEntry(
      0, 1234, StartsWith("hello.world"), 11));
  EXPECT_CALL(*fuse_methods_, NotifyInvalEntry(0, 1234, StartsWith("42"), 2));

  cache_mgr_->InvalidateInode(InodeNum(1234), true);
  Sleep(0.005)();
}

TEST_F(ClientCacheMgrTest, InvalAll) {
  cache_mgr_->RegisterInode(1233);
  cache_mgr_->RegisterInode(1235);
  cache_mgr_->AddEntry(1234, "hello.world");
  cache_mgr_->AddEntry(1234, "42");
  cache_mgr_->AddEntry(1235, "foo.bar");
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1233, 0, 0));
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, 0, 0));
  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1235, 0, 0));
  EXPECT_CALL(*fuse_methods_, NotifyInvalEntry(
      0, 1234, StartsWith("hello.world"), 11));
  EXPECT_CALL(*fuse_methods_, NotifyInvalEntry(0, 1234, StartsWith("42"), 2));
  EXPECT_CALL(*fuse_methods_, NotifyInvalEntry(
      0, 1235, StartsWith("foo.bar"), 7));

  cache_mgr_->InvalidateAll();
  Sleep(0.005)();
  Mock::VerifyAndClear(&*fuse_methods_);
  cache_mgr_->InvalidateAll();
  Sleep(0.005)();
}

TEST_F(ClientCacheMgrTest, CleanMgr) {
  cache_mgr_->RegisterInode(1233);
  cache_mgr_->RegisterInode(1235);
  cache_mgr_->AddEntry(1234, "hello.world");
  cache_mgr_->AddEntry(1234, "42");
  cache_mgr_->AddEntry(1235, "foo.bar");
  Sleep(1)();
  cache_mgr_->AddEntry(1234, "42");
  cache_mgr_->CleanMgr(1);

  EXPECT_CALL(*fuse_methods_, NotifyInvalInode(0, 1234, 0, 0));
  EXPECT_CALL(*fuse_methods_, NotifyInvalEntry(0, 1234, StartsWith("42"), 2));

  cache_mgr_->InvalidateAll();
  Sleep(0.005)();
}

}  // namespace
}  // namespace client
}  // namespace cpfs
