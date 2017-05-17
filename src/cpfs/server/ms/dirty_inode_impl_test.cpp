/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/dirty_inode_impl.hpp"

#include <stdint.h>

#include <cerrno>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "mock_actions.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/dirty_inode_mock.hpp"
#include "server/ms/store_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::Return;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/dirty_inode_impl_test_XXXXXX";

class DirtyInodeMgrTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  boost::scoped_ptr<IDirtyInodeMgr> dirty_inode_mgr_;

  DirtyInodeMgrTest()
      : data_path_mgr_(kDataPath),
        dirty_inode_mgr_(MakeDirtyInodeMgr(data_path_mgr_.GetPath())) {}
};

TEST_F(DirtyInodeMgrTest, GetSetPersist) {
  // Initial state
  dirty_inode_mgr_->Reset(false);
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(2));
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(3));
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(4));
  // Set dirty works as expected
  uint64_t gen;
  EXPECT_TRUE(dirty_inode_mgr_->SetVolatile(3, false, true, &gen));
  EXPECT_TRUE(dirty_inode_mgr_->IsVolatile(3));
  EXPECT_EQ(0U, gen);
  EXPECT_TRUE(dirty_inode_mgr_->SetVolatile(4, true, true, &gen));
  EXPECT_EQ(1U, gen);
  EXPECT_TRUE(dirty_inode_mgr_->IsVolatile(4));
  // Can get the dirty list correctly
  DirtyInodeMap dirty_inode_map = dirty_inode_mgr_->GetList();
  EXPECT_EQ(dirty_inode_map.end(), dirty_inode_map.find(2));
  EXPECT_NE(dirty_inode_map.end(), dirty_inode_map.find(3));
  EXPECT_FALSE(dirty_inode_map[3].active);
  EXPECT_EQ(0U, dirty_inode_map[3].gen);
  EXPECT_TRUE(dirty_inode_map[4].active);
  EXPECT_EQ(1U, dirty_inode_map[4].gen);
  // Data is persisted for another manager
  dirty_inode_mgr_.reset(MakeDirtyInodeMgr(data_path_mgr_.GetPath()));
  dirty_inode_mgr_->Reset(true);
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(2));
  EXPECT_TRUE(dirty_inode_mgr_->IsVolatile(3));
  EXPECT_TRUE(dirty_inode_mgr_->IsVolatile(4));
  dirty_inode_map = dirty_inode_mgr_->GetList();
  EXPECT_FALSE(dirty_inode_map[3].clean);
  EXPECT_FALSE(dirty_inode_map[4].clean);
  // But the list is set to all inactive
  EXPECT_EQ(dirty_inode_map.end(), dirty_inode_map.find(2));
  EXPECT_NE(dirty_inode_map.end(), dirty_inode_map.find(3));
  EXPECT_FALSE(dirty_inode_map[3].active);
  EXPECT_NE(dirty_inode_map.end(), dirty_inode_map.find(4));
  EXPECT_FALSE(dirty_inode_map[4].active);
  // Reset clean things up
  dirty_inode_mgr_.reset(MakeDirtyInodeMgr(data_path_mgr_.GetPath()));
  dirty_inode_mgr_->Reset(false);
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(2));
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(3));
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(4));
}

TEST_F(DirtyInodeMgrTest, ClearCleaning) {
  dirty_inode_mgr_->Reset(false);
  // Create an unclean inode
  uint64_t gen;
  EXPECT_FALSE(dirty_inode_mgr_->SetVolatile(4, true, false, &gen));
  EXPECT_FALSE(dirty_inode_mgr_->SetVolatile(4, false, true, &gen));
  // Start cleaning it
  EXPECT_TRUE(dirty_inode_mgr_->StartCleaning(4));
  // Cannot clean again
  EXPECT_FALSE(dirty_inode_mgr_->StartCleaning(4));
  // Clear cleaning
  dirty_inode_mgr_->ClearCleaning();
  // Can clean again now
  EXPECT_TRUE(dirty_inode_mgr_->StartCleaning(4));
}

TEST_F(DirtyInodeMgrTest, Clean) {
  dirty_inode_mgr_->Reset(false);
  uint64_t version1 = dirty_inode_mgr_->version();
  uint64_t version2;
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(4, &version2));
  EXPECT_EQ(version1, version2);
  EXPECT_FALSE(dirty_inode_mgr_->StartCleaning(4));
  // okay for gen_ret to be null
  EXPECT_TRUE(dirty_inode_mgr_->SetVolatile(4, true, true, 0));
  // Clean not allowed when active
  uint64_t gen;
  EXPECT_FALSE(dirty_inode_mgr_->SetVolatile(4, true, false, &gen));
  EXPECT_EQ(2U, gen);
  EXPECT_FALSE(dirty_inode_mgr_->StartCleaning(4));
  EXPECT_FALSE(dirty_inode_mgr_->Clean(4, &gen));
  EXPECT_EQ(0U, gen);
  EXPECT_TRUE(dirty_inode_mgr_->IsVolatile(4, &version2));
  EXPECT_EQ(version1, version2);
  // Clean would not happen for old generation
  EXPECT_FALSE(dirty_inode_mgr_->SetVolatile(4, false, true, &gen));
  EXPECT_TRUE(dirty_inode_mgr_->StartCleaning(4));
  EXPECT_FALSE(dirty_inode_mgr_->StartCleaning(4));  // Advise not to repeat
  --gen;
  EXPECT_FALSE(dirty_inode_mgr_->Clean(4, &gen));
  EXPECT_EQ(2U, gen);
  // Clean would not happen if attribute set
  EXPECT_TRUE(dirty_inode_mgr_->StartCleaning(4));
  dirty_inode_mgr_->NotifyAttrSet(4);
  EXPECT_FALSE(dirty_inode_mgr_->Clean(4, &gen));
  EXPECT_EQ(3U, gen);
  // Clean allowed when inactive
  EXPECT_TRUE(dirty_inode_mgr_->IsVolatile(4, &version2));
  EXPECT_EQ(version1, version2);
  EXPECT_TRUE(dirty_inode_mgr_->StartCleaning(4));
  boost::unique_lock<MUTEX_TYPE> lock;
  EXPECT_TRUE(dirty_inode_mgr_->Clean(4, &gen, &lock));
  lock.unlock();
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(4, &version2));
  EXPECT_LT(version1, version2);
  // Clean won't break if already clean
  EXPECT_TRUE(dirty_inode_mgr_->Clean(4, &gen));
  // Cleaned item are persisted
  dirty_inode_mgr_.reset(MakeDirtyInodeMgr(data_path_mgr_.GetPath()));
  dirty_inode_mgr_->Reset(true);
  EXPECT_FALSE(dirty_inode_mgr_->IsVolatile(4));
}

TEST_F(DirtyInodeMgrTest, ResetFromEmpty) {
  dirty_inode_mgr_->Reset(true);  // Should not throw
  DirtyInodeMap dirty_inode_map = dirty_inode_mgr_->GetList();
  EXPECT_TRUE(dirty_inode_map.empty());
}

class AttrUpdaterTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  MockIStore* store_;
  MockITrackerMapper* tracker_mapper_;
  MockIDirtyInodeMgr* dirty_inode_mgr_;
  boost::scoped_ptr<IAttrUpdater> attr_updater_;

  AttrUpdaterTest() {
    server_.set_store(store_ = new MockIStore);
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    server_.set_dirty_inode_mgr(dirty_inode_mgr_ = new MockIDirtyInodeMgr);
    attr_updater_.reset(MakeAttrUpdater(&server_));
  }
};

TEST_F(AttrUpdaterTest, FailGetGroupIds) {
  EXPECT_CALL(*store_, GetFileGroupIds(5, _))
      .WillOnce(Return(-EIO));

  EXPECT_THROW(attr_updater_->AsyncUpdateAttr(5, FSTime(), 0,
                                              AttrUpdateHandler()),
               std::runtime_error);
}

TEST_F(AttrUpdaterTest, NormalAPI) {
  // Add one callback
  std::vector<GroupId> group_ids;
  group_ids.push_back(1);
  EXPECT_CALL(*store_, GetFileGroupIds(5, _))
      .WillOnce(DoAll(SetArgPointee<1>(group_ids),
                      Return(0)));
  boost::shared_ptr<MockIReqTracker> ds_trackers[kNumDSPerGroup];
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    ds_trackers[r].reset(new MockIReqTracker);
    EXPECT_CALL(*ds_trackers[r], name())
        .WillRepeatedly(Return(std::string("DS 1-") + char('0' + r)));
  }
  boost::shared_ptr<IReqEntry> req_entries[kNumDSPerGroup];
  for (GroupRole r = 0; r < kNumDSPerGroup - 1; ++r) {
    EXPECT_CALL(*tracker_mapper_, GetDSTracker(1, r))
        .WillOnce(Return(ds_trackers[r]));
    EXPECT_CALL(*ds_trackers[r], AddRequestEntry(_, _))
        .WillOnce(DoAll(SaveArg<0>(&req_entries[r]),
                        Return(r != 0)));  // No reply from first
  }
  // No tracker for last
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(1, kNumDSPerGroup - 1))
      .WillOnce(Return(boost::shared_ptr<IReqTracker>()));

  FSTime mtime = {123456789, 12345};
  MockFunction<void(FSTime mtime, uint64_t size)> cb;
  EXPECT_TRUE(attr_updater_->AsyncUpdateAttr(
      5, mtime, 123, boost::bind(GetMockCall(cb), &cb, _1, _2)));
  ASSERT_TRUE(req_entries[1]->request());
  ASSERT_EQ(kAttrUpdateFim, req_entries[1]->request()->type());
  AttrUpdateFim& req = static_cast<AttrUpdateFim&>(*req_entries[1]->request());
  ASSERT_EQ(5U, req->inode);

  // Callback called
  mtime.sec = 123456790;
  mtime.ns = 1234;
  EXPECT_CALL(cb, Call(mtime, 123));

  for (GroupRole r = 1; r < kNumDSPerGroup - 2; ++r) {
    FIM_PTR<AttrUpdateReplyFim> reply = AttrUpdateReplyFim::MakePtr();
    (*reply)->mtime = mtime;
    (*reply)->size = 12;
    req_entries[r]->SetReply(reply, 1);
  }
  req_entries[kNumDSPerGroup - 2]->SetReply(FIM_PTR<IFim>(), 1);

  Mock::VerifyAndClear(tracker_mapper_);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
