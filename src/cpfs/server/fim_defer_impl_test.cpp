/* Copyright 2013 ClusterTech Ltd */
#include "server/fim_defer_impl.hpp"

#include <algorithm>
#include <stdexcept>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "server/fim_defer.hpp"

namespace cpfs {

class IFimSocket;

namespace server {
namespace {

class FimDeferMgr : public ::testing::Test {
 protected:
  boost::scoped_ptr<IFimDeferMgr> fim_defer_mgr_;
  boost::scoped_ptr<IInodeFimDeferMgr> inode_fim_defer_mgr_;
  boost::scoped_ptr<ISegmentFimDeferMgr> segment_fim_defer_mgr_;

  FimDeferMgr()
      : fim_defer_mgr_(MakeFimDeferMgr()),
        inode_fim_defer_mgr_(MakeInodeFimDeferMgr()),
        segment_fim_defer_mgr_(MakeSegmentFimDeferMgr()) {}
};

TEST_F(FimDeferMgr, InodeAPI) {
  EXPECT_FALSE(inode_fim_defer_mgr_->IsLocked(1));
  inode_fim_defer_mgr_->SetLocked(1, true);
  EXPECT_TRUE(inode_fim_defer_mgr_->IsLocked(1));
  inode_fim_defer_mgr_->SetLocked(2, true);
  std::vector<InodeNum> locked = inode_fim_defer_mgr_->LockedInodes();
  EXPECT_EQ(2U, locked.size());
  EXPECT_NE(locked.end(), std::find(locked.begin(), locked.end(), 1));
  EXPECT_NE(locked.end(), std::find(locked.begin(), locked.end(), 2));
  inode_fim_defer_mgr_->SetLocked(1, false);
  EXPECT_FALSE(inode_fim_defer_mgr_->IsLocked(1));
  locked = inode_fim_defer_mgr_->LockedInodes();
  EXPECT_EQ(1U, locked.size());
  EXPECT_EQ(locked.end(), std::find(locked.begin(), locked.end(), 1));

  inode_fim_defer_mgr_->AddFimEntry(5, FIM_PTR<IFim>(),
                                    boost::shared_ptr<IFimSocket>());
  inode_fim_defer_mgr_->AddFimEntry(100, FIM_PTR<IFim>(),
                                    boost::shared_ptr<IFimSocket>());
  inode_fim_defer_mgr_->AddFimEntry(100, FIM_PTR<IFim>(),
                                    boost::shared_ptr<IFimSocket>());

  EXPECT_TRUE(inode_fim_defer_mgr_->HasFimEntry(100));
  EXPECT_FALSE(inode_fim_defer_mgr_->HasFimEntry(101));
  inode_fim_defer_mgr_->GetNextFimEntry(100);
  inode_fim_defer_mgr_->GetNextFimEntry(100);
  EXPECT_FALSE(inode_fim_defer_mgr_->HasFimEntry(100));
  EXPECT_THROW(inode_fim_defer_mgr_->GetNextFimEntry(100),
               std::runtime_error);
  EXPECT_TRUE(inode_fim_defer_mgr_->IsLocked(100));
  inode_fim_defer_mgr_->SetLocked(100, false);
  EXPECT_THROW(inode_fim_defer_mgr_->GetNextFimEntry(100),
               std::runtime_error);
}

TEST_F(FimDeferMgr, SegmentAPI) {
  // Lock and unlock
  EXPECT_FALSE(segment_fim_defer_mgr_->IsLocked(1, 100));
  segment_fim_defer_mgr_->SetLocked(1, 100, true);
  EXPECT_TRUE(segment_fim_defer_mgr_->IsLocked(1, 100));
  segment_fim_defer_mgr_->SetLocked(1, 100, false);
  EXPECT_FALSE(segment_fim_defer_mgr_->IsLocked(1, 100));

  // AddFimEntry / SegmentHasFimEntry / GetSegmentNextFimEntry
  segment_fim_defer_mgr_->AddFimEntry(5, 70, FIM_PTR<IFim>(),
                                      boost::shared_ptr<IFimSocket>());
  segment_fim_defer_mgr_->AddFimEntry(5, 70, FIM_PTR<IFim>(),
                                      boost::shared_ptr<IFimSocket>());
  segment_fim_defer_mgr_->AddFimEntry(5, 71, FIM_PTR<IFim>(),
                                      boost::shared_ptr<IFimSocket>());
  segment_fim_defer_mgr_->AddFimEntry(6, 70, FIM_PTR<IFim>(),
                                      boost::shared_ptr<IFimSocket>());
  EXPECT_TRUE(segment_fim_defer_mgr_->SegmentHasFimEntry(5, 71));
  EXPECT_TRUE(segment_fim_defer_mgr_->SegmentHasFimEntry(6, 70));
  EXPECT_FALSE(segment_fim_defer_mgr_->SegmentHasFimEntry(6, 71));
  segment_fim_defer_mgr_->GetSegmentNextFimEntry(5, 71);
  EXPECT_FALSE(segment_fim_defer_mgr_->SegmentHasFimEntry(5, 71));
  EXPECT_THROW(segment_fim_defer_mgr_->GetSegmentNextFimEntry(5, 71),
               std::runtime_error);
  EXPECT_TRUE(segment_fim_defer_mgr_->IsLocked(5, 71));
  segment_fim_defer_mgr_->SetLocked(5, 71, false);
  EXPECT_THROW(segment_fim_defer_mgr_->GetSegmentNextFimEntry(5, 71),
               std::runtime_error);
  EXPECT_FALSE(segment_fim_defer_mgr_->IsLocked(5, 71));

  // ClearNextFimEntry
  DeferredFimEntry entry;
  EXPECT_TRUE(segment_fim_defer_mgr_->ClearNextFimEntry(&entry));
  EXPECT_TRUE(segment_fim_defer_mgr_->ClearNextFimEntry(&entry));
  EXPECT_TRUE(segment_fim_defer_mgr_->SegmentHasFimEntry(5, 70) ||
              segment_fim_defer_mgr_->SegmentHasFimEntry(6, 71));
  EXPECT_TRUE(segment_fim_defer_mgr_->ClearNextFimEntry(&entry));
  EXPECT_FALSE(segment_fim_defer_mgr_->SegmentHasFimEntry(5, 70));
  EXPECT_FALSE(segment_fim_defer_mgr_->SegmentHasFimEntry(6, 71));
  EXPECT_FALSE(segment_fim_defer_mgr_->ClearNextFimEntry(&entry));
}

}  // namespace
}  // namespace server
}  // namespace cpfs
