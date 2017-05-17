/* Copyright 2013 ClusterTech Ltd */
#include "ds_query_mgr.hpp"

#include <cstddef>
#include <map>
#include <stdexcept>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "base_ms_mock.hpp"
#include "common.hpp"
#include "ds_query_mgr_impl.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StartsWith;
using ::testing::Throw;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class DSQueryMgrTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  MockITrackerMapper* tracker_mapper_;
  boost::scoped_ptr<IDSQueryMgr> query_mgr_;
  boost::shared_ptr<MockIReqTracker> ds_req_trackers_[kNumDSPerGroup * 2];
  boost::shared_ptr<IReqEntry> req_entries_[kNumDSPerGroup * 2];

  DSQueryMgrTest()
      : tracker_mapper_(new MockITrackerMapper),
        query_mgr_(MakeDSQueryMgr(&server_)) {
    server_.set_tracker_mapper(tracker_mapper_);
    for (std::size_t i = 0; i < kNumDSPerGroup * 2; ++i)
      ds_req_trackers_[i] = boost::make_shared<MockIReqTracker>();
  }

  void PrepareAddRequests() {
    for (GroupId group = 0; group < 2; ++group) {
      for (GroupRole role = 0; role < kNumDSPerGroup; ++role) {
        boost::shared_ptr<MockIReqTracker> tracker =
            ds_req_trackers_[group * kNumDSPerGroup + role];
        EXPECT_CALL(*tracker_mapper_, GetDSTracker(group, role))
            .WillRepeatedly(Return(tracker));
        EXPECT_CALL(*tracker,  AddRequestEntry(_, _))
            .WillOnce(
                DoAll(SaveArg<0>(&req_entries_[group * kNumDSPerGroup + role]),
                      Return(true)));
      }
    }
  }

 public:
  void ReplyEntries(bool success, int num_groups) {
    Sleep(0.2)();
    for (std::size_t i = 0; i < kNumDSPerGroup * num_groups; ++i) {
      if (success) {
        req_entries_[i]->SetReply(FCStatFSReplyFim::MakePtr(), i);
      } else {
        FIM_PTR<ResultCodeReplyFim> reply = ResultCodeReplyFim::MakePtr();
        (*reply)->err_no = i;
        req_entries_[i]->SetReply(reply, i);
      }
    }
  }
};

TEST_F(DSQueryMgrTest, RequestSuccess) {
  PrepareAddRequests();
  std::vector<GroupId> req_groups;
  req_groups.push_back(0);
  req_groups.push_back(1);

  boost::thread replier(
      boost::bind(&DSQueryMgrTest::ReplyEntries, this, true, 2));
  DSReplies replies = query_mgr_->Request(FCStatFSFim::MakePtr(), req_groups);

  for (GroupId group = 0; group < req_groups.size(); ++group)
    for (GroupRole role = 0; role < kNumDSPerGroup; ++role)
      EXPECT_EQ(kFCStatFSReplyFim, replies[group][role]->type());

  replier.join();
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(DSQueryMgrTest, RequestFailure) {
  // Rely errors
  PrepareAddRequests();
  std::vector<GroupId> req_groups;
  req_groups.push_back(0);
  req_groups.push_back(1);

  boost::thread replier(
      boost::bind(&DSQueryMgrTest::ReplyEntries, this, false, 2));
  EXPECT_THROW(query_mgr_->Request(FCStatFSFim::MakePtr(), req_groups),
               std::runtime_error);

  replier.join();
  Mock::VerifyAndClear(tracker_mapper_);

  // AddRequest errors
  boost::shared_ptr<MockIReqTracker> tracker =
      boost::make_shared<MockIReqTracker>();
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(_, _))
      .WillRepeatedly(Return(tracker));
  EXPECT_CALL(*tracker,  AddRequestEntry(_, _))
      .WillRepeatedly(Return(false));

  DSReplies replies = query_mgr_->Request(FCStatFSFim::MakePtr(), req_groups);
  EXPECT_TRUE(replies.empty());
  Mock::VerifyAndClear(tracker_mapper_);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
