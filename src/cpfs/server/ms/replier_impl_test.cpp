/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/replier_impl.hpp"

#include <cerrno>
#include <cstring>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/inode_mutex.hpp"
#include "server/ms/replier.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/reply_set_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StartsWith;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class MSReplierTest : public ::testing::Test {
 protected:
  MockBaseMetaServer meta_server_;
  MockITrackerMapper* tracker_mapper_;
  MockIReplySet* recent_reply_set_;
  MockIStateMgr* state_mgr_;
  boost::scoped_ptr<IReplier> replier_;
  boost::shared_ptr<MockIFimSocket> peer1_, peer2_, peerm_;
  boost::shared_ptr<MockIReqTracker> peer1_tracker_, peer2_tracker_,
    peerm_tracker_;
  std::vector<boost::shared_ptr<MockIReqEntry> > req_entries_;

  MockFunction<void()> cb_;
  ReplierCallback complete_cb_, empty_complete_cb_;

  MSReplierTest()
      : tracker_mapper_(new MockITrackerMapper),
        recent_reply_set_(new MockIReplySet),
        state_mgr_(new MockIStateMgr),
        replier_(MakeReplier(&meta_server_)),
        peer1_(new MockIFimSocket),
        peer2_(new MockIFimSocket),
        peerm_(new MockIFimSocket),
        peer1_tracker_(new MockIReqTracker),
        peer2_tracker_(new MockIReqTracker),
        peerm_tracker_(new MockIReqTracker) {
    meta_server_.set_tracker_mapper(tracker_mapper_);
    meta_server_.set_recent_reply_set(recent_reply_set_);
    meta_server_.set_state_mgr(state_mgr_);
    EXPECT_CALL(*peer1_, GetReqTracker())
        .WillRepeatedly(Return(peer1_tracker_.get()));
    EXPECT_CALL(*peer2_, GetReqTracker())
        .WillRepeatedly(Return(peer2_tracker_.get()));
    EXPECT_CALL(*peerm_, GetReqTracker())
        .WillRepeatedly(Return(peerm_tracker_.get()));
    EXPECT_CALL(*peer1_tracker_, peer_client_num())
        .WillRepeatedly(Return(1));
    EXPECT_CALL(*peer2_tracker_, peer_client_num())
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*peerm_tracker_, peer_client_num())
        .WillRepeatedly(Return(kNotClient));
    EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
        .WillRepeatedly(Return(peerm_));
    EXPECT_CALL(*tracker_mapper_, GetMSTracker())
        .WillRepeatedly(Return(peerm_tracker_));
    complete_cb_ = boost::bind(GetMockCall(cb_), &cb_);
  }

  ~MSReplierTest() {
    Mock::VerifyAndClear(peer1_.get());
    Mock::VerifyAndClear(peer2_.get());
    Mock::VerifyAndClear(peerm_.get());
    Mock::VerifyAndClear(peer1_tracker_.get());
    Mock::VerifyAndClear(peer2_tracker_.get());
    Mock::VerifyAndClear(peerm_tracker_.get());
    Mock::VerifyAndClear(tracker_mapper_);
  }

  FIM_PTR<IFim> MakeRequest(ReqId req_id) {
    FIM_PTR<UnlinkFim> req(new UnlinkFim(4));
    req->set_req_id(req_id);
    (*req)->inode = 100;
    std::memcpy(req->tail_buf(), "foo", 4);
    return req;
  }

  FIM_PTR<IFim> MakeReply(int err_no) {
    FIM_PTR<ResultCodeReplyFim> reply(new ResultCodeReplyFim);
    (*reply)->err_no = err_no;
    return reply;
  }

  boost::shared_ptr<MockIReqEntry> MakeReqEntry() {
    boost::shared_ptr<MockIReqEntry> ret(new MockIReqEntry);
    req_entries_.push_back(ret);
    return ret;
  }
};

// No standby MS
TEST_F(MSReplierTest, NonReplicating) {
  Mock::VerifyAndClear(tracker_mapper_);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));
  FIM_PTR<IFim> reply = MakeReply(ENOENT);
  EXPECT_CALL(*peer1_, WriteMsg(reply));
  EXPECT_CALL(cb_, Call());

  OpContext context;
  replier_->DoReply(MakeRequest(42), reply, peer1_, &context, complete_cb_);
  EXPECT_EQ(42U, reply->req_id());
  EXPECT_TRUE(reply->is_final());
}

// Sent from active MS to standby MS
TEST_F(MSReplierTest, FailedReplicated) {
  FIM_PTR<IFim> reply = MakeReply(ENOENT);
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateStandby));
  EXPECT_CALL(*recent_reply_set_, AddReply(reply));
  EXPECT_CALL(*peerm_, WriteMsg(reply));
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  EXPECT_CALL(callback, Call(PLEVEL(error, Replicate),
                             StartsWith("Error for replicate req #"), _));

  OpContext context;
  replier_->DoReply(MakeRequest(42), reply, peerm_, &context, complete_cb_);
  EXPECT_EQ(42U, reply->req_id());
}

// Simple read with standby
TEST_F(MSReplierTest, ReplicatingRead) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateStandby));
  FIM_PTR<IFim> reply = MakeReply(ENOENT);
  EXPECT_CALL(*peer1_, WriteMsg(reply));
  EXPECT_CALL(cb_, Call());

  OpContext context;
  replier_->DoReply(MakeRequest(42), reply, peer1_, &context, complete_cb_);
  EXPECT_EQ(42U, reply->req_id());
  EXPECT_TRUE(reply->is_final());
}

// Simple write with standby, need replication but failed on
// replication, treated as completed immediately
TEST_F(MSReplierTest, FailedReplicatingWrite) {
  FIM_PTR<IFim> req = MakeRequest(42);
  FIM_PTR<IFim> reply = MakeReply(ENOENT);
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateActive));
  boost::shared_ptr<IReqEntry> req_entry;
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry),
                      Return(false)));
  EXPECT_CALL(*peer1_, WriteMsg(reply));
  EXPECT_CALL(cb_, Call());

  OpContext context;
  context.inodes_changed.push_back(100);
  replier_->DoReply(req, reply, peer1_, &context, complete_cb_);
  EXPECT_EQ(42U, reply->req_id());
  EXPECT_TRUE(reply->is_final());
}

// Simple write with standby, need replication but will reply immediately
TEST_F(MSReplierTest, FreeReplicatingWrite) {
  FIM_PTR<IFim> req = MakeRequest(42);
  FIM_PTR<IFim> reply = MakeReply(ENOENT);
  boost::shared_ptr<IReqEntry> req_entry;
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateActive));
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry),
                      Return(true)));
  EXPECT_CALL(*peer1_, WriteMsg(reply));

  OpContext context;
  context.inodes_changed.push_back(100);
  replier_->DoReply(req, reply, peer1_, &context, complete_cb_);
  EXPECT_EQ(42U, reply->req_id());
  EXPECT_FALSE(reply->is_final());

  // On reply, send final reply, and run completion CB
  FIM_PTR<IFim> final_reply;
  EXPECT_CALL(*peer1_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&final_reply));
  EXPECT_CALL(cb_, Call());

  req_entry->SetReply(reply, 1);
  FinalReplyFim& rfinal_reply =
      dynamic_cast<FinalReplyFim&>(*final_reply.get());
  EXPECT_EQ(unsigned(ENOENT), rfinal_reply->err_no);
  EXPECT_TRUE(final_reply->is_final());
}

// Write followed by read from different client, will defer reply to read
TEST_F(MSReplierTest, DelayedReplicatingWrite) {
  // Write
  FIM_PTR<IFim> req1 = MakeRequest(42);
  FIM_PTR<IFim> reply1 = MakeReply(ENOENT);
  boost::shared_ptr<IReqEntry> req1_entry;
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req1_entry),
                      Return(true)));
  EXPECT_CALL(*peer1_, WriteMsg(reply1));

  OpContext context1;
  context1.inodes_changed.push_back(100);
  context1.inodes_changed.push_back(100);
  replier_->DoReply(req1, reply1, peer1_, &context1, empty_complete_cb_);
  EXPECT_EQ(42U, reply1->req_id());

  // Read
  FIM_PTR<IFim> req2 = MakeRequest(43);
  FIM_PTR<IFim> reply2 = MakeReply(ENOENT);

  OpContext context2;
  context2.inodes_read.push_back(100);
  context2.inodes_read.push_back(100);
  replier_->DoReply(req2, reply2, peer2_, &context2, empty_complete_cb_);
  EXPECT_EQ(43U, reply2->req_id());
  EXPECT_FALSE(reply1->is_final());

  // Write replication completion
  FIM_PTR<IFim> final_reply;
  EXPECT_CALL(*peer1_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&final_reply));
  EXPECT_CALL(*peer2_, WriteMsg(reply2));

  req1_entry->SetReply(reply2, 1);
  EXPECT_TRUE(final_reply->is_final());
  EXPECT_EQ(kFinalReplyFim, final_reply->type());
  EXPECT_TRUE(reply2->is_final());
}

// Write followed by write from same client, will optimze out delay of reply
TEST_F(MSReplierTest, OptimizedReplicatingWrite) {
  FIM_PTR<IFim> req1 = MakeRequest(42);
  FIM_PTR<IFim> reply1 = MakeReply(ENOENT);
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*peer1_, WriteMsg(reply1));

  OpContext context1;
  context1.inodes_changed.push_back(100);
  replier_->DoReply(req1, reply1, peer1_, &context1, empty_complete_cb_);
  EXPECT_EQ(42U, reply1->req_id());
  EXPECT_FALSE(reply1->is_final());

  FIM_PTR<IFim> req2 = MakeRequest(43);
  FIM_PTR<IFim> reply2 = MakeReply(ENOENT);
  EXPECT_CALL(*peer1_, WriteMsg(reply2));
  boost::shared_ptr<MockIReqEntry> req2_entry = MakeReqEntry();
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(Return(true));

  OpContext context2;
  context2.inodes_changed.push_back(100);
  replier_->DoReply(req2, reply2, peer1_, &context2, empty_complete_cb_);
  EXPECT_EQ(43U, reply2->req_id());
  EXPECT_FALSE(reply2->is_final());
}

// Two writes by different clients followed by read all on same inode,
// will defer reply to read
TEST_F(MSReplierTest, UnoptimizingReplicatingWrite) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  // Write 1
  FIM_PTR<IFim> req1 = MakeRequest(42);
  FIM_PTR<IFim> reply1 = MakeReply(ENOENT);
  boost::shared_ptr<IReqEntry> req1_entry;
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req1_entry),
                      Return(true)));
  EXPECT_CALL(*peer1_, WriteMsg(reply1));

  OpContext context1;
  context1.inodes_changed.push_back(100);
  replier_->DoReply(req1, reply1, peer1_, &context1, empty_complete_cb_);
  EXPECT_EQ(42U, reply1->req_id());
  EXPECT_FALSE(reply1->is_final());

  // Write 2
  FIM_PTR<IFim> req2 = MakeRequest(43);
  FIM_PTR<IFim> reply2 = MakeReply(ENOENT);
  boost::shared_ptr<IReqEntry> req2_entry;
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req2_entry),
                      Return(true)));

  OpContext context2;
  context2.inodes_changed.push_back(100);
  replier_->DoReply(req2, reply2, peer2_, &context2, empty_complete_cb_);
  EXPECT_EQ(43U, reply2->req_id());
  EXPECT_FALSE(reply2->is_final());

  // Read
  FIM_PTR<IFim> req3 = MakeRequest(44);
  FIM_PTR<IFim> reply3 = MakeReply(ENOENT);

  OpContext context3;
  context3.inodes_read.push_back(100);
  replier_->DoReply(req3, reply3, peer1_, &context3, empty_complete_cb_);
  EXPECT_EQ(44U, reply3->req_id());
  EXPECT_TRUE(reply3->is_final());

  // Write 1 replication completion
  EXPECT_CALL(*peer1_, WriteMsg(_));
  EXPECT_CALL(*peer2_, WriteMsg(reply2));

  req1_entry->SetReply(reply2, 1);

  // Write 2 replication completion
  EXPECT_CALL(*peer2_, WriteMsg(_));
  EXPECT_CALL(*peer1_, WriteMsg(reply3));

  req2_entry->SetReply(reply3, 1);
}

// Two writes by different clients on different inodes followed by
// read on both inodes by one of them, will defer reply to read to
// reply of the other
TEST_F(MSReplierTest, PartiallyOptimizingReplicatingWrite) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  // Write 1
  FIM_PTR<IFim> req1 = MakeRequest(42);
  FIM_PTR<IFim> reply1 = MakeReply(ENOENT);
  boost::shared_ptr<IReqEntry> req1_entry;
  boost::function<void(boost::shared_ptr<IReqEntry>)> reply1_cb;
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req1_entry),
                      Return(true)));
  EXPECT_CALL(*peer1_, WriteMsg(reply1));

  OpContext context1;
  context1.inodes_changed.push_back(100);
  replier_->DoReply(req1, reply1, peer1_, &context1, empty_complete_cb_);
  EXPECT_EQ(42U, reply1->req_id());

  // Write 2
  FIM_PTR<IFim> req2 = MakeRequest(43);
  FIM_PTR<IFim> reply2 = MakeReply(ENOENT);
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*peer2_, WriteMsg(reply2));

  OpContext context2;
  context2.inodes_changed.push_back(101);
  replier_->DoReply(req2, reply2, peer2_, &context2, empty_complete_cb_);
  EXPECT_EQ(43U, reply2->req_id());

  // Read
  FIM_PTR<IFim> req3 = MakeRequest(44);
  FIM_PTR<IFim> reply3 = MakeReply(ENOENT);

  OpContext context3;
  context3.inodes_read.push_back(100);
  context3.inodes_read.push_back(101);
  replier_->DoReply(req3, reply3, peer2_, &context3, empty_complete_cb_);
  EXPECT_EQ(44U, reply3->req_id());
  EXPECT_TRUE(reply3->is_final());

  // Write 1 replication completion, trigger reply to read already
  EXPECT_CALL(*peer1_, WriteMsg(_));
  EXPECT_CALL(*peer2_, WriteMsg(reply3));

  req1_entry->SetReply(reply3, 1);
}

TEST_F(MSReplierTest, ReplCallbacks) {
  EXPECT_CALL(*state_mgr_, GetState())
      .WillRepeatedly(Return(kStateActive));
  FIM_PTR<IFim> req = MakeRequest(42);
  FIM_PTR<IFim> reply = MakeReply(ENOENT);
  EXPECT_CALL(*peerm_tracker_, AddRequestEntry(_, _))
      .Times(3).WillRepeatedly(Return(true));
  EXPECT_CALL(*peer1_, WriteMsg(reply)).Times(3);

  OpContext context;
  context.inodes_changed.push_back(100);
  replier_->DoReply(req, reply, peer1_, &context, complete_cb_);
  replier_->DoReply(req, reply, peer1_, &context, complete_cb_);
  replier_->ExpireCallbacks(0);

  replier_->DoReply(req, reply, peer1_, &context, complete_cb_);
  EXPECT_CALL(cb_, Call());
  replier_->RedoCallbacks();
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
