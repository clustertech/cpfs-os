/* Copyright 2013 ClusterTech Ltd */
#include "req_entry_impl.hpp"

#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim.hpp"
#include "fims.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::StartsWith;
using ::testing::StrEq;

namespace cpfs {
namespace {

class Callback {
 public:
  MOCK_METHOD1(func, void(boost::shared_ptr<IReqEntry>));
  ReqAckCallback GetCallback() {
    return boost::bind(&Callback::func, this, _1);
  }
};

void WaitReply(boost::shared_ptr<IReqEntry> entry,
               FIM_PTR<IFim> reply, bool* completed_ret) {
  EXPECT_EQ(reply, entry->WaitReply());
  *completed_ret = true;
}

TEST(ReqEntryTest, DefaultReqEntry) {
  // Construction
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  FIM_PTR<IFim> req(GetattrFim::MakePtr(0));
  boost::shared_ptr<IReqEntry> entry = MakeDefaultReqEntry(tracker, req);
  Callback cb1, cb2;
  entry->OnAck(cb1.GetCallback());
  entry->OnAck(cb2.GetCallback(), true);
  EXPECT_EQ(req, entry->request());
  EXPECT_FALSE(entry->reply());

  // About to be queued
  ReqIdGenerator gen;
  gen.SetClientNum(1);
  EXPECT_CALL(*tracker, req_id_gen())
      .WillOnce(Return(&gen));

  entry->PreQueueHook();
  EXPECT_EQ(0x100000000001U, req->req_id());
  EXPECT_EQ(0x100000000001U, entry->req_id());

  // Sent
  EXPECT_FALSE(entry->sent());
  entry->set_sent(true);
  EXPECT_TRUE(entry->sent());

  // Unset request
  entry->UnsetRequest();
  EXPECT_FALSE(entry->request());
  EXPECT_EQ(0x100000000001U, entry->req_id());

  // Reply
  EXPECT_FALSE(entry->reply());
  FIM_PTR<IFim> reply(ResultCodeReplyFim::MakePtr(0));
  bool reply_completed = false;
  boost::thread th(boost::bind(WaitReply, entry, reply, &reply_completed));
  Sleep(0.01)();
  EXPECT_FALSE(reply_completed);

  EXPECT_CALL(cb1, func(entry));

  entry->SetReply(reply, 123);
  Sleep(0.01)();
  EXPECT_TRUE(reply_completed);
  EXPECT_EQ(123U, entry->reply_no());

  // Dump
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Fim),
                   StartsWith("Request entry: type D, request Id "), _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Fim),
                   StartsWith("  request (null), sent = 1"), _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Fim),
                   StrEq("  reply #P00000-000000(ResultCodeReply), err = 0"),
                   _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Fim),
                   StrEq("  Number of callbacks (reg/fin): 0/1"),
                   _));

  entry->Dump();

  // Final reply
  EXPECT_CALL(cb2, func(entry));

  FIM_PTR<IFim> f_reply(ResultCodeReplyFim::MakePtr(0));
  f_reply->set_final();
  entry->SetReply(f_reply, 123);

  // Persistent
  EXPECT_TRUE(entry->IsPersistent());

  // FillLock
  {
    boost::unique_lock<MUTEX_TYPE> lock;
    EXPECT_FALSE(lock);

    entry->FillLock(&lock);
    EXPECT_TRUE(lock);
  }
}

TEST(ReqEntryTest, MakeReqEntryReplyError) {
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  FIM_PTR<IFim> req(GetattrFim::MakePtr(0));
  boost::shared_ptr<IReqEntry> entry = MakeDefaultReqEntry(tracker, req);
  entry->SetReply(FIM_PTR<IFim>(), 125);
  EXPECT_THROW(entry->WaitReply(), std::runtime_error);
}

void WaitReplyError(boost::shared_ptr<IReqEntry> entry, bool* completed_ret) {
  EXPECT_THROW(entry->WaitReply(), std::runtime_error);
  *completed_ret = true;
}

TEST(ReqEntryTest, MakeReqEntryReplyErrorWithWait) {
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  FIM_PTR<IFim> req(GetattrFim::MakePtr(0));
  boost::shared_ptr<IReqEntry> entry = MakeDefaultReqEntry(tracker, req);
  Callback cb;
  entry->OnAck(cb.GetCallback());

  bool reply_completed = false;
  boost::thread th(boost::bind(WaitReplyError, entry, &reply_completed));
  Sleep(0.01)();
  EXPECT_FALSE(reply_completed);

  EXPECT_CALL(cb, func(entry));

  entry->SetReply(FIM_PTR<IFim>(), 123);
  Sleep(0.01)();
  EXPECT_TRUE(reply_completed);
  EXPECT_FALSE(entry->reply());
}

TEST(ReqEntryTest, MakeReqEntryEarlyFinal) {
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  FIM_PTR<IFim> req(GetattrFim::MakePtr(0));
  boost::shared_ptr<IReqEntry> entry = MakeDefaultReqEntry(tracker, req);
  EXPECT_THROW(entry->SetReply(FinalReplyFim::MakePtr(), 125),
               std::runtime_error);
}

TEST(ReqEntryTest, TransientReqEntry) {
  // Construction
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  FIM_PTR<IFim> req(GetattrFim::MakePtr(0));
  boost::shared_ptr<IReqEntry> entry = MakeTransientReqEntry(tracker, req);
  EXPECT_EQ(req, entry->request());

  // About to be queued
  ReqIdGenerator gen;
  gen.SetClientNum(1);
  EXPECT_CALL(*tracker, req_id_gen())
      .WillOnce(Return(&gen));

  entry->PreQueueHook();
  EXPECT_EQ(0x100000000001U, req->req_id());
  EXPECT_EQ(0x100000000001U, entry->req_id());

  // Persistent
  EXPECT_FALSE(entry->IsPersistent());
}

TEST(ReqEntryTest, ReplReqEntry) {
  // Construction
  boost::shared_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  FIM_PTR<IFim> req(GetattrFim::MakePtr(0));
  req->set_req_id(100);
  boost::shared_ptr<IReqEntry> entry = MakeReplReqEntry(tracker, req);
  EXPECT_EQ(req, entry->request());

  // About to be queued
  entry->PreQueueHook();
  EXPECT_EQ(100U, req->req_id());
  EXPECT_EQ(100U, entry->req_id());

  // Persistent
  EXPECT_FALSE(entry->IsPersistent());
}

void GenReqId(ReqIdGenerator* gen, bool* completed_ret) {
  EXPECT_EQ(0x100000000001U, gen->GenReqId());
  *completed_ret = true;
}

TEST(ReqEntryTest, ReqIdGeneratorSetClientNum) {
  ReqIdGenerator gen;
  bool completed = false;
  boost::thread th(boost::bind(GenReqId, &gen, &completed));
  Sleep(0.01)();
  EXPECT_FALSE(completed);

  gen.SetClientNum(1);
  Sleep(0.01)();
  EXPECT_TRUE(completed);

  gen.SetClientNum(1);
  EXPECT_THROW(gen.SetClientNum(2), std::runtime_error);
}

}  // namespace
}  // namespace cpfs
