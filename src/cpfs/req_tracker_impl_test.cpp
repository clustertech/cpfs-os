/* Copyright 2013 ClusterTech Ltd */
#include "req_tracker_impl.hpp"

#include <stdexcept>
#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "asio_policy_mock.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "mutex_util.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker.hpp"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StartsWith;
using ::testing::StrEq;

namespace cpfs {

class IReqLimiter;

namespace {

class ReqTrackerTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<MockIAsioPolicy> asio_policy_;
  ReqIdGenerator gen_;
  boost::shared_ptr<IReqTracker> req_tracker_;

  ReqTrackerTest()
      : asio_policy_(new MockIAsioPolicy),
        req_tracker_(MakeReqTracker("myname", asio_policy_.get(), &gen_, 42)) {}
};

TEST_F(ReqTrackerTest, APITest) {
  // Naming
  EXPECT_EQ(std::string("myname"), req_tracker_->name());

  // Init
  EXPECT_EQ(42U, req_tracker_->peer_client_num());
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  gen_.SetClientNum(2);

  // Create non-persistent entry
  boost::shared_ptr<MockIReqEntry> entry1(new MockIReqEntry);
  FIM_PTR<IFim> req1 = GetattrFim::MakePtr(0);
  req1->set_req_id(1);
  EXPECT_CALL(*entry1, request())
      .WillRepeatedly(Return(req1));
  EXPECT_CALL(*entry1, IsPersistent())
      .WillRepeatedly(Return(false));

  // Before setting Fim socket, cannot send non-persistent entries
  EXPECT_CALL(*entry1, PreQueueHook());
  EXPECT_CALL(*entry1, SetReply(_, _));

  EXPECT_FALSE(req_tracker_->AddRequestEntry(entry1));

  // Set Fim socket
  req_tracker_->SetFimSocket(fim_socket);
  EXPECT_EQ(fim_socket, req_tracker_->GetFimSocket());

  // Can now send non-persistent entry
  EXPECT_CALL(*entry1, PreQueueHook());
  boost::unique_lock<MUTEX_TYPE> lock;
  EXPECT_CALL(*entry1, FillLock(&lock));
  EXPECT_CALL(*fim_socket, WriteMsg(req1));
  EXPECT_CALL(*entry1, set_sent(true));
  EXPECT_CALL(*entry1, UnsetRequest());

  EXPECT_TRUE(req_tracker_->AddRequestEntry(entry1, &lock));
  EXPECT_EQ(entry1, req_tracker_->GetRequestEntry(1));

  // DumpPendingRequests results in Dump of request entries
  EXPECT_CALL(*entry1, Dump());

  req_tracker_->DumpPendingRequests();

  // Full reply to req
  FIM_PTR<IFim> reply = GetattrFim::MakePtr(0);
  reply->set_req_id(1ULL);
  reply->set_final();
  EXPECT_CALL(*entry1, SetReply(reply, 2));

  req_tracker_->AddReply(reply);

  // Can directly get a default request entry with AddRequest()
  FIM_PTR<IFim> req2 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req2));

  req_tracker_->AddRequest(req2);

  // Too early FinalReplyFim would throw exception
  FIM_PTR<FinalReplyFim> reply2 = FinalReplyFim::MakePtr(0);
  reply2->set_req_id(req2->req_id());
  EXPECT_THROW(req_tracker_->AddReply(reply2), std::runtime_error);

  // limiter
  req_tracker_->set_limiter_max_send(1024 * 1024 * 1024);
  IReqLimiter* limiter = req_tracker_->GetReqLimiter();
  EXPECT_TRUE(limiter);
  EXPECT_EQ(limiter, req_tracker_->GetReqLimiter());

  Mock::VerifyAndClear(fim_socket.get());
}

TEST_F(ReqTrackerTest, UnpluggedTest) {
  // Try plugging a request tracker with no FimSocket
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  EXPECT_CALL(callback,
              Call(PLEVEL(warning, Fim),
                   StrEq("Plugging a ReqTracker without FimSocket, ignored"),
                   _));

  req_tracker_->Plug();

  // Make an unplugged request tracker with FimSocket
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  gen_.SetClientNum(2);
  req_tracker_->SetFimSocket(fim_socket, false);
  EXPECT_EQ(fim_socket, req_tracker_->GetFimSocket());

  // Add a request, no message should be sent
  FIM_PTR<IFim> req(new GetattrFim);
  boost::shared_ptr<IReqEntry> entry = req_tracker_->AddRequest(req);
  EXPECT_EQ(0x200000000001ULL, req->req_id());

  // Send the message using resend
  EXPECT_CALL(*fim_socket, WriteMsg(req));

  req_tracker_->Plug(true);

  // Should now be plugged, send another message
  FIM_PTR<IFim> req2(new GetattrFim);
  EXPECT_CALL(*fim_socket, WriteMsg(req2));

  req_tracker_->AddRequest(req2);

  // Can also directly plug it
  FIM_PTR<IFim> req3(new GetattrFim);
  EXPECT_CALL(*fim_socket, WriteMsg(req3));

  req_tracker_->Plug(false);
  req_tracker_->Plug(true);
  req_tracker_->AddTransientRequest(req3);
}

TEST_F(ReqTrackerTest, ExpectingTest) {
  // Setup
  gen_.SetClientNum(2);
  FIM_PTR<IFim> req(new GetattrFim);
  boost::shared_ptr<MockIReqEntry> entry(new MockIReqEntry);
  EXPECT_CALL(*entry, request())
      .WillRepeatedly(Return(req));
  EXPECT_CALL(*entry, IsPersistent())
      .WillRepeatedly(Return(false));

  // When expecting, adding transient requests should be okay
  EXPECT_CALL(*entry, PreQueueHook());

  req_tracker_->SetExpectingFimSocket(true);
  EXPECT_TRUE(req_tracker_->AddRequestEntry(entry));

  // On unset of the most current one, this queues a task
  ServiceTask task;
  EXPECT_CALL(*asio_policy_, Post(_))
      .WillOnce(SaveArg<0>(&task));

  req_tracker_->SetExpectingFimSocket(false);

  // Upon running, the task notify failure
  EXPECT_CALL(*entry, SetReply(FIM_PTR<IFim>(), 1));

  task();

  // Set expecting and add entry again
  EXPECT_CALL(*entry, PreQueueHook());

  req_tracker_->SetExpectingFimSocket(true);
  EXPECT_TRUE(req_tracker_->AddRequestEntry(entry));

  // Message is sent when FimSocket is assigned and plugged
  EXPECT_CALL(*entry, sent());
  EXPECT_CALL(*entry, set_reply_error(false));
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*fim_socket, WriteMsg(req));
  EXPECT_CALL(*entry, set_sent(true));
  EXPECT_CALL(*entry, UnsetRequest());

  req_tracker_->SetFimSocket(fim_socket, true);
}

TEST_F(ReqTrackerTest, RedundantAckTest) {
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  FIM_PTR<GetattrFim> reply = GetattrFim::MakePtr(0);
  reply->set_req_id(1);
  EXPECT_CALL(callback,
              Call(PLEVEL(warning, Fim),
                   StartsWith("No request matches Fim #Q00000-000001"
                              "(Getattr)"),
                   _));
  req_tracker_->AddReply(reply);
}

TEST_F(ReqTrackerTest, ReqResend) {
  // Before Init
  EXPECT_FALSE(req_tracker_->ResendReplied());

  // Init
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  gen_.SetClientNum(2);
  req_tracker_->SetFimSocket(fim_socket);
  EXPECT_EQ(fim_socket, req_tracker_->GetFimSocket());
  // Initial state: No requests to resend
  EXPECT_FALSE(req_tracker_->ResendReplied());
  req_tracker_->Plug();

  // Fixtures
  FIM_PTR<IFim> req[6];
  boost::shared_ptr<IReqEntry> entry[6];
  for (unsigned i = 0; i < 6; ++i) {
    req[i] = GetattrFim::MakePtr(0);
    EXPECT_CALL(*fim_socket, WriteMsg(req[i]));
  }

  // Requests
  req[0]->set_req_id(1);
  MockIReqEntry* np_entry;
  entry[0].reset(np_entry = new MockIReqEntry);
  EXPECT_CALL(*np_entry, IsPersistent())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*np_entry, request())
      .WillRepeatedly(Return(req[0]));
  EXPECT_CALL(*np_entry, PreQueueHook());
  EXPECT_CALL(*np_entry, set_sent(true));
  EXPECT_CALL(*np_entry, UnsetRequest());
  req_tracker_->AddRequestEntry(entry[0]);
  entry[1] = req_tracker_->AddRequest(req[1]);
  entry[2] = req_tracker_->AddRequest(req[2]);
  entry[3] = req_tracker_->AddRequest(req[3]);
  entry[4] = req_tracker_->AddRequest(req[4]);
  entry[5] = req_tracker_->AddRequest(req[5]);
  FIM_PTR<IFim> reply[6];
  for (unsigned i = 0; i < 6; ++i) {
    reply[i] = GetattrFim::MakePtr(0);
    reply[i]->set_req_id(req[i]->req_id());
  }

  // Replies
  EXPECT_CALL(*np_entry, SetReply(reply[0], 1));

  req_tracker_->AddReply(reply[0]);  // Treated as full: non-persistent entry
  req_tracker_->AddReply(reply[5]);
  req_tracker_->AddReply(reply[2]);
  reply[3]->set_final();
  req_tracker_->AddReply(reply[3]);

  // Reconnect
  req_tracker_->SetFimSocket(boost::shared_ptr<IFimSocket>());
  req_tracker_->SetFimSocket(fim_socket, false);

  // Check reply order on resend
  boost::shared_ptr<MockIFimSocket> fim_socket2(new MockIFimSocket);
  req_tracker_->SetFimSocket(fim_socket2, false);
  {
    InSequence seq;
    EXPECT_CALL(*fim_socket2, WriteMsg(req[5]));
    EXPECT_CALL(*fim_socket2, WriteMsg(req[2]));
  }

  EXPECT_TRUE(req_tracker_->ResendReplied());

  {
    InSequence seq;
    EXPECT_CALL(*fim_socket2, WriteMsg(req[1]));
    EXPECT_CALL(*fim_socket2, WriteMsg(req[4]));
  }

  req_tracker_->Plug();

  Mock::VerifyAndClear(fim_socket.get());
}

TEST_F(ReqTrackerTest, Shutdown) {
  gen_.SetClientNum(2);
  FIM_PTR<IFim> req[2];
  boost::shared_ptr<IReqEntry> entry[2];
  req[0] = GetattrFim::MakePtr(0);
  req[1] = GetattrFim::MakePtr(0);
  entry[0] = req_tracker_->AddRequest(req[0]);
  entry[1] = req_tracker_->AddRequest(req[1]);

  // Shutdown should cause a task to be queued
  ServiceTask task;
  EXPECT_CALL(*asio_policy_, Post(_))
      .WillOnce(SaveArg<0>(&task));

  req_tracker_->Shutdown();

  // Run the task would notify the failure
  task();

  // Should be non-blocking
  EXPECT_THROW(entry[0]->WaitReply(), std::runtime_error);
  EXPECT_THROW(entry[1]->WaitReply(), std::runtime_error);
}

TEST_F(ReqTrackerTest, SingleRedirect) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  req_tracker_->SetFimSocket(fim_socket);
  gen_.SetClientNum(2);
  boost::shared_ptr<IReqTracker> tracker1(
      MakeReqTracker("t", asio_policy_.get(), &gen_, 0));
  boost::shared_ptr<MockIFimSocket> socket1(new MockIFimSocket);
  tracker1->SetFimSocket(socket1);

  // Entry not found
  req_tracker_->RedirectRequest(1, tracker1);

  // Entry found: cause message to be sent
  FIM_PTR<IFim> req1 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req1));

  boost::shared_ptr<IReqEntry> entry1 = req_tracker_->AddRequest(req1);

  EXPECT_CALL(*socket1, WriteMsg(req1));

  req_tracker_->RedirectRequest(req1->req_id(), tracker1);
  EXPECT_EQ(boost::shared_ptr<IReqEntry>(),
            req_tracker_->GetRequestEntry(req1->req_id()));
  EXPECT_EQ(entry1, tracker1->GetRequestEntry(req1->req_id()));
}

TEST_F(ReqTrackerTest, ReqRedirectNormal) {
  // Fixtures
  MockFunction<boost::shared_ptr<IReqTracker>(FIM_PTR<IFim>)> mock_redirector;
  ReqRedirector redirector = boost::bind(
      GetMockCall(mock_redirector), &mock_redirector, _1);
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  req_tracker_->SetFimSocket(fim_socket);
  gen_.SetClientNum(2);

  // No Fim to redirect
  EXPECT_FALSE(req_tracker_->RedirectRequests(redirector));

  // Create some Fims for redirection

  FIM_PTR<IFim> req1 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req1));

  boost::shared_ptr<IReqEntry> entry1 = req_tracker_->AddRequest(req1);

  FIM_PTR<IFim> req2 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req2));

  boost::shared_ptr<IReqEntry> entry2 = req_tracker_->AddRequest(req2);

  FIM_PTR<IFim> req3 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req3));

  boost::shared_ptr<IReqEntry> entry3 = req_tracker_->AddRequest(req3);

  // Make entry3 initially replied
  FIM_PTR<IFim> rep3 = ResultCodeReplyFim::MakePtr(0);
  rep3->set_req_id(entry3->req_id());
  req_tracker_->AddReply(rep3);

  // Make some more trackers to redirect to, and associate them with
  // FimSocket's
  boost::shared_ptr<IReqTracker> tracker1(
      MakeReqTracker("t1", asio_policy_.get(), &gen_, 0));
  boost::shared_ptr<MockIFimSocket> socket1(new MockIFimSocket);
  tracker1->SetFimSocket(socket1);
  boost::shared_ptr<IReqTracker> tracker2(
      MakeReqTracker("t2", asio_policy_.get(), &gen_, 0));
  boost::shared_ptr<MockIFimSocket> socket2(new MockIFimSocket);
  tracker2->SetFimSocket(socket2);
  boost::shared_ptr<IReqTracker> tracker3(
      MakeReqTracker("t3", asio_policy_.get(), &gen_, 0));
  boost::shared_ptr<MockIFimSocket> socket3(new MockIFimSocket);
  tracker3->SetFimSocket(socket3);

  // Redirect the Fims
  InSequence seq;
  EXPECT_CALL(mock_redirector, Call(req3))
      .WillOnce(Return(tracker3));
  EXPECT_CALL(*socket3, WriteMsg(req3));
  EXPECT_CALL(mock_redirector, Call(req1))
      .WillOnce(Return(tracker1));
  EXPECT_CALL(*socket1, WriteMsg(req1));
  EXPECT_CALL(mock_redirector, Call(req2))
      .WillOnce(Return(tracker2));
  EXPECT_CALL(*socket2, WriteMsg(req2));

  EXPECT_TRUE(req_tracker_->RedirectRequests(redirector));

  tracker1->SetFimSocket(boost::shared_ptr<IFimSocket>());
  tracker2->SetFimSocket(boost::shared_ptr<IFimSocket>());
  tracker3->SetFimSocket(boost::shared_ptr<IFimSocket>());
}

TEST_F(ReqTrackerTest, ReqRedirectUnexpected) {
  // Fixtures
  MockFunction<boost::shared_ptr<IReqTracker>(FIM_PTR<IFim>)> mock_redirector;
  ReqRedirector redirector = boost::bind(
      GetMockCall(mock_redirector), &mock_redirector, _1);
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  req_tracker_->SetFimSocket(fim_socket);
  gen_.SetClientNum(2);

  // Create some Fims for redirection

  FIM_PTR<IFim> req1 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req1));

  boost::shared_ptr<IReqEntry> entry1 = req_tracker_->AddRequest(req1);

  FIM_PTR<IFim> req2 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req2));

  boost::shared_ptr<IReqEntry> entry2 = req_tracker_->AddRequest(req2);

  FIM_PTR<IFim> req3 = GetattrFim::MakePtr(0);
  EXPECT_CALL(*fim_socket, WriteMsg(req3));

  boost::shared_ptr<IReqEntry> entry3 = req_tracker_->AddRequest(req3);

  // Make entry3 initially replied
  FIM_PTR<IFim> rep3 = ResultCodeReplyFim::MakePtr(0);
  rep3->set_req_id(entry3->req_id());
  req_tracker_->AddReply(rep3);

  // Make some more trackers to redirect to, and associate them with
  // FimSocket's
  boost::shared_ptr<IReqTracker> tracker1(
      MakeReqTracker("other", asio_policy_.get(), &gen_, 0));
  boost::shared_ptr<MockIFimSocket> socket1(new MockIFimSocket);
  tracker1->SetFimSocket(socket1);

  InSequence seq;
  EXPECT_CALL(mock_redirector, Call(req3))
      .WillOnce(Return(tracker1));
  EXPECT_CALL(*socket1, WriteMsg(req3));
  EXPECT_CALL(mock_redirector, Call(req1))
      .WillOnce(Return(boost::shared_ptr<IReqTracker>()));
  EXPECT_CALL(mock_redirector, Call(req2))
      .WillOnce(Return(req_tracker_));

  EXPECT_TRUE(req_tracker_->RedirectRequests(redirector));

  tracker1->SetFimSocket(boost::shared_ptr<IFimSocket>());
}

}  // namespace
}  // namespace cpfs
