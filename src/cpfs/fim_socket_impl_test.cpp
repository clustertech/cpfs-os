/* Copyright 2013 ClusterTech Ltd */
#include "fim_socket_impl.hpp"

#include <cstring>
#include <stdexcept>
#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "periodic_timer_mock.hpp"
#include "req_tracker_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StartsWith;
using ::testing::Throw;

namespace cpfs {
namespace {

typedef boost::asio::mutable_buffer MIOBuf;
typedef boost::asio::const_buffer CIOBuf;

const double kTimerTolerance = 1.0 / 32;

// Testing Class
class FimSocketTest : public ::testing::Test {
  class ExpectCallTcpNoDelay {
   public:
    ExpectCallTcpNoDelay(MockIAsioPolicy* policy, TcpSocket* socket) {
      EXPECT_CALL(*policy, SetTcpNoDelay(socket));
    }
  };

 protected:
  boost::shared_ptr<MockIReqTracker> req_tracker_;
  IOService io_service_;
  TcpSocket* socket_;
  boost::scoped_ptr<MockIAsioPolicy> policy_;
  ExpectCallTcpNoDelay expect_call_;
  MockIFimProcessor fim_processor_;
  boost::shared_ptr<MockIPeriodicTimer> heartbeat_timer_;
  boost::shared_ptr<MockIPeriodicTimer> idle_timer_;
  boost::function<bool()> heartbeat_cb_;
  boost::function<bool()> idle_cb_;
  MockFunction<void()> cleanup_cb_;
  boost::shared_ptr<IFimSocket> fim_socket_;
  bool shutdown_prepared_;

  FimSocketTest()
      : req_tracker_(new MockIReqTracker()),
        socket_(new TcpSocket(io_service_)),
        policy_(new MockIAsioPolicy),
        expect_call_(policy_.get(), socket_),
        heartbeat_timer_(new MockIPeriodicTimer),
        idle_timer_(new MockIPeriodicTimer),
        fim_socket_(kFimSocketMaker(socket_, policy_.get())),
        shutdown_prepared_(false) {
    fim_socket_->SetFimProcessor(&fim_processor_);
    fim_socket_->SetReqTracker(req_tracker_);
    fim_socket_->SetReqTracker(req_tracker_);
    EXPECT_CALL(*req_tracker_, name())
        .WillRepeatedly(Return("Peer"));
    EXPECT_CALL(*heartbeat_timer_, OnTimeout(_)).WillOnce(
        SaveArg<0>(&heartbeat_cb_));
    fim_socket_->SetHeartbeatTimer(heartbeat_timer_);
    EXPECT_CALL(*idle_timer_, OnTimeout(_)).WillOnce(
        SaveArg<0>(&idle_cb_));
    fim_socket_->SetIdleTimer(idle_timer_);
    fim_socket_->OnCleanup(boost::bind(GetMockCall(cleanup_cb_), &cleanup_cb_));
  }

  ~FimSocketTest() {
    Mock::VerifyAndClear(req_tracker_.get());  // Remove fim_socket_ ref
    EXPECT_CALL(*req_tracker_, name())
        .WillRepeatedly(Return("Peer"));
    fim_socket_->Reset();
    fim_socket_.reset();
  }

  void PrepareSocketShutdown() {
    if (shutdown_prepared_)
      return;
    shutdown_prepared_ = true;
    EXPECT_CALL(cleanup_cb_, Call());
    EXPECT_CALL(*req_tracker_, GetFimSocket())
        .WillOnce(Return(fim_socket_));
    EXPECT_CALL(*req_tracker_, SetFimSocket(
        boost::shared_ptr<IFimSocket>(), true));
  }

  template<typename T>
  void FillBuf(MIOBuf* rbuf, const T& to_fill) {
    *reinterpret_cast<T*>(boost::asio::buffer_cast<char*>(*rbuf)) = to_fill;
  }
};

TEST_F(FimSocketTest, Empty) {
  // We need to be able to create empty FimSocket, and operations on it
  // should have no effect.
  fim_socket_ = kFimSocketMaker(0, policy_.get());
  fim_socket_->StartRead();
  FIM_PTR<ResultCodeReplyFim> fim1(new ResultCodeReplyFim);
  (*fim1)->err_no = 10;
  fim_socket_->WriteMsg(fim1);
  fim_socket_->Shutdown();
  EXPECT_TRUE(fim_socket_->ShutdownCalled());
}

TEST_F(FimSocketTest, Getters) {
  EXPECT_EQ(policy_.get(), fim_socket_->asio_policy());
  EXPECT_EQ(socket_, fim_socket_->socket());
  EXPECT_EQ(req_tracker_.get(), fim_socket_->GetReqTracker());

  EXPECT_EQ(std::string("Peer"),
            boost::lexical_cast<std::string>(*fim_socket_));

  TcpSocket* socket2 = new TcpSocket(io_service_);
  EXPECT_CALL(*policy_, SetTcpNoDelay(socket2));
  EXPECT_CALL(*policy_, GetRemoteEndpoint(socket2))
      .WillRepeatedly(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("10.0.0.1"), 4001)));

  boost::shared_ptr<IFimSocket> fim_socket2(
      kFimSocketMaker(socket2, policy_.get()));
  EXPECT_FALSE(fim_socket2->GetReqTracker());
  EXPECT_EQ(std::string("10.0.0.1:4001"),
            boost::lexical_cast<std::string>(*fim_socket2));

  EXPECT_CALL(*policy_, GetRemoteEndpoint(socket2))
      .WillOnce(Throw(std::runtime_error("error")));
  EXPECT_EQ(std::string("Unconnected FIM socket"),
            boost::lexical_cast<std::string>(*fim_socket2));
}

// Test reading two FIMs
TEST_F(FimSocketTest, Read) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());

  // Start read
  MIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  fim_socket_->StartRead();

  // Read header
  MIOBuf buf2;
  SIOHandler handler2;
  EXPECT_CALL(*idle_timer_, Reset(kTimerTolerance));
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf2),
                      SaveArg<2>(&handler2)));

  ResultCodeReplyFim fim_a;
  FillBuf(&buf, *(reinterpret_cast<const FimHead*>(fim_a.msg())));
  handler(boost::system::error_code(), 1);

  // Read body
  MIOBuf buf3;
  SIOHandler handler3;
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf3),
                      SaveArg<2>(&handler3)));
  EXPECT_CALL(fim_processor_, Process(_, _))
      .WillOnce(Return(true));

  FillBuf(&buf2,
          *(reinterpret_cast<const ResultCodeReplyFimPart*>(fim_a.body())));
  handler2(boost::system::error_code(), 1);

  // Read empty Fim
  EXPECT_CALL(*idle_timer_, Reset(kTimerTolerance));
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _));
  EXPECT_CALL(fim_processor_, Process(_, _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Fim),
                   StartsWith("FIM processing: Uninteresting"), _));

  HeartbeatFim fim_b;
  FillBuf(&buf3, *(reinterpret_cast<const FimHead*>(fim_b.msg())));
  handler3(boost::system::error_code(), 1);
}

class FimProcessorChanger {
 public:
  FimProcessorChanger(boost::shared_ptr<IFimSocket> fim_socket,
                      IFimProcessor* fim_processor,
                      boost::shared_ptr<IReqTracker> req_tracker)
      : fim_socket_(fim_socket),
        fim_processor_(fim_processor),
        req_tracker_(req_tracker) {}

  bool operator() () {
    fim_socket_->SetFimProcessor(fim_processor_);
    fim_socket_->SetReqTracker(req_tracker_);
    return true;
  }

 private:
  boost::shared_ptr<IFimSocket> fim_socket_;
  IFimProcessor* fim_processor_;
  boost::shared_ptr<IReqTracker> req_tracker_;
};

TEST_F(FimSocketTest, ChangeProcessor) {
  // Start reading
  MIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  fim_socket_->StartRead();

  // Read header, no body  (Heartbeat body is void)
  MIOBuf buf2;
  SIOHandler handler2;
  EXPECT_CALL(*idle_timer_, Reset(kTimerTolerance));
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf2),
                      SaveArg<2>(&handler2)));
  MockIFimProcessor new_processor;
  EXPECT_CALL(fim_processor_, Process(_, _))
      .WillOnce(InvokeWithoutArgs(
          FimProcessorChanger(fim_socket_, &new_processor, req_tracker_)));

  HeartbeatFim fim_b;
  FillBuf(&buf, *(reinterpret_cast<const FimHead*>(fim_b.msg())));
  handler(boost::system::error_code(), 1);

  // Read another fim, should be processed in new_processor
  FillBuf(&buf2, *(reinterpret_cast<const FimHead*>(fim_b.msg())));
  EXPECT_CALL(*idle_timer_, Reset(kTimerTolerance));
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _));
  // The new callback handler is called
  EXPECT_CALL(new_processor, Process(_, _));

  handler2(boost::system::error_code(), 1);

  Mock::VerifyAndClear(&fim_processor_);
}

// When unknown FIM is received, error is logged and read continues
TEST_F(FimSocketTest, ReadHeaderError) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  // Start reading
  MIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  fim_socket_->StartRead();

  // Error on reading header
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Fim),
                   StartsWith("FIM header: corrupted FIM received from "), _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StartsWith("Peer disconnected"), _));
  PrepareSocketShutdown();

  handler(boost::asio::error::broken_pipe, 0);
}

TEST_F(FimSocketTest, ReadCorruptedHeader) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  // Start reading
  MIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));
  fim_socket_->StartRead();
  // Read header gets corrupted header
  PrepareSocketShutdown();
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Fim),
                   StartsWith("FIM header: failed interpreting FIM"), _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StartsWith("Peer disconnected"), _));

  FimHead corrupted_head;
  std::memset(&corrupted_head, '\0', sizeof(corrupted_head));
  FillBuf(&buf, corrupted_head);
  handler(boost::system::error_code(), 1);
}

TEST_F(FimSocketTest, ReadBodyError) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  // Start reading
  MIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  fim_socket_->StartRead();
  // Error on reading body
  MIOBuf buf2;
  SIOHandler handler2;
  EXPECT_CALL(*idle_timer_, Reset(kTimerTolerance));
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf2),
                      SaveArg<2>(&handler2)));
  ResultCodeReplyFim fim_a;
  fim_a->err_no = 10;
  FillBuf(&buf, *(reinterpret_cast<const FimHead*>(fim_a.msg())));
  handler(boost::system::error_code(), 1);
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Fim),
                    StartsWith("FIM body: failed interpreting FIM"), _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(notice, Server),
                   StartsWith("Peer disconnected"), _));
  PrepareSocketShutdown();
  handler2(boost::system::error_code(boost::asio::error::broken_pipe), 0);
}

TEST_F(FimSocketTest, ReadFimProcessorError) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());

  // Start reading
  MIOBuf buf[2];
  SIOHandler handler[2];
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf[0]),
                      SaveArg<2>(&handler[0])));

  fim_socket_->StartRead();
  // Read body succussfully, but throw on processor
  HeartbeatFim fim_c;
  EXPECT_CALL(*idle_timer_, Reset(kTimerTolerance));
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf[1]),
                      SaveArg<2>(&handler[1])));
  EXPECT_CALL(fim_processor_, Process(_, _))
      .WillOnce(Throw(std::runtime_error("Exception thrown")));
  EXPECT_CALL(mock_log_callback,
      Call(PLEVEL(warning, Fim),
           StartsWith("FIM processing: Failed processing "
                      "#Q00000-000000(Heartbeat)"),
                      _));

  FillBuf(&buf[0], *(reinterpret_cast<const FimHead*>(fim_c.msg())));
  handler[0](boost::system::error_code(), 1);
}

// Test reading two FIMs
TEST_F(FimSocketTest, ReadNoProcessor) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());

  // Empty the processor
  fim_socket_->SetFimProcessor(0);

  // Start read
  MIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  fim_socket_->StartRead();

  // Read empty Fim
  EXPECT_CALL(*idle_timer_, Reset(kTimerTolerance));
  EXPECT_CALL(*policy_, AsyncRead(socket_, _, _));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Fim),
                   StartsWith("FIM processing: No processor for"),
                   _));

  HeartbeatFim fim_b;
  FillBuf(&buf, *(reinterpret_cast<const FimHead*>(fim_b.msg())));
  handler(boost::system::error_code(), 1);
}

TEST_F(FimSocketTest, Write) {
  // No fim is queued yet
  EXPECT_FALSE(fim_socket_->pending_write());

  // Write a Fim
  CIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncWrite(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  FIM_PTR<ResultCodeReplyFim> fim1(new ResultCodeReplyFim);
  (*fim1)->err_no = 10;
  fim_socket_->WriteMsg(fim1);
  EXPECT_TRUE(fim_socket_->pending_write());
  // Queued
  FIM_PTR<ResultCodeReplyFim> fim2(new ResultCodeReplyFim);
  (*fim2)->err_no = 20;
  fim_socket_->WriteMsg(fim2);
  EXPECT_TRUE(fim_socket_->pending_write());

  // Once fim1 is trigger, AsyncWrite is invoked for fim2
  EXPECT_CALL(*policy_, AsyncWrite(_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  // Trigger fim1
  EXPECT_CALL(*heartbeat_timer_, Reset(kTimerTolerance));
  handler(boost::system::error_code(), 1);
  // Trigger fim2
  EXPECT_CALL(*heartbeat_timer_, Reset(kTimerTolerance));
  handler(boost::system::error_code(), 1);
  // All fims are sent
  EXPECT_FALSE(fim_socket_->pending_write());

  // Write completion has not effect after Shutdown()
  EXPECT_CALL(*policy_, AsyncWrite(socket_, _, _))
      .WillOnce(SaveArg<2>(&handler));

  fim_socket_->WriteMsg(fim1);
  fim_socket_->WriteMsg(fim2);

  PrepareSocketShutdown();

  EXPECT_FALSE(fim_socket_->ShutdownCalled());
  fim_socket_->Shutdown();
  EXPECT_TRUE(fim_socket_->ShutdownCalled());
  handler(boost::system::error_code(), 0);
}

TEST_F(FimSocketTest, WriteError) {
  // Write a Fim
  CIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncWrite(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));

  FIM_PTR<ResultCodeReplyFim> fim1(new ResultCodeReplyFim);
  (*fim1)->err_no = 10;
  fim_socket_->WriteMsg(fim1);

  PrepareSocketShutdown();
  handler(boost::system::error_code(boost::asio::error::broken_pipe), 0);
}

TEST_F(FimSocketTest, HeartbeatGeneration) {
  // Write a Fim
  CIOBuf buf;
  SIOHandler handler;
  EXPECT_CALL(*policy_, AsyncWrite(socket_, _, _))
      .WillOnce(DoAll(SaveArg<1>(&buf),
                      SaveArg<2>(&handler)));
  heartbeat_cb_();
}

TEST_F(FimSocketTest, HeartbeatLoss) {
  PrepareSocketShutdown();
  idle_cb_();
}

TEST_F(FimSocketTest, Migrate) {
  IOService io_service2;
  boost::scoped_ptr<MockIAsioPolicy> policy2(new MockIAsioPolicy);
  EXPECT_CALL(*policy2, io_service())
      .WillRepeatedly(Return(&io_service2));
  boost::shared_ptr<MockIPeriodicTimer>
      heartbeat_timer2(new MockIPeriodicTimer);
  EXPECT_CALL(*heartbeat_timer_, Duplicate(&io_service2))
      .WillOnce(Return(heartbeat_timer2));
  EXPECT_CALL(*heartbeat_timer_, Stop());
  boost::shared_ptr<MockIPeriodicTimer> idle_timer2(new MockIPeriodicTimer);
  EXPECT_CALL(*idle_timer_, Duplicate(&io_service2))
      .WillOnce(Return(idle_timer2));
  EXPECT_CALL(*idle_timer_, Stop());
  TcpSocket* socket2 = new TcpSocket(io_service2);
  EXPECT_CALL(*policy2, DuplicateSocket(socket_))
      .WillOnce(Return(socket2));

  fim_socket_->Migrate(policy2.get());
  EXPECT_EQ(socket2, fim_socket_->socket());

  Mock::VerifyAndClear(heartbeat_timer_.get());
  Mock::VerifyAndClear(idle_timer_.get());
}

TEST_F(FimSocketTest, Shutdown) {
  PrepareSocketShutdown();
  fim_socket_->Shutdown();
  // Various operations check shutdown_ flag
  fim_socket_->Shutdown();
  fim_socket_->StartRead();
  fim_socket_->WriteMsg(ResultCodeReplyFim::MakePtr());
}

}  // namespace
}  // namespace cpfs
