/* Copyright 2014 ClusterTech Ltd */
#include "authenticator_impl.hpp"

#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "authenticator.hpp"
#include "crypto_mock.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "periodic_timer_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StartsWith;
using ::testing::Throw;
using ::testing::WithArg;

namespace cpfs {
namespace {

class AuthenticatorTest : public ::testing::Test {
 protected:
  MockICrypto* crypto_;
  IOService io_service_;
  MockPeriodicTimerMaker timer_maker_;
  boost::shared_ptr<MockIPeriodicTimer> timer_;
  boost::scoped_ptr<TcpSocket> tcp_socket_;
  boost::scoped_ptr<MockIAsioPolicy> asio_policy_;
  boost::scoped_ptr<IAuthenticator> authenticator_;
  boost::function<void()> timeout_cb_;

  AuthenticatorTest()
      : timer_(new MockIPeriodicTimer),
        tcp_socket_(new TcpSocket(io_service_)),
        asio_policy_(new MockIAsioPolicy),
        authenticator_(
            MakeAuthenticator(crypto_ = new MockICrypto, asio_policy_.get())) {
    EXPECT_CALL(timer_maker_, Make(_, _))
        .WillOnce(Return(timer_));
    authenticator_->SetPeriodicTimerMaker(timer_maker_.GetMaker(), 1);
    EXPECT_CALL(*timer_, OnTimeout(_))
        .WillOnce(SaveArg<0>(&timeout_cb_));
    EXPECT_CALL(*asio_policy_, io_service())
        .WillOnce(Return(&io_service_));
    setenv("CPFS_KEY_PATH", ".", 1);
    authenticator_->Init();
  }

  ~AuthenticatorTest() {
    Mock::VerifyAndClear(&timer_maker_);
  }
};

ACTION_P(StrCpyToArg, str) { std::strncpy(arg0, str, 20); }

TEST_F(AuthenticatorTest, InitFailure) {
  EXPECT_CALL(timer_maker_, Make(_, _))
      .WillOnce(Return(timer_));
  authenticator_->SetPeriodicTimerMaker(timer_maker_.GetMaker(), 1);
  EXPECT_CALL(*timer_, OnTimeout(_));
  setenv("CPFS_KEY_PATH", "/not_exist", 1);
  EXPECT_CALL(*asio_policy_, io_service())
      .WillOnce(Return(&io_service_));
  EXPECT_THROW(authenticator_->Init(), std::runtime_error);
}

TEST_F(AuthenticatorTest, Connecting) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  IFimProcessor* processor;

  EXPECT_CALL(*crypto_, CreateNonce())
      .WillOnce(Return(1234));
  EXPECT_CALL(*fim_socket, SetFimProcessor(_))
      .WillOnce(SaveArg<0>(&processor));
  EXPECT_CALL(*fim_socket, StartRead());
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  EXPECT_CALL(*fim_socket, socket())
      .WillRepeatedly(Return(tcp_socket_.get()));
  EXPECT_CALL(*asio_policy_, GetLocalEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("127.0.0.1"), 4001)));

  MockFunction<void()> auth_handler;
  authenticator_->AsyncAuth(fim_socket, boost::bind(GetMockCall(auth_handler),
                                                    &auth_handler), true);

  EXPECT_CALL(*asio_policy_, GetRemoteEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("10.0.0.1"), 4001)));
  // AuthMsg2Fim
  EXPECT_CALL(*crypto_, Sign(_, _, _))
      .WillOnce(WithArg<2>(StrCpyToArg("EE56781234EEEEEEEEEE")))
      .WillOnce(WithArg<2>(StrCpyToArg("EE12345678EEEEEEEEEE")));
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  FIM_PTR<AuthMsg2Fim> fim1 = AuthMsg2Fim::MakePtr(20);
  (*fim1)->nonce = 5678;
  std::strncpy(fim1->tail_buf(), "EE56781234EEEEEEEEEE", 20);
  EXPECT_CALL(auth_handler, Call());

  // Actual call
  processor->Process(fim1, fim_socket);
}

TEST_F(AuthenticatorTest, ConnectingCorrupted) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  IFimProcessor* processor;

  EXPECT_CALL(*crypto_, CreateNonce())
      .WillOnce(Return(1234));
  EXPECT_CALL(*fim_socket, SetFimProcessor(_))
      .WillOnce(SaveArg<0>(&processor));
  EXPECT_CALL(*fim_socket, StartRead());
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  EXPECT_CALL(*fim_socket, socket())
      .WillRepeatedly(Return(tcp_socket_.get()));
  EXPECT_CALL(*asio_policy_, GetLocalEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("127.0.0.1"), 4001)));

  MockFunction<void()> auth_handler;
  authenticator_->AsyncAuth(fim_socket, boost::bind(GetMockCall(auth_handler),
                                                    &auth_handler), true);

  EXPECT_CALL(*asio_policy_, GetRemoteEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("10.0.0.1"), 4001)));
  // Tail buffer size mismatch
  EXPECT_CALL(*crypto_, Sign(_, _, _))
      .WillOnce(WithArg<2>(StrCpyToArg("EE56781234EEEEEEEEEE")));
  FIM_PTR<AuthMsg2Fim> err_fim = AuthMsg2Fim::MakePtr(4);
  (*err_fim)->nonce = 5678;
  std::strncpy(err_fim->tail_buf(), "TEST", 4);
  // Error message
  EXPECT_CALL(*fim_socket, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*fim_socket, Shutdown());
  // Actual call
  processor->Process(err_fim, fim_socket);
}

TEST_F(AuthenticatorTest, Listening) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  IFimProcessor* processor;

  EXPECT_CALL(*crypto_, CreateNonce())
      .WillOnce(Return(5678));
  EXPECT_CALL(*fim_socket, SetFimProcessor(_))
      .WillOnce(SaveArg<0>(&processor));
  EXPECT_CALL(*fim_socket, StartRead());
  EXPECT_CALL(*fim_socket, socket())
      .WillRepeatedly(Return(tcp_socket_.get()));
  EXPECT_CALL(*asio_policy_, GetLocalEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("127.0.0.1"), 4001)));

  MockFunction<void()> auth_handler;
  authenticator_->AsyncAuth(fim_socket, boost::bind(GetMockCall(auth_handler),
                                                    &auth_handler), false);

  // AuthMsg1Fim
  EXPECT_CALL(*asio_policy_, GetRemoteEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("10.0.0.1"), 4001)));
  FIM_PTR<AuthMsg1Fim> fim1 = AuthMsg1Fim::MakePtr();
  (*fim1)->nonce = 1234;
  EXPECT_CALL(*crypto_, Sign(_, _, _));
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  // Actual call
  processor->Process(fim1, fim_socket);

  // AuthMsg3Fim
  EXPECT_CALL(*crypto_, Sign(_, _, _))
     .WillOnce(WithArg<2>(StrCpyToArg("EE12345678EEEEEEEEEE")));
  EXPECT_CALL(auth_handler, Call());
  FIM_PTR<AuthMsg3Fim> fim2 = AuthMsg3Fim::MakePtr(20);
  std::strncpy(fim2->tail_buf(), "EE12345678EEEEEEEEEE", 20);
  // Actual call
  processor->Process(fim2, fim_socket);
}

TEST_F(AuthenticatorTest, ListeningCorrupted) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  IFimProcessor* processor;

  EXPECT_CALL(*crypto_, CreateNonce())
      .WillOnce(Return(5678));
  EXPECT_CALL(*fim_socket, SetFimProcessor(_))
      .WillOnce(SaveArg<0>(&processor));
  EXPECT_CALL(*fim_socket, StartRead());
  EXPECT_CALL(*fim_socket, socket())
      .WillRepeatedly(Return(tcp_socket_.get()));
  EXPECT_CALL(*asio_policy_, GetLocalEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("127.0.0.1"), 4001)));

  MockFunction<void()> auth_handler;
  authenticator_->AsyncAuth(fim_socket, boost::bind(GetMockCall(auth_handler),
                                                    &auth_handler), false);

  EXPECT_CALL(*asio_policy_, GetRemoteEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("10.0.0.1"), 4001)));
  // AuthMsg1Fim: Echo the same nonce
  FIM_PTR<AuthMsg1Fim> fim1 = AuthMsg1Fim::MakePtr();
  (*fim1)->nonce = 5678;
  EXPECT_CALL(*crypto_, Sign(_, _, _));
  EXPECT_CALL(*fim_socket, WriteMsg(_));
  // Actual call
  processor->Process(fim1, fim_socket);

  // AuthMsg3Fim
  EXPECT_CALL(*crypto_, Sign(_, _, _));
  processor->Process(AuthMsg3Fim::MakePtr(), fim_socket);
}

TEST_F(AuthenticatorTest, Expire) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);

  EXPECT_CALL(*crypto_, CreateNonce());
  EXPECT_CALL(*fim_socket, SetFimProcessor(_));
  EXPECT_CALL(*fim_socket, StartRead());
  EXPECT_CALL(*fim_socket, socket())
      .WillRepeatedly(Return(tcp_socket_.get()));
  EXPECT_CALL(*asio_policy_, GetLocalEndpoint(_))
      .WillOnce(Return(TcpEndpoint(
          boost::asio::ip::address::from_string("127.0.0.1"), 4001)));

  // Start and expire immediately
  MockFunction<void()> auth_handler;
  authenticator_->AsyncAuth(fim_socket, boost::bind(GetMockCall(auth_handler),
                                                    &auth_handler), false);

  EXPECT_CALL(*fim_socket, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));
  EXPECT_CALL(*fim_socket, Shutdown());
  timeout_cb_();
  Sleep(1)();
  timeout_cb_();
}

}  // namespace
}  // namespace cpfs
