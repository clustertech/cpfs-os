/* Copyright 2013 ClusterTech Ltd */
#include "connecting_socket_impl.hpp"

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "connecting_socket.hpp"
#include "mock_actions.hpp"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::SaveArg;
using ::testing::Return;

namespace cpfs {
namespace {

class ConnectingSocketTest : public ::testing::Test {
 public:
  ConnectingSocketTest() {
    EXPECT_CALL(policy_, io_service())
        .WillRepeatedly(Return(&io_service_));

    socket_ = kConnectingSocketMaker(
        &policy_, endpoint_,
        boost::bind(&ConnectHandler::OnConnected, &handler_, _1));
  }

 protected:
  class ConnectHandler {
   public:
    MOCK_METHOD1(OnConnected, void(TcpSocket* sock));
  };

  class TimeoutHandler {
   public:
    MOCK_METHOD0(OnTimeout, double());
  };

  MockIAsioPolicy policy_;
  IOService io_service_;
  TcpEndpoint endpoint_;
  ConnectHandler handler_;
  boost::shared_ptr<IConnectingSocket> socket_;
};

TEST_F(ConnectingSocketTest, ConnectionNormal) {
  IOHandler handler;
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _))
      .WillOnce(SaveArg<2>(&handler));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));

  socket_->AsyncConnect();

  // Trigger connected
  TcpSocket* socket;
  EXPECT_CALL(handler_, OnConnected(_))
      .WillOnce(SaveArg<0>(&socket));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));

  handler(boost::system::error_code());
  EXPECT_TRUE(socket_->connected());
  EXPECT_TRUE(boost::scoped_ptr<TcpSocket>(socket));
}

TEST_F(ConnectingSocketTest, ConnectionReconnect) {
  IOHandler handler1;
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _))
      .WillOnce(SaveArg<2>(&handler1));
  IOHandler timeout_handler;
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));

  socket_->AsyncConnect();

  // Connection is failed, a timer of 1s will be set for reconnect
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 1, _))
      .WillOnce(SaveArg<2>(&timeout_handler));

  Sleep(0.001)();
  handler1(boost::asio::error::broken_pipe);

  // Reconnect is trigger, AsyncConnect will be called again
  EXPECT_FALSE(socket_->connected());
  IOHandler handler2;
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _))
      .WillOnce(SaveArg<2>(&handler2));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _))
      .WillOnce(SaveArg<2>(&timeout_handler));

  timeout_handler(boost::system::error_code());

  // Trigger connected
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));
  TcpSocket* socket;
  EXPECT_CALL(handler_, OnConnected(_))
      .WillOnce(SaveArg<0>(&socket));

  Sleep(0.001)();
  handler2(boost::system::error_code());
  EXPECT_TRUE(socket_->connected());
  EXPECT_TRUE(boost::shared_ptr<TcpSocket>(socket));
  double total_elapsed = socket_->TotalElapsed();
  EXPECT_GE(total_elapsed - socket_->AttemptElapsed(), 0.001);
  EXPECT_GT(socket_->AttemptElapsed(), 0);
}

TEST_F(ConnectingSocketTest, ConnectionReconnectImmediate) {
  // Set timeout
  TimeoutHandler t_callback;
  socket_->SetTimeout(
      2, boost::bind(&TimeoutHandler::OnTimeout, &t_callback));

  // Connect
  IOHandler timeout_handler;
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 2, _))
      .WillOnce(SaveArg<2>(&timeout_handler));
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _));

  socket_->AsyncConnect();

  // Timeout, immediate retry
  EXPECT_CALL(t_callback, OnTimeout())
      .WillOnce(Return(0));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 2, _))
      .WillOnce(SaveArg<2>(&timeout_handler));
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _));

  timeout_handler(boost::system::error_code());

  // Timeout again, give up
  EXPECT_CALL(t_callback, OnTimeout())
      .WillOnce(Return(-1));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));

  timeout_handler(boost::system::error_code());
}

TEST_F(ConnectingSocketTest, ConnectionTimeout) {
  // Set timeout
  TimeoutHandler t_callback;
  socket_->SetTimeout(
      2, boost::bind(&TimeoutHandler::OnTimeout, &t_callback));

  // Connect
  IOHandler timeout_handler;
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 2, _))
      .WillOnce(SaveArg<2>(&timeout_handler));
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _));

  socket_->AsyncConnect();

  // Timeout
  EXPECT_CALL(t_callback, OnTimeout())
      .WillOnce(Return(3));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 3, _))
      .WillOnce(SaveArg<2>(&timeout_handler));

  timeout_handler(boost::system::error_code());
}

TEST_F(ConnectingSocketTest, ConnectionTimeoutReset) {
  // Set timeout
  TimeoutHandler t_callback;
  socket_->SetTimeout(
      2, boost::bind(&TimeoutHandler::OnTimeout, &t_callback));

  // Connect
  IOHandler timeout_handler;
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 2, _));
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _));

  socket_->AsyncConnect();

  // Set timeout again before timeout
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 4, _))
      .WillOnce(SaveArg<2>(&timeout_handler));

  socket_->SetTimeout(
      4, boost::bind(&TimeoutHandler::OnTimeout, &t_callback));

  // Timeout
  EXPECT_CALL(t_callback, OnTimeout())
      .WillOnce(Return(3));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 3, _))
      .WillOnce(SaveArg<2>(&timeout_handler));

  timeout_handler(boost::system::error_code());
}

TEST_F(ConnectingSocketTest, TimeoutCancelled) {
  // Set timeout
  IOHandler timeout_handler;
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 3, _))
      .WillOnce(SaveArg<2>(&timeout_handler));
  TimeoutHandler t_callback;
  EXPECT_CALL(t_callback, OnTimeout())
      .Times(0);

  socket_->SetTimeout(
      3, boost::bind(&TimeoutHandler::OnTimeout, &t_callback));

  // Connect
  IOHandler handler;
  EXPECT_CALL(policy_, AsyncConnect(_, endpoint_, _))
      .WillOnce(SaveArg<2>(&handler));

  socket_->AsyncConnect();

  // Connected, timeout callback should not be invoked
  EXPECT_CALL(policy_, SetDeadlineTimer(_, 0, _));
  TcpSocket* socket;
  EXPECT_CALL(handler_, OnConnected(_))
      .WillOnce(SaveArg<0>(&socket));

  handler(boost::system::error_code());
  EXPECT_TRUE(boost::shared_ptr<TcpSocket>(socket));
}

}  // namespace
}  // namespace cpfs
