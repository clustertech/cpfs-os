/* Copyright 2013 ClusterTech Ltd */
#include "listening_socket_impl.hpp"

#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "listening_socket.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"

using ::testing::InSequence;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StrEq;

namespace cpfs {
namespace {

class MockAcceptHandler {
 public:
  MOCK_METHOD1(OnAccepted, void(TcpSocket* sock));
};

class ListeningSocketTest : public ::testing::Test {
 protected:
  IOService service_;
  MockIAsioPolicy policy_;
  TcpAcceptor* acceptor_;
  boost::shared_ptr<IListeningSocket> socket_;
  IOHandler accept_callback_;
  MockAcceptHandler accept_handler_;

  ListeningSocketTest() : socket_(kListeningSocketMaker(&policy_)) {
    // AsyncAccept
    TcpEndpoint endpoint(boost::asio::ip::address::from_string("0.0.0.0"),
                         4001);
    EXPECT_CALL(policy_, MakeAcceptor(_))
        .WillOnce(Return(acceptor_ = new TcpAcceptor(service_, endpoint)));
    EXPECT_CALL(policy_, io_service())
        .WillRepeatedly(Return(&service_));
    EXPECT_CALL(policy_, AsyncAccept(_, _, _))
        .WillOnce(SaveArg<2>(&accept_callback_));

    socket_->Listen(endpoint, boost::bind(&MockAcceptHandler::OnAccepted,
                                          &accept_handler_, _1));
  }
};

TEST_F(ListeningSocketTest, Accepted) {
  // Connected, callback is called, AsyncAccept called again
  EXPECT_CALL(policy_, AsyncAccept(_, _, _))
      .WillOnce(SaveArg<2>(&accept_callback_));
  TcpSocket* sock = 0;
  EXPECT_CALL(accept_handler_, OnAccepted(_))
      .WillOnce(SaveArg<0>(&sock));

  accept_callback_(boost::system::error_code());
  EXPECT_TRUE(boost::shared_ptr<TcpSocket>(sock));
}

TEST_F(ListeningSocketTest, ErrorAccept) {
  // Trigger error, expect error is logged, AsyncAccept called again
  EXPECT_CALL(policy_, AsyncAccept(_, _, _));
  MockLogCallback log_callback;
  LogRoute route(log_callback.GetLogCallback());
  boost::system::error_code ec(boost::asio::error::broken_pipe);
  EXPECT_CALL(
      log_callback,
      Call(PLEVEL(error, Fim),
           StrEq("Error in connection accept: " + ec.message() +
                 ", Bind Address: 0.0.0.0:4001"), _));

  accept_callback_(ec);
}

}  // namespace
}  // namespace cpfs
