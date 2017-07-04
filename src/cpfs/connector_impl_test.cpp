/* Copyright 2013 ClusterTech Ltd */
#include "connector_impl.hpp"

#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "authenticator_mock.hpp"
#include "connecting_socket_mock.hpp"
#include "connector.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "listening_socket_mock.hpp"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {

class IPeriodicTimer;

namespace {

class ConnectHandler {
 public:
  MOCK_METHOD1(OnConnected, void(boost::shared_ptr<IFimSocket>));
};

TEST(ConnectorTest, Connect) {
  IOService service;
  MockIAsioPolicy policy;
  MockIAuthenticator authenticator;
  EXPECT_CALL(policy, io_service())
      .WillRepeatedly(Return(&service));
  MockFimSocketMaker fsm;
  MockConnectingSocketMaker csm;

  const std::string ms1_host = "10.1.1.3";
  int ms1_port = 3000;
  ConnectSuccessHandler dummy_cb;
  ConnectSuccessHandler connected_cb;
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  // Create a new ConnectingSocket to be returned
  boost::shared_ptr<MockIConnectingSocket> conn_socket(
      new MockIConnectingSocket);
  // ConnectingSocketMaker
  EXPECT_CALL(csm, Make(_, _, _)).WillOnce(DoAll(
    SaveArg<2>(&connected_cb),
    Return(conn_socket)));
  // FimSocketMaker
  EXPECT_CALL(fsm, Make(_, &policy))
      .WillOnce(Return(fim_socket));
  boost::shared_ptr<IPeriodicTimer> heartbeat_timer;
  boost::shared_ptr<IPeriodicTimer> idle_timer;
  EXPECT_CALL(*fim_socket, SetHeartbeatTimer(_))
      .WillOnce(SaveArg<0>(&heartbeat_timer));
  EXPECT_CALL(*fim_socket, SetIdleTimer(_))
      .WillOnce(SaveArg<0>(&idle_timer));

  // Test Connector
  boost::scoped_ptr<IConnector> connector(
      MakeConnector(&policy, &authenticator));
  connector->SetFimSocketMaker(fsm.GetMaker());
  connector->SetConnectingSocketMaker(csm.GetMaker());
  connector->set_heartbeat_interval(10.0);
  connector->set_socket_read_timeout(15.0);

  // Try to connect to MS1
  ConnectHandler handler1;
  boost::scoped_ptr<MockIFimProcessor> proc(new MockIFimProcessor);
  EXPECT_CALL(handler1, OnConnected(_));
  EXPECT_CALL(*conn_socket, SetTimeout(0.5, _));
  EXPECT_CALL(*conn_socket, AsyncConnect());

  connector->AsyncConnect(ms1_host, ms1_port, proc.get(),
                          boost::bind(&ConnectHandler::OnConnected,
                                      &handler1, _1),
                          true, 0.5);

  FimSocketCleanupCallback cleanup_callback;
  EXPECT_CALL(*fim_socket, OnCleanup(_))
      .WillOnce(SaveArg<0>(&cleanup_callback));
  AuthHandler auth_handler;
  EXPECT_CALL(authenticator, AsyncAuth(_, _, _))
      .WillOnce(SaveArg<1>(&auth_handler));
  // Trigger connected
  connected_cb(0);

  EXPECT_CALL(*fim_socket, OnCleanup(_));
  EXPECT_CALL(*fim_socket, SetFimProcessor(proc.get()));
  // Trigger authenticated
  auth_handler();

  // Trigger cleanup
  EXPECT_CALL(csm, Make(_, _, _)).WillOnce(DoAll(
    SaveArg<2>(&connected_cb),
    Return(conn_socket)));
  EXPECT_CALL(*conn_socket, SetTimeout(0.5, _));
  EXPECT_CALL(*conn_socket, AsyncConnect());

  cleanup_callback();

  // Cleanup and check
  Mock::VerifyAndClear(&fsm);
  Mock::VerifyAndClear(&csm);
}

TEST(ConnectorTest, Listen) {
  IOService service;
  MockIAsioPolicy policy;
  EXPECT_CALL(policy, io_service())
      .WillRepeatedly(Return(&service));
  MockFimSocketMaker fsm;
  MockListeningSocketMaker lsm;
  MockIFimProcessor fim_processor;
  MockIAuthenticator authenticator;

  // Test Connector
  boost::scoped_ptr<IConnector> connector(
      MakeConnector(&policy, &authenticator));
  connector->SetFimSocketMaker(fsm.GetMaker());
  connector->SetListeningSocketMaker(lsm.GetMaker());
  connector->set_heartbeat_interval(10.0);
  connector->set_socket_read_timeout(15.0);

  // ListeningSocketMaker
  // Create a new ListeningSocket to be returned
  AcceptCallback accept_cb;
  boost::shared_ptr<MockIListeningSocket> listen_socket(
      new MockIListeningSocket);
  EXPECT_CALL(*listen_socket, Listen(_, _))
      .WillOnce(SaveArg<1>(&accept_cb));
  EXPECT_CALL(lsm, Make(_)).WillOnce(Return(listen_socket));
  connector->Listen("127.0.0.1", 12345, &fim_processor);

  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(fsm, Make(_, _))
      .WillOnce(Return(fim_socket));
  EXPECT_CALL(*fim_socket, SetHeartbeatTimer(_));
  EXPECT_CALL(*fim_socket, SetIdleTimer(_));

  AuthHandler auth_handler;
  EXPECT_CALL(authenticator, AsyncAuth(_, _, _))
      .WillOnce(SaveArg<1>(&auth_handler));

  TcpSocket* socket = 0;
  accept_cb(socket);


  EXPECT_CALL(*fim_socket, SetFimProcessor(_));
  // Trigger authenticated
  auth_handler();

  Mock::VerifyAndClear(&lsm);
  Mock::VerifyAndClear(&fsm);
}

}  // namespace
}  // namespace cpfs
