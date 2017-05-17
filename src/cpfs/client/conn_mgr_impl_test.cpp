/* Copyright 2013 ClusterTech Ltd */
#include "client/conn_mgr_impl.hpp"

#include <string>
#include <vector>

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "connecting_socket.hpp"
#include "connector_mock.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "req_tracker_mock.hpp"
#include "service_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "util.hpp"
#include "client/base_client.hpp"
#include "client/cache_mock.hpp"
#include "client/conn_mgr.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::SaveArg;
using ::testing::Return;
using ::testing::Mock;

namespace cpfs {
namespace client {
namespace {

class ClientConnMgrTest : public ::testing::Test {
 protected:
  BaseFSClient client_;
  IOService service_;
  MockIAsioPolicy* asio_policy_;
  MockIConnector* conn_;
  MockITrackerMapper* tracker_mapper_;
  MockIFimProcessor* ms_processor_;
  MockIFimProcessor* ds_processor_;
  MockICacheMgr* cache_mgr_;
  IConnMgr* conn_mgr_;
  MockIService* fso_runner_;

  ClientConnMgrTest() {
    client_.set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    EXPECT_CALL(*asio_policy_, io_service())
        .WillRepeatedly(Return(&service_));
    client_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    client_.set_connector(conn_ = new MockIConnector);
    client_.set_ms_fim_processor(ms_processor_ = new MockIFimProcessor);
    client_.set_ds_fim_processor(ds_processor_ = new MockIFimProcessor);
    client_.set_cache_mgr(cache_mgr_ = new MockICacheMgr);
    client_.set_conn_mgr(conn_mgr_ = MakeConnMgr(&client_, 'F'));
    client_.set_runner(fso_runner_ = new MockIService);
  }
};

TEST_F(ClientConnMgrTest, MSConnect) {
  // Connect MS
  DeadlineTimer* timers[2] = {
    new DeadlineTimer(service_),
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]))
      .WillOnce(Return(timers[1]));
  FimSocketCreatedHandler conn_ms_handler[2];
  EXPECT_CALL(*conn_, AsyncConnect("127.0.0.1", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler[0]));
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.4", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler[1]));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("localhost:3000");
  meta_servers.push_back("10.1.1.4:3000");
  conn_mgr_->Init(meta_servers);

  // Connection complete
  boost::shared_ptr<MockIFimSocket> ms_sock(new MockIFimSocket);
  FIM_PTR<IFim> conn_fim;
  EXPECT_CALL(*ms_sock, WriteMsg(_))
      .WillOnce(SaveArg<0>(&conn_fim));

  client_.set_client_num(5);
  conn_ms_handler[0](ms_sock);
  FCMSRegFim& reg_fim = dynamic_cast<FCMSRegFim&>(*conn_fim);
  EXPECT_EQ('\x0', reg_fim->is_reconnect);
  EXPECT_EQ(5U, reg_fim->client_num);
}

TEST_F(ClientConnMgrTest, MSConnectSuccess) {
  // Connect MS
  DeadlineTimer* timers[1] = {
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]));
  FimSocketCreatedHandler conn_ms_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("10.1.1.3:3000");
  conn_mgr_->Init(meta_servers);

  // Connection complete
  boost::shared_ptr<MockIFimSocket> ms_sock(new MockIFimSocket);
  EXPECT_CALL(*ms_sock, WriteMsg(_));

  client_.set_client_num(5);
  conn_ms_handler(ms_sock);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ClientConnMgrTest, MSConnectRejected) {
  // Connect MS
  DeadlineTimer* timers[1] = {
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]));
  FimSocketCreatedHandler conn_ms_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("10.1.1.3:3000");
  conn_mgr_->Init(meta_servers);

  // Connection complete
  boost::shared_ptr<MockIFimSocket> ms_sock(new MockIFimSocket);
  EXPECT_CALL(*ms_sock, WriteMsg(_));

  client_.set_client_num(5);
  conn_ms_handler(ms_sock);

  // Disconnect
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));
  IOHandler handler;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timers[0], 5.0, _))
      .WillOnce(SaveArg<2>(&handler));

  conn_mgr_->ForgetMS(ms_sock, true);

  std::vector<bool> reject_info = conn_mgr_->GetMSRejectInfo();
  EXPECT_TRUE(reject_info[0]);

  // Try connect again after timeout
  FimSocketCreatedHandler reconnect_ms_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&reconnect_ms_handler));
  handler(boost::system::error_code());

  // Force start
  conn_mgr_->SetForceStartMS(0);
  FIM_PTR<IFim> conn_fim;
  EXPECT_CALL(*ms_sock, WriteMsg(_))
      .WillOnce(SaveArg<0>(&conn_fim));
  client_.set_client_num(5);
  reconnect_ms_handler(ms_sock);

  FCMSRegFim& reg_fim = dynamic_cast<FCMSRegFim&>(*conn_fim);
  EXPECT_TRUE(reg_fim->force_start);
}

TEST_F(ClientConnMgrTest, MSReconnectConnecting) {
  // Connect MS
  DeadlineTimer* timers[2] = {
    new DeadlineTimer(service_),
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]))
      .WillOnce(Return(timers[1]));
  FimSocketCreatedHandler conn_ms_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler));
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.4", 3000, ms_processor_, _,
                                   true, _, _, 0));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("10.1.1.3:3000");
  meta_servers.push_back("10.1.1.4:3000");
  conn_mgr_->Init(meta_servers);

  // Connection complete
  boost::shared_ptr<MockIFimSocket> ms_sock(new MockIFimSocket);
  EXPECT_CALL(*ms_sock, WriteMsg(_));

  client_.set_client_num(5);
  conn_ms_handler(ms_sock);

  // Reconnect at this point is skipped because the other is connecting already
  conn_mgr_->ReconnectMS(ms_sock);

  EXPECT_TRUE(conn_mgr_->IsReconnectingMS());
  conn_mgr_->SetReconnectingMS(false);
  EXPECT_FALSE(conn_mgr_->IsReconnectingMS());
}

TEST_F(ClientConnMgrTest, MSReconnectFailed) {
  // Connect MS
  DeadlineTimer* timers[2] = {
    new DeadlineTimer(service_),
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]))
      .WillOnce(Return(timers[1]));
  FimSocketCreatedHandler conn_ms_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler));
  ConnectErrorHandler conn_err_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.4", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<6>(&conn_err_handler));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("10.1.1.3:3000");
  meta_servers.push_back("10.1.1.4:3000");
  conn_mgr_->Init(meta_servers);

  // Connection complete
  boost::shared_ptr<MockIFimSocket> ms_sock(new MockIFimSocket);
  EXPECT_CALL(*ms_sock, WriteMsg(_));

  client_.set_client_num(5);
  conn_ms_handler(ms_sock);

  // The other connection stopped
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_sock));

  conn_err_handler(boost::shared_ptr<IConnectingSocket>(),
                    boost::system::error_code());

  // Reconnect
  FimSocketCreatedHandler conn_ms_handler_2;
  ConnectErrorHandler error_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.4", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(DoAll(SaveArg<3>(&conn_ms_handler_2),
                      SaveArg<6>(&error_handler)));

  conn_mgr_->ReconnectMS(ms_sock);

  EXPECT_TRUE(conn_mgr_->IsReconnectingMS());

  // Error handler will ask for reconnect only if not already connected
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));

  boost::shared_ptr<IConnectingSocket> c_socket;
  EXPECT_EQ(5.0, error_handler(c_socket, boost::asio::error::timed_out));

  boost::shared_ptr<IFimSocket> ms_fim_sock(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_fim_sock));

  EXPECT_EQ(-1.0, error_handler(c_socket, boost::asio::error::timed_out));

  // Connection complete
  boost::shared_ptr<MockIFimSocket> ms_sock2(new MockIFimSocket);
  EXPECT_CALL(*ms_sock2, WriteMsg(_));

  conn_ms_handler_2(ms_sock2);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ClientConnMgrTest, MSReconnectAbort) {
  // Set reconnect once
  conn_mgr_->SetInitConnRetry(1);

  DeadlineTimer* timers[2] = {
    new DeadlineTimer(service_),
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]))
      .WillOnce(Return(timers[1]));
  ConnectErrorHandler conn_err_handlers[2];
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<6>(&conn_err_handlers[0]));
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.4", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<6>(&conn_err_handlers[1]));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("10.1.1.3:3000");
  meta_servers.push_back("10.1.1.4:3000");
  conn_mgr_->Init(meta_servers);

  boost::shared_ptr<MockIFimSocket> ms_sock;
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_sock));

  // Trigger connect error handler: reconnect
  EXPECT_EQ(5.0, conn_err_handlers[0](
      boost::shared_ptr<IConnectingSocket>(), boost::system::error_code()));
  EXPECT_EQ(5.0, conn_err_handlers[1](
      boost::shared_ptr<IConnectingSocket>(), boost::system::error_code()));

  EXPECT_CALL(*tracker_mapper_, SetClientNum(0)).Times(2);
  EXPECT_CALL(*tracker_mapper_, Shutdown()).Times(2);
  EXPECT_CALL(*fso_runner_, Shutdown()).Times(2);

  // Trigger connect error handler: aborts. SetInitConnRetry=1
  EXPECT_EQ(-1.0, conn_err_handlers[0](
      boost::shared_ptr<IConnectingSocket>(), boost::system::error_code()));
  EXPECT_EQ(-1.0, conn_err_handlers[1](
      boost::shared_ptr<IConnectingSocket>(), boost::system::error_code()));

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ClientConnMgrTest, MSReconnectRejected) {
  // Connect MS
  DeadlineTimer* timers[2] = {
    new DeadlineTimer(service_),
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]))
      .WillOnce(Return(timers[1]));
  FimSocketCreatedHandler conn_ms_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler));
  FimSocketCreatedHandler conn_ms_handler2;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.4", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler2));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("10.1.1.3:3000");
  meta_servers.push_back("10.1.1.4:3000");
  conn_mgr_->Init(meta_servers);

  // Connection complete for both
  boost::shared_ptr<MockIFimSocket> ms_sock(new MockIFimSocket);
  EXPECT_CALL(*ms_sock, WriteMsg(_));

  client_.set_client_num(5);
  conn_ms_handler(ms_sock);

  boost::shared_ptr<MockIFimSocket> ms_sock2(new MockIFimSocket);
  EXPECT_CALL(*ms_sock2, WriteMsg(_));

  conn_ms_handler2(ms_sock2);

  // The other connection stopped
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_sock));

  conn_mgr_->ForgetMS(ms_sock2, true);

  // Reconnect
  FimSocketCreatedHandler conn_ms_handler_2;
  ConnectErrorHandler error_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.4", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(DoAll(SaveArg<3>(&conn_ms_handler_2),
                      SaveArg<6>(&error_handler)));

  conn_mgr_->ReconnectMS(ms_sock);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ClientConnMgrTest, DSConnect) {
  // Connect MS
  DeadlineTimer* timers[1] = {
    new DeadlineTimer(service_),
  };
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timers[0]));
  FimSocketCreatedHandler conn_ms_handler;
  EXPECT_CALL(*conn_, AsyncConnect("10.1.1.3", 3000, ms_processor_, _,
                                   true, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ms_handler));

  std::vector<std::string> meta_servers;
  meta_servers.push_back("10.1.1.3:3000");
  conn_mgr_->Init(meta_servers);

  // Connection complete
  boost::shared_ptr<MockIFimSocket> ms_sock(new MockIFimSocket);
  EXPECT_CALL(*ms_sock, WriteMsg(_));

  client_.set_client_num(5);
  conn_ms_handler(ms_sock);

  // Connect existing DS
  boost::shared_ptr<IFimSocket> ds_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 0))
      .WillOnce(Return(ds_socket));

  conn_mgr_->ConnectDS(IPToInt("192.168.1.101"), 5000, 0, 0);

  // Connect DS
  FimSocketCreatedHandler conn_ds_handler;
  EXPECT_CALL(*conn_, AsyncConnect("192.168.1.101", 5000, ds_processor_,
                                   _, false, _, _, 0))
      .WillOnce(SaveArg<3>(&conn_ds_handler));
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(0, 1))
      .WillOnce(Return(boost::shared_ptr<MockIFimSocket>()));

  conn_mgr_->ConnectDS(IPToInt("192.168.1.101"), 5000, 0, 1);

  // Connection accepted
  boost::shared_ptr<MockIFimSocket> ds_sock(new MockIFimSocket);
  FIM_PTR<IFim> ds_conn_fim;
  EXPECT_CALL(*ds_sock, WriteMsg(_))
      .WillOnce(SaveArg<0>(&ds_conn_fim));
  EXPECT_CALL(*tracker_mapper_, SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(ds_sock), 0, 1, false));
  boost::shared_ptr<MockIReqTracker> ds_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 1))
      .WillOnce(Return(ds_tracker));
  EXPECT_CALL(*ds_tracker, Plug(true));

  conn_ds_handler(ds_sock);
  FCDSRegFim& reg_fim = dynamic_cast<FCDSRegFim&>(*ds_conn_fim);
  EXPECT_EQ(5U, reg_fim->client_num);

  Mock::VerifyAndClear(tracker_mapper_);
}

}  // namespace
}  // namespace client
}  // namespace cpfs
