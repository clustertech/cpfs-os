/* Copyright 2013 ClusterTech Ltd */
#include "server/ds/conn_mgr_impl.hpp"

#include <stdint.h>

#include <string>

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "config_mgr.hpp"
#include "connecting_socket.hpp"
#include "connector_mock.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "util.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/conn_mgr.hpp"
#include "server/ds/store_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;

namespace cpfs {
namespace server {
namespace ds {
namespace {

class DSConnMgrTest : public ::testing::Test {
 protected:
  MockIAsioPolicy* asio_policy_;
  MockIAsioPolicy* ds_asio_policy_;
  IOService io_service_;
  DeadlineTimer* timers_[2];
  boost::shared_ptr<MockIFimSocket> ms_sock_[2];
  boost::shared_ptr<MockIFimSocket> ds_sock_;
  MockITrackerMapper* tracker_mapper_;
  MockIConnector* conn_;
  MockIStore* store_;
  MockITimeKeeper* dsg_ready_time_keeper_;
  MockIThreadGroup* thread_group_;
  BaseDataServer data_server_;
  IConnMgr* conn_mgr_;

  MockIFimProcessor* ms_fim_proc_;
  MockIFimProcessor* ds_fim_proc_;
  FimSocketCreatedHandler conn_handler_[2];
  ConnectErrorHandler error_handler_[2];

  static ConfigMgr MakeConfig() {
    ConfigMgr ret;
    ret.set_ms1_host("192.168.1.100");
    ret.set_ms1_port(5000);
    ret.set_ms2_host("192.168.1.101");
    ret.set_ms2_port(5001);
    ret.set_ds_host("192.168.1.102");
    ret.set_ds_port(3001);
    return ret;
  }

  DSConnMgrTest(ConfigMgr config = MakeConfig())
      : ds_sock_(new MockIFimSocket), data_server_(config) {
    ms_sock_[0] = boost::shared_ptr<MockIFimSocket>(new MockIFimSocket);
    ms_sock_[1] = boost::shared_ptr<MockIFimSocket>(new MockIFimSocket);
    data_server_.set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    data_server_.set_ds_asio_policy(ds_asio_policy_ = new MockIAsioPolicy);
    EXPECT_CALL(*asio_policy_, io_service())
        .WillRepeatedly(Return(&io_service_));
    data_server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    data_server_.set_connector(conn_ = new MockIConnector);
    data_server_.set_store(store_ = new MockIStore);
    data_server_.set_ms_fim_processor(ms_fim_proc_ = new MockIFimProcessor);
    data_server_.set_ds_fim_processor(ds_fim_proc_ = new MockIFimProcessor);
    data_server_.set_dsg_ready_time_keeper(
        dsg_ready_time_keeper_ = new MockITimeKeeper);
    data_server_.set_thread_group(thread_group_ = new MockIThreadGroup);

    EXPECT_CALL(*store_, is_role_set())
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*store_, ds_group())
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*store_, ds_role())
        .WillRepeatedly(Return(3));
    EXPECT_CALL(*store_, GetUUID())
        .WillRepeatedly(Return("aabbccdd-15b4-235b-cac2-3beb9e546014"));

    PrepareMSConnect();
  }

  void PrepareMSConnect() {
    EXPECT_CALL(*conn_, AsyncConnect("192.168.1.100", 5000,
                                     ms_fim_proc_, _, true, _, _, 0))
        .WillOnce(DoAll(SaveArg<3>(conn_handler_),
                        SaveArg<6>(error_handler_)));
    EXPECT_CALL(*conn_, AsyncConnect("192.168.1.101", 5001,
                                     ms_fim_proc_, _, true, _, _, 0))
        .WillOnce(DoAll(SaveArg<3>(conn_handler_ + 1),
                        SaveArg<6>(error_handler_ + 1)));
    timers_[0] = new DeadlineTimer(io_service_);
    timers_[1] = new DeadlineTimer(io_service_);
    EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
        .WillOnce(Return(timers_[0]))
        .WillOnce(Return(timers_[1]));

    data_server_.set_conn_mgr(conn_mgr_ = MakeConnMgr(&data_server_));
    conn_mgr_->Init();
  }

  void ExpectMSConnection(
      int idx, uint64_t last_ready, FIM_PTR<IFim>* fim_ret) {
    EXPECT_CALL(*dsg_ready_time_keeper_, GetLastUpdate())
        .WillOnce(Return(last_ready));
    if (fim_ret)
      EXPECT_CALL(*ms_sock_[idx], WriteMsg(_))
          .WillOnce(SaveArg<0>(fim_ret));
    else
      EXPECT_CALL(*ms_sock_[idx], WriteMsg(_));
  }
};

TEST_F(DSConnMgrTest, Init) {
  // Trigger callback
  FIM_PTR<IFim> fim;
  ExpectMSConnection(1, 12345678, &fim);

  conn_handler_[1](ms_sock_[1]);
  DSMSRegFim& rfim = dynamic_cast<DSMSRegFim&>(*fim);
  EXPECT_EQ("aabbccdd-15b4-235b-cac2-3beb9e546014", std::string(rfim->uuid));
  EXPECT_EQ('\1', rfim->is_grouped);
  EXPECT_EQ(2U, rfim->ds_group);
  EXPECT_EQ(3U, rfim->ds_role);
  EXPECT_EQ(IPToInt("192.168.1.102"), rfim->ip);
  EXPECT_EQ(3001, rfim->port);
  EXPECT_EQ('\1', rfim->opt_resync);
  EXPECT_EQ('\0', rfim->distressed);

  FIM_PTR<IFim> fim2;
  PrepareMSConnect();
  ExpectMSConnection(1, 0, &fim2);

  conn_handler_[1](ms_sock_[1]);
  DSMSRegFim& rfim2 = dynamic_cast<DSMSRegFim&>(*fim2);
  EXPECT_EQ('\0', rfim2->opt_resync);
}

TEST_F(DSConnMgrTest, ConnectMSRetry) {
  // Error handler will ask for reconnect only if not already connected
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));

  EXPECT_EQ(5.0, error_handler_[0](boost::shared_ptr<IConnectingSocket>(),
                                   boost::asio::error::timed_out));

  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_sock_[1]));

  EXPECT_EQ(-1.0, error_handler_[0](boost::shared_ptr<IConnectingSocket>(),
                                    boost::asio::error::timed_out));
}

TEST_F(DSConnMgrTest, ReconnectMSConnecting) {
  // Init
  ExpectMSConnection(0, 0, 0);

  conn_handler_[0](ms_sock_[0]);

  // Reconnect at this point is skipped because the other is connecting already
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*thread_group_, EnqueueAll(_))
      .WillOnce(SaveArg<0>(&fim));

  conn_mgr_->ReconnectMS(ms_sock_[0]);
  DSInodeLockFim& rfim = static_cast<DSInodeLockFim&>(*fim);
  EXPECT_EQ(0U, rfim->inode);
  EXPECT_EQ('\2', rfim->lock);
}

TEST_F(DSConnMgrTest, ReconnectMSFailed) {
  // Init
  ExpectMSConnection(0, 0, 0);

  conn_handler_[0](ms_sock_[0]);

  // The other connection stopped
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_sock_[0]));

  error_handler_[1](boost::shared_ptr<IConnectingSocket>(),
                    boost::system::error_code());

  // Reconnect
  EXPECT_CALL(*thread_group_, EnqueueAll(_));
  ConnectErrorHandler error_handler;
  EXPECT_CALL(*conn_, AsyncConnect("192.168.1.101", 5001, _, _, true, _, _, 0))
      .WillOnce(SaveArg<6>(&error_handler));

  conn_mgr_->ReconnectMS(ms_sock_[0]);
}

TEST_F(DSConnMgrTest, ReconnectMSRejected) {
  // Init
  ExpectMSConnection(0, 0, 0);

  conn_handler_[0](ms_sock_[0]);

  ExpectMSConnection(1, 0, 0);

  conn_handler_[1](ms_sock_[1]);

  // Stop MS2 connection attempt
  EXPECT_CALL(*ms_sock_[1], Shutdown());
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(ms_sock_[0]));

  conn_mgr_->DisconnectMS(ms_sock_[1]);

  // Reconnect
  EXPECT_CALL(*thread_group_, EnqueueAll(_));
  ConnectErrorHandler error_handler;
  EXPECT_CALL(*conn_, AsyncConnect("192.168.1.101", 5001, _, _, true, _, _, 0))
      .WillOnce(SaveArg<6>(&error_handler));

  conn_mgr_->ReconnectMS(ms_sock_[0]);
}

TEST_F(DSConnMgrTest, DisconnectMS) {
  // Init
  ExpectMSConnection(0, 0, 0);

  conn_handler_[0](ms_sock_[0]);

  // Disconnect
  EXPECT_CALL(*ms_sock_[0], Shutdown());
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));
  IOHandler handler;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timers_[0], 5.0, _))
      .WillOnce(SaveArg<2>(&handler));

  conn_mgr_->DisconnectMS(ms_sock_[0]);

  // Reconnect on timeout
  EXPECT_CALL(*conn_, AsyncConnect("192.168.1.100", 5000,
                                   ms_fim_proc_, _, true, _, _, 0));

  handler(boost::system::error_code());
}

// DS1 connects to MS1
// MS1 replies the IP:port of DS0
TEST_F(DSConnMgrTest, ConnectDS) {
  // Init
  ExpectMSConnection(0, 0, 0);

  conn_handler_[0](ms_sock_[0]);

  // ConnectDS(), preexisting
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(2, 4))
      .WillOnce(Return(ds_sock_));

  conn_mgr_->ConnectDS(IPToInt("192.168.1.104"), 3002, 2, 4);

  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(2, 4))
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));
  FimSocketCreatedHandler conn_ds_callback;
  EXPECT_CALL(*conn_, AsyncConnect("192.168.1.104", 3002, ds_fim_proc_,
                                   _, false, _, _, ds_asio_policy_))
      .WillOnce(SaveArg<3>(&conn_ds_callback));

  conn_mgr_->ConnectDS(IPToInt("192.168.1.104"), 3002, 2, 4);

  // connected
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*ds_sock_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));
  EXPECT_CALL(*tracker_mapper_, SetDSFimSocket(_, 2, 4, true));

  conn_ds_callback(ds_sock_);
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
