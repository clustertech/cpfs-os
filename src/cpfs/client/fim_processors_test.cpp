/* Copyright 2013 ClusterTech Ltd */
#include "client/fim_processors.hpp"

#include <stdint.h>  // IWYU pragma: keep

#include <string>

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "op_completion_mock.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker_mock.hpp"
#include "shutdown_mgr_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "client/base_client.hpp"
#include "client/cache_mock.hpp"
#include "client/conn_mgr_mock.hpp"

using ::testing::_;
using ::testing::InvokeArgument;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace client {
namespace {

class ClientFimProcessorTest : public ::testing::Test {
 protected:
  BaseFSClient client_;
  boost::scoped_ptr<IFimProcessor> ms_proc_;
  boost::scoped_ptr<IFimProcessor> ds_proc_;
  boost::scoped_ptr<IFimProcessor> client_proc_;
  MockITrackerMapper* tracker_mapper_;
  MockIConnMgr* conn_mgr_;
  MockICacheMgr* cache_mgr_;
  MockIShutdownMgr* shutdown_mgr_;
  MockIOpCompletionCheckerSet* op_completion_checker_set_;

  ClientFimProcessorTest()
      : ms_proc_(MakeMSCtrlFimProcessor(&client_)),
        ds_proc_(MakeDSCtrlFimProcessor(&client_)),
        client_proc_(MakeGenCtrlFimProcessor(&client_)) {
    client_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    client_.set_conn_mgr(conn_mgr_ = new MockIConnMgr);
    client_.set_cache_mgr(cache_mgr_ = new MockICacheMgr);
    client_.set_shutdown_mgr(shutdown_mgr_ = new MockIShutdownMgr);
    client_.set_op_completion_checker_set(
        op_completion_checker_set_ = new MockIOpCompletionCheckerSet);
  }
};

TEST_F(ClientFimProcessorTest, MSProcRegSuccess) {
  boost::shared_ptr<MockIFimSocket> socket =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<IFimSocket> isocket = socket;
  EXPECT_CALL(*tracker_mapper_, SetMSFimSocket(isocket, false));
  EXPECT_CALL(*tracker_mapper_, SetClientNum(1));
  FimSocketCleanupCallback cleanup_callback;
  EXPECT_CALL(*socket, OnCleanup(_))
      .WillOnce(SaveArg<0>(&cleanup_callback));

  EXPECT_CALL(*socket, name());
  EXPECT_CALL(*socket, remote_info())
      .WillOnce(Return(std::string("127.0.0.1:1234")));

  FIM_PTR<FCMSRegSuccessFim> success_fim(new FCMSRegSuccessFim);
  (*success_fim)->client_num = 1;
  ms_proc_->Process(success_fim, socket);

  // Trigger callback
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(socket));
  EXPECT_CALL(*cache_mgr_, InvalidateAll());
  EXPECT_CALL(*conn_mgr_, ReconnectMS(isocket));

  cleanup_callback();

  // Case of shutting down
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillOnce(Return(socket));
  EXPECT_CALL(*conn_mgr_, ForgetMS(isocket, false));

  client_.Shutdown();
  cleanup_callback();

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ClientFimProcessorTest, MSProcRegRejected) {
  boost::shared_ptr<MockIFimSocket> socket(new MockIFimSocket);
  EXPECT_CALL(*socket, Shutdown());
  EXPECT_CALL(*conn_mgr_, ForgetMS(
      boost::static_pointer_cast<IFimSocket>(socket), true));

  ms_proc_->Process(MSRegRejectedFim::MakePtr(), socket);
}

TEST_F(ClientFimProcessorTest, DSGStateChangeWait) {
  boost::shared_ptr<MockIFimSocket> socket(new MockIFimSocket);
  FIM_PTR<IFim> response;
  EXPECT_CALL(*op_completion_checker_set_, OnCompleteAllGlobal(_))
      .WillOnce(InvokeArgument<0>());
  EXPECT_CALL(*socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&response));

  FIM_PTR<DSGStateChangeWaitFim> fim = DSGStateChangeWaitFim::MakePtr();
  (*fim)->enable = true;
  ms_proc_->Process(fim, socket);
  EXPECT_EQ(kDSGStateChangeReadyFim, response->type());
}

TEST_F(ClientFimProcessorTest, MSProcDSJoined) {
  boost::shared_ptr<IFimSocket> socket(new MockIFimSocket);
  EXPECT_CALL(*conn_mgr_, ConnectDS(12345678, 12345, 2, 3));

  FIM_PTR<TopologyChangeFim> change_fim(new TopologyChangeFim);
  (*change_fim)->type = 'D';
  (*change_fim)->joined = '\1';
  (*change_fim)->ds_group = 2;
  (*change_fim)->ds_role = 3;
  (*change_fim)->ip = 12345678;
  (*change_fim)->port = 12345;
  ms_proc_->Process(change_fim, socket);
}

TEST_F(ClientFimProcessorTest, MSProcDSLeft) {
  boost::shared_ptr<IFimSocket> socket(new MockIFimSocket);
  boost::shared_ptr<MockIFimSocket> ds_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(2, 3))
      .WillOnce(Return(ds_socket));
  EXPECT_CALL(*ds_socket, Shutdown());

  FIM_PTR<TopologyChangeFim> change_fim(new TopologyChangeFim);
  (*change_fim)->type = 'D';
  (*change_fim)->joined = '\0';
  (*change_fim)->ds_group = 2;
  (*change_fim)->ds_role = 3;
  ms_proc_->Process(change_fim, socket);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ClientFimProcessorTest, MSProcDSGStateChange) {
  // Notify ready
  EXPECT_CALL(*cache_mgr_, InvalidateAll());
  EXPECT_CALL(*conn_mgr_, IsReconnectingMS())
      .WillOnce(Return(true));
  EXPECT_CALL(*conn_mgr_, SetReconnectingMS(false));
  boost::shared_ptr<MockIReqTracker> ms_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillRepeatedly(Return(ms_tracker));
  EXPECT_CALL(*ms_tracker, ResendReplied())
      .WillOnce(Return(true));
  boost::shared_ptr<MockIFimSocket> ms_sock2(new MockIFimSocket);
  EXPECT_CALL(*ms_tracker, GetFimSocket())
      .WillOnce(Return(ms_sock2));
  EXPECT_CALL(*ms_sock2, WriteMsg(_));

  FIM_PTR<DSGStateChangeFim> change_fim(new DSGStateChangeFim);
  (*change_fim)->state_change_id = 2;
  (*change_fim)->ds_group = 2;
  (*change_fim)->failed = 0;
  (*change_fim)->state = kDSGReady;
  (*change_fim)->ready = 1;
  boost::shared_ptr<IFimSocket> socket(new MockIFimSocket);
  ms_proc_->Process(change_fim, socket);
  GroupRole failed;
  EXPECT_EQ(kDSGReady, client_.dsg_state(2, &failed));
  EXPECT_EQ(0U, failed);

  // Degrade
  EXPECT_CALL(*conn_mgr_, IsReconnectingMS())
      .WillOnce(Return(false));
  EXPECT_CALL(*ms_tracker, Plug(true));
  boost::shared_ptr<MockIReqTracker> ds_tracker(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(2, 0))
      .WillOnce(Return(ds_tracker));
  ReqRedirector redirector;
  EXPECT_CALL(*ds_tracker, RedirectRequests(_))
      .WillOnce(DoAll(SaveArg<0>(&redirector),
                      Return(true)));
  EXPECT_CALL(*cache_mgr_, InvalidateAll());

  (*change_fim)->state = kDSGDegraded;
  ms_proc_->Process(change_fim, socket);

  // Redirect target for various Fims
  boost::shared_ptr<MockIReqTracker> ds_tracker2(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(2, 1))
      .WillOnce(Return(ds_tracker2));

  FIM_PTR<ReadFim> read_fim(new ReadFim);
  (*read_fim)->checksum_role = 1;
  EXPECT_EQ(ds_tracker2, redirector(read_fim));

  EXPECT_CALL(*tracker_mapper_, GetDSTracker(2, 3))
      .WillOnce(Return(ds_tracker2));

  FIM_PTR<WriteFim> write_fim(new WriteFim);
  (*write_fim)->checksum_role = 3;
  EXPECT_EQ(ds_tracker2, redirector(write_fim));

  EXPECT_FALSE(redirector(change_fim));

  Mock::VerifyAndClear(ms_tracker.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ClientFimProcessorTest, MSSysHalt) {
  FIM_PTR<SysHaltFim> fim(new SysHaltFim);
  boost::shared_ptr<IFimSocket> socket(new MockIFimSocket);

  EXPECT_CALL(*shutdown_mgr_, Shutdown());

  ms_proc_->Process(fim, socket);
}

TEST_F(ClientFimProcessorTest, MSSysShutdownReq) {
  FIM_PTR<SysShutdownReqFim> fim(new SysShutdownReqFim);
  boost::shared_ptr<IFimSocket> socket(new MockIFimSocket);

  EXPECT_CALL(*shutdown_mgr_, Init(_));

  ms_proc_->Process(fim, socket);
}

TEST_F(ClientFimProcessorTest, DSNotDegraded) {
  boost::shared_ptr<MockIReqTracker> stracker(new MockIReqTracker);
  boost::shared_ptr<MockIFimSocket> sender(new MockIFimSocket);
  EXPECT_CALL(*sender, GetReqTracker())
      .WillRepeatedly(Return(stracker.get()));

  // No request entry: simply return
  EXPECT_CALL(*stracker, GetRequestEntry(4))
      .WillOnce(Return(boost::shared_ptr<IReqEntry>()));

  FIM_PTR<NotDegradedFim> nd_fim = NotDegradedFim::MakePtr();
  (*nd_fim)->redirect_req = 4;
  ds_proc_->Process(nd_fim, sender);

  // Redirect ReadFim correctly
  FIM_PTR<ReadFim> rfim = ReadFim::MakePtr();
  rfim->set_req_id(1);
  (*rfim)->target_group = 2;
  (*rfim)->target_role = 1;
  boost::shared_ptr<MockIReqEntry> entry(new MockIReqEntry);
  EXPECT_CALL(*stracker, GetRequestEntry(1))
      .WillOnce(Return(entry));
  EXPECT_CALL(*entry, request())
      .WillOnce(Return(rfim));
  boost::shared_ptr<MockIReqTracker> target(new MockIReqTracker);
  boost::shared_ptr<IReqTracker> itarget(target);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(2, 1))
      .WillOnce(Return(target));
  EXPECT_CALL(*stracker, RedirectRequest(1, itarget));

  (*nd_fim)->redirect_req = 1;
  ds_proc_->Process(nd_fim, sender);

  // Redirect WriteFim correctly
  FIM_PTR<WriteFim> wfim = WriteFim::MakePtr();
  wfim->set_req_id(2);
  (*wfim)->target_group = 3;
  (*wfim)->target_role = 0;
  EXPECT_CALL(*stracker, GetRequestEntry(2))
      .WillOnce(Return(entry));
  EXPECT_CALL(*entry, request())
      .WillOnce(Return(wfim));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(3, 0))
      .WillOnce(Return(target));
  EXPECT_CALL(*stracker, RedirectRequest(2, itarget));

  (*nd_fim)->redirect_req = 2;
  ds_proc_->Process(nd_fim, sender);

  // Other Fims: simply return
  FIM_PTR<OpenFim> ofim = OpenFim::MakePtr();
  EXPECT_CALL(*stracker, GetRequestEntry(2))
      .WillOnce(Return(entry));
  EXPECT_CALL(*entry, request())
      .WillOnce(Return(ofim));

  ds_proc_->Process(nd_fim, sender);

  // No Fim: simply return
  EXPECT_CALL(*stracker, GetRequestEntry(2))
      .WillOnce(Return(entry));
  EXPECT_CALL(*entry, request())
      .WillOnce(Return(FIM_PTR<IFim>()));

  ds_proc_->Process(nd_fim, sender);

  Mock::VerifyAndClear(tracker_mapper_);
  Mock::VerifyAndClear(stracker.get());
}

TEST_F(ClientFimProcessorTest, GenInvalidate) {
  boost::shared_ptr<MockIFimSocket> sender =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<InvInodeFim> inv_fim = InvInodeFim::MakePtr();
  inv_fim->set_req_id(10);
  (*inv_fim)->inode = 5;
  (*inv_fim)->clear_page = false;
  EXPECT_CALL(*cache_mgr_, InvalidateInode(5, false));

  client_proc_->Process(inv_fim, sender);
}

TEST_F(ClientFimProcessorTest, GenSysStateNotify) {
  boost::shared_ptr<MockIFimSocket> sender =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<MockIFimSocket> ms_socket =
      boost::make_shared<MockIFimSocket>();
  FIM_PTR<SysStateNotifyFim> notify_fim
      = SysStateNotifyFim::MakePtr();
  boost::shared_ptr<MockIReqTracker> ms_tracker =
      boost::make_shared<MockIReqTracker>();
  (*notify_fim)->type = 'M';
  (*notify_fim)->ready = true;
  notify_fim->set_req_id(10);

  EXPECT_CALL(*tracker_mapper_, GetMSTracker())
      .WillOnce(Return(ms_tracker));
  EXPECT_CALL(*ms_tracker, Plug(true));

  client_proc_->Process(notify_fim, sender);
  Mock::VerifyAndClear(ms_socket.get());
  Mock::VerifyAndClear(ms_tracker.get());
  Mock::VerifyAndClear(tracker_mapper_);
}

}  // namespace
}  // namespace client
}  // namespace cpfs
