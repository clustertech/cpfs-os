/* Copyright 2013 ClusterTech Ltd */
#include "client/admin_fim_processor.hpp"

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "tracker_mapper_mock.hpp"
#include "client/base_client.hpp"
#include "client/conn_mgr_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace client {
namespace {

class AdminFimProcessorTest : public ::testing::Test {
 protected:
  BaseAdminClient client_;
  boost::scoped_ptr<IFimProcessor> client_proc_;
  MockITrackerMapper* tracker_mapper_;
  MockIConnMgr* conn_mgr_;

  AdminFimProcessorTest() : client_proc_(MakeAdminFimProcessor(&client_)) {
    client_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    client_.set_conn_mgr(conn_mgr_ = new MockIConnMgr);
  }
};

TEST_F(AdminFimProcessorTest, MSProcRegSuccess) {
  boost::shared_ptr<MockIFimSocket> socket =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<IFimSocket> isocket = socket;
  EXPECT_CALL(*tracker_mapper_, SetMSFimSocket(isocket, true));
  EXPECT_CALL(*tracker_mapper_, SetClientNum(1));

  FIM_PTR<FCMSRegSuccessFim> success_fim(new FCMSRegSuccessFim);
  (*success_fim)->client_num = 1;
  client_proc_->Process(success_fim, isocket);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(AdminFimProcessorTest, MSProcRegRejected) {
  boost::shared_ptr<MockIFimSocket> socket(new MockIFimSocket);
  EXPECT_CALL(*socket, Shutdown());
  EXPECT_CALL(*conn_mgr_, ForgetMS(
      boost::static_pointer_cast<IFimSocket>(socket), true));

  client_proc_->Process(MSRegRejectedFim::MakePtr(), socket);
}

TEST_F(AdminFimProcessorTest, MSProcDSJoined) {
  boost::shared_ptr<IFimSocket> socket(new MockIFimSocket);
  EXPECT_CALL(*conn_mgr_, ConnectDS(12345678, 12345, 2, 3));

  FIM_PTR<TopologyChangeFim> change_fim(new TopologyChangeFim);
  (*change_fim)->type = 'D';
  (*change_fim)->joined = '\1';
  (*change_fim)->ds_group = 2;
  (*change_fim)->ds_role = 3;
  (*change_fim)->ip = 12345678;
  (*change_fim)->port = 12345;
  client_proc_->Process(change_fim, socket);
}

TEST_F(AdminFimProcessorTest, MSProcDSLeft) {
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
  client_proc_->Process(change_fim, socket);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(AdminFimProcessorTest, MSProcDSGStateChange) {
  boost::shared_ptr<IFimSocket> socket(new MockIFimSocket);

  FIM_PTR<DSGStateChangeFim> change_fim(new DSGStateChangeFim);
  (*change_fim)->ready = 1;
  client_proc_->Process(change_fim, socket);
}

}  // namespace
}  // namespace client
}  // namespace cpfs
