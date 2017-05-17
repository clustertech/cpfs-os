/* Copyright 2013 ClusterTech Ltd */
#include "tracker_mapper_impl.hpp"

#include <stdexcept>
#include <string>

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_policy_mock.hpp"
#include "common.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "tracker_mapper.hpp"

using ::testing::_;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace {

class TrackerMapperTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<MockIAsioPolicy> asio_policy_;
  boost::scoped_ptr<ITrackerMapper> tracker_mapper_;

  TrackerMapperTest()
      : asio_policy_(new MockIAsioPolicy),
        tracker_mapper_(MakeTrackerMapper(asio_policy_.get())) {}
};

TEST_F(TrackerMapperTest, MS) {
  tracker_mapper_->SetClientNum(1);
  boost::shared_ptr<IReqTracker> tracker = tracker_mapper_->GetMSTracker();
  ASSERT_TRUE(tracker);
  tracker_mapper_->SetLimiterMaxSend(1024 * 1024);
  EXPECT_EQ(std::string("MS"), tracker_mapper_->GetMSTracker()->name());
  tracker_mapper_->DumpPendingRequests();
  EXPECT_FALSE(tracker_mapper_->GetMSFimSocket());
  tracker_mapper_->SetClientNum(1);
  EXPECT_THROW(tracker_mapper_->SetClientNum(2), std::runtime_error);

  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*fim_socket, SetReqTracker(tracker));

  tracker_mapper_->SetMSFimSocket(fim_socket);
  EXPECT_EQ(fim_socket, tracker_mapper_->GetMSFimSocket());

  tracker_mapper_->SetMSFimSocket(boost::shared_ptr<IFimSocket>());
  EXPECT_TRUE(tracker_mapper_->GetMSTracker());
  EXPECT_FALSE(tracker_mapper_->GetMSFimSocket());
}

TEST_F(TrackerMapperTest, DS) {
  boost::shared_ptr<IReqTracker> tracker = tracker_mapper_->GetDSTracker(1, 2);
  ASSERT_TRUE(tracker);
  EXPECT_EQ(std::string("DS 1-1"),
            tracker_mapper_->GetDSTracker(1, 1)->name());
  EXPECT_FALSE(tracker_mapper_->GetDSFimSocket(1, 1));

  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  GroupId group_id;
  GroupRole role_id;
  EXPECT_FALSE(tracker_mapper_->FindDSRole(fim_socket, &group_id, &role_id));

  EXPECT_CALL(*fim_socket, SetReqTracker(tracker));

  tracker_mapper_->SetDSFimSocket(fim_socket, 1, 2);
  EXPECT_EQ(fim_socket, tracker_mapper_->GetDSFimSocket(1, 2));
  EXPECT_TRUE(tracker_mapper_->FindDSRole(fim_socket, &group_id, &role_id));
  EXPECT_EQ(1U, group_id);
  EXPECT_EQ(2U, role_id);
  tracker_mapper_->DumpPendingRequests();

  tracker_mapper_->SetDSFimSocket(boost::shared_ptr<IFimSocket>(), 1, 2);
  EXPECT_TRUE(tracker_mapper_->GetDSTracker(1, 1));
  EXPECT_FALSE(tracker_mapper_->GetDSFimSocket(1, 1));
  EXPECT_FALSE(tracker_mapper_->FindDSRole(fim_socket, &group_id, &role_id));
  EXPECT_FALSE(tracker_mapper_->FindDSRole(boost::shared_ptr<IFimSocket>(),
                                           &group_id, &role_id));
}

TEST_F(TrackerMapperTest, FC) {
  EXPECT_FALSE(tracker_mapper_->GetFCTracker(5));
  EXPECT_FALSE(tracker_mapper_->GetFCFimSocket(5));

  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<IReqTracker> tracker;
  EXPECT_CALL(*fim_socket, SetReqTracker(_))
      .WillOnce(SaveArg<0>(&tracker));

  tracker_mapper_->SetFCFimSocket(fim_socket, 5);
  EXPECT_EQ(fim_socket, tracker_mapper_->GetFCFimSocket(5));
  EXPECT_EQ(tracker, tracker_mapper_->GetFCTracker(5));
  EXPECT_EQ(std::string("FC 5"), tracker->name());
  tracker_mapper_->DumpPendingRequests();
  EXPECT_FALSE(tracker_mapper_->GetFCTracker(6));

  tracker_mapper_->SetFCFimSocket(boost::shared_ptr<IFimSocket>(), 5);
  EXPECT_FALSE(tracker_mapper_->GetFCTracker(5));
  EXPECT_FALSE(tracker_mapper_->GetFCFimSocket(5));

  tracker_mapper_->SetFCFimSocket(boost::shared_ptr<IFimSocket>(), 6);
  EXPECT_FALSE(tracker_mapper_->GetFCTracker(6));

  // To cover cleanup
  EXPECT_CALL(*fim_socket, SetReqTracker(_))
      .WillOnce(SaveArg<0>(&tracker));

  tracker_mapper_->SetFCFimSocket(fim_socket, 6);

  EXPECT_CALL(*fim_socket, Shutdown());
}

TEST_F(TrackerMapperTest, Admin) {
  EXPECT_FALSE(tracker_mapper_->GetAdminTracker(5));
  EXPECT_FALSE(tracker_mapper_->GetAdminFimSocket(5));

  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<IReqTracker> tracker;
  EXPECT_CALL(*fim_socket, SetReqTracker(_))
      .WillOnce(SaveArg<0>(&tracker));

  tracker_mapper_->SetAdminFimSocket(fim_socket, 5);
  EXPECT_EQ(fim_socket, tracker_mapper_->GetAdminFimSocket(5));
  EXPECT_EQ(tracker, tracker_mapper_->GetAdminTracker(5));
  EXPECT_EQ(std::string("Admin 5"), tracker->name());
  EXPECT_FALSE(tracker_mapper_->GetAdminTracker(6));

  tracker_mapper_->SetAdminFimSocket(boost::shared_ptr<IFimSocket>(), 5);
  EXPECT_FALSE(tracker_mapper_->GetAdminTracker(5));
  EXPECT_FALSE(tracker_mapper_->GetAdminFimSocket(5));

  tracker_mapper_->SetAdminFimSocket(boost::shared_ptr<IFimSocket>(), 6);
  EXPECT_FALSE(tracker_mapper_->GetAdminTracker(6));
}

TEST_F(TrackerMapperTest, DSGBroadcast) {
  boost::shared_ptr<MockIFimSocket> fim_socket1 =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<MockIFimSocket> fim_socket2 =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<MockIFimSocket> fim_socket3 =
      boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*fim_socket1, SetReqTracker(_));
  EXPECT_CALL(*fim_socket2, SetReqTracker(_));
  EXPECT_CALL(*fim_socket3, SetReqTracker(_));
  tracker_mapper_->SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_socket1), 1, 1);
  tracker_mapper_->SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_socket2), 1, 2);
  tracker_mapper_->SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_socket3), 1, 3);
  FIM_PTR<IFim> broadcast_fim;
  EXPECT_CALL(*fim_socket1, WriteMsg(broadcast_fim));
  EXPECT_CALL(*fim_socket3, WriteMsg(broadcast_fim));
  tracker_mapper_->DSGBroadcast(1, broadcast_fim, 2);

  EXPECT_CALL(*fim_socket1, Shutdown());
  EXPECT_CALL(*fim_socket2, Shutdown());
  EXPECT_CALL(*fim_socket3, Shutdown());
}

TEST_F(TrackerMapperTest, FCBroadcast) {
  boost::shared_ptr<MockIFimSocket> fim_socket1 =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<MockIFimSocket> fim_socket2 =
      boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*fim_socket1, SetReqTracker(_));
  EXPECT_CALL(*fim_socket2, SetReqTracker(_));
  tracker_mapper_->SetFCFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_socket1), 5);
  tracker_mapper_->SetFCFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_socket2), 103);
  FIM_PTR<IFim> broadcast_fim;
  EXPECT_CALL(*fim_socket1, WriteMsg(broadcast_fim));
  EXPECT_CALL(*fim_socket2, WriteMsg(broadcast_fim));
  tracker_mapper_->FCBroadcast(broadcast_fim);

  EXPECT_CALL(*fim_socket1, Shutdown());
  EXPECT_CALL(*fim_socket2, Shutdown());
}

TEST_F(TrackerMapperTest, Plug) {
  boost::shared_ptr<MockIFimSocket> fim_sockets[3];
  fim_sockets[0].reset(new MockIFimSocket);
  fim_sockets[1].reset(new MockIFimSocket);
  fim_sockets[2].reset(new MockIFimSocket);
  EXPECT_CALL(*fim_sockets[0], SetReqTracker(_));
  EXPECT_CALL(*fim_sockets[1], SetReqTracker(_));
  EXPECT_CALL(*fim_sockets[2], SetReqTracker(_));
  tracker_mapper_->SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_sockets[0]), 1, 1);
  tracker_mapper_->SetDSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_sockets[1]), 1, 2);
  tracker_mapper_->SetMSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_sockets[2]));

  // Real call
  tracker_mapper_->Plug(false);

  // Add request should have no effect when unplugged
  FIM_PTR<IFim> req = GetattrFim::MakePtr(0);
  req->set_req_id(1);
  boost::shared_ptr<IReqTracker> tracker = tracker_mapper_->GetMSTracker();
  EXPECT_FALSE(tracker->AddRequestEntry(MakeReplReqEntry(tracker, req)));

  EXPECT_CALL(*fim_sockets[0], Shutdown());
  EXPECT_CALL(*fim_sockets[1], Shutdown());
  EXPECT_CALL(*fim_sockets[2], Shutdown());
}

TEST_F(TrackerMapperTest, HasPendingWrite) {
  boost::shared_ptr<MockIFimSocket> fim_sockets[2];
  fim_sockets[0].reset(new MockIFimSocket);
  fim_sockets[1].reset(new MockIFimSocket);

  EXPECT_CALL(*fim_sockets[0], SetReqTracker(_));
  EXPECT_CALL(*fim_sockets[1], SetReqTracker(_));
  tracker_mapper_->SetMSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_sockets[0]));
  tracker_mapper_->SetFCFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_sockets[1]), 5);
  // DS disconnected
  tracker_mapper_->SetDSFimSocket(boost::shared_ptr<IFimSocket>(), 1, 1);

  EXPECT_CALL(*fim_sockets[0], pending_write())
      .WillOnce(Return(true))
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*fim_sockets[1], pending_write())
      .WillOnce(Return(true))
      .WillRepeatedly(Return(false));
  EXPECT_TRUE(tracker_mapper_->HasPendingWrite());
  EXPECT_TRUE(tracker_mapper_->HasPendingWrite());
  EXPECT_FALSE(tracker_mapper_->HasPendingWrite());

  EXPECT_CALL(*fim_sockets[0], Shutdown());
  EXPECT_CALL(*fim_sockets[1], Shutdown());
}

TEST_F(TrackerMapperTest, Shutdown) {
  boost::shared_ptr<MockIFimSocket> fim_sockets[2];
  fim_sockets[0].reset(new MockIFimSocket);
  fim_sockets[1].reset(new MockIFimSocket);

  boost::shared_ptr<IReqTracker> trackers[2];
  EXPECT_CALL(*fim_sockets[0], SetReqTracker(_))
      .WillOnce(SaveArg<0>(&trackers[0]));
  EXPECT_CALL(*fim_sockets[1], SetReqTracker(_))
      .WillOnce(SaveArg<0>(&trackers[1]));
  tracker_mapper_->SetMSFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_sockets[0]));
  tracker_mapper_->SetFCFimSocket(
      boost::static_pointer_cast<IFimSocket>(fim_sockets[1]), 5);
  ServiceTask task1, task2;
  EXPECT_CALL(*asio_policy_, Post(_))
      .WillOnce(SaveArg<0>(&task1))
      .WillOnce(SaveArg<0>(&task2));

  tracker_mapper_->Shutdown();
  task1();
  task2();

  EXPECT_CALL(*fim_sockets[0], Shutdown());
  EXPECT_CALL(*fim_sockets[1], Shutdown());
}

}  // namespace
}  // namespace cpfs
