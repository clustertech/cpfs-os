/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/stat_keeper_impl.hpp"

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "common.hpp"  // IWYU pragma: keep
#include "fim.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"
#include "topology_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/stat_keeper.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class StatKeeperTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  MockIAsioPolicy* asio_policy_;
  MockITopologyMgr* topology_mgr_;
  MockITrackerMapper* tracker_mapper_;
  boost::scoped_ptr<IStatKeeper> stat_keeper_;
  IOService service_;

  StatKeeperTest()
      : stat_keeper_(MakeStatKeeper(&server_)) {
    server_.set_asio_policy(asio_policy_ = new MockIAsioPolicy);
    server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
  }
};

TEST_F(StatKeeperTest, Empty) {
  AllDSSpaceStat stat = stat_keeper_->GetLastStat();
  EXPECT_EQ(0U, stat.size());
}

TEST_F(StatKeeperTest, DegeneratedRun) {
  DeadlineTimer* timer = new DeadlineTimer(service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 0, _));
  EXPECT_CALL(*topology_mgr_, DSGReady(0))
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*topology_mgr_, num_groups())
      .WillRepeatedly(Return(1));

  stat_keeper_->Run();
}

TEST_F(StatKeeperTest, SimpleRun) {
  // On Run(): start request
  DeadlineTimer* timer = new DeadlineTimer(service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 0, _));
  EXPECT_CALL(*topology_mgr_, DSGReady(0))
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*topology_mgr_, num_groups())
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(_, _))
      .WillRepeatedly(Return(boost::shared_ptr<IReqTracker>()));
  boost::shared_ptr<MockIReqTracker> ds_tracker0(new MockIReqTracker);
  boost::shared_ptr<MockIReqTracker> ds_tracker1(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(_, 0))
      .WillRepeatedly(Return(ds_tracker0));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(_, 1))
      .WillRepeatedly(Return(ds_tracker1));
  boost::shared_ptr<IReqEntry> req_entry0;
  EXPECT_CALL(*ds_tracker0, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry0),
                      Return(true)));
  boost::shared_ptr<IReqEntry> req_entry1;
  EXPECT_CALL(*ds_tracker1, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry1),
                      Return(true)));

  stat_keeper_->Run();

  // On first Completion: continue waiting
  req_entry0->SetReply(FIM_PTR<IFim>(), 99);

  // On second completion: run callback, schedule next refresh
  MockFunction<void()> glob_callback;
  stat_keeper_->OnAllStat(boost::bind(GetMockCall(glob_callback),
                                      &glob_callback));
  EXPECT_CALL(glob_callback, Call());
  IOHandler handler;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 10, _))
      .WillOnce(SaveArg<2>(&handler));

  FIM_PTR<MSStatFSReplyFim> reply = MSStatFSReplyFim::MakePtr();
  (*reply)->total_space = 54321;
  (*reply)->free_space = 12345;
  req_entry1->SetReply(reply, 100);

  // Values obtained from GetLastStat is correct
  AllDSSpaceStat stat = stat_keeper_->GetLastStat();
  EXPECT_EQ(1U, stat.size());
  EXPECT_FALSE(stat[0][0].online);
  EXPECT_TRUE(stat[0][1].online);
  EXPECT_EQ(54321U, stat[0][1].total_space);
  EXPECT_EQ(12345U, stat[0][1].free_space);

  // Handler called: query again
  EXPECT_CALL(*ds_tracker0, AddRequestEntry(_, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*ds_tracker1, AddRequestEntry(_, _))
      .WillOnce(Return(false));

  boost::system::error_code ec;
  handler(ec);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(StatKeeperTest, OnNewStat) {
  // On Run(): start request
  DeadlineTimer* timer = new DeadlineTimer(service_);
  EXPECT_CALL(*asio_policy_, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 0, _));
  EXPECT_CALL(*topology_mgr_, DSGReady(0))
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*topology_mgr_, num_groups())
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(_, _))
      .WillRepeatedly(Return(boost::shared_ptr<IReqTracker>()));
  boost::shared_ptr<MockIReqTracker> ds_tracker0(new MockIReqTracker);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(_, 0))
      .WillRepeatedly(Return(ds_tracker0));
  boost::shared_ptr<IReqEntry> req_entry0;
  EXPECT_CALL(*ds_tracker0, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry0),
                      Return(true)));

  stat_keeper_->Run();

  // OnNewStat called during fetching, becomes a single pending entry
  MockFunction<void()> new_stat_callback_1;
  stat_keeper_->OnNewStat(boost::bind(GetMockCall(new_stat_callback_1),
                                      &new_stat_callback_1));
  MockFunction<void()> new_stat_callback_2;
  stat_keeper_->OnNewStat(boost::bind(GetMockCall(new_stat_callback_2),
                                      &new_stat_callback_2));

  // On completion: run callback, immediate re-fetch
  boost::shared_ptr<IReqEntry> req_entry1;
  EXPECT_CALL(*ds_tracker0, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry1),
                      Return(true)));

  FIM_PTR<MSStatFSReplyFim> reply = MSStatFSReplyFim::MakePtr();
  (*reply)->total_space = 54321;
  (*reply)->free_space = 12345;
  req_entry0->SetReply(reply, 100);

  // When re-fetch completes, call callback, and reschedule
  EXPECT_CALL(new_stat_callback_1, Call());
  EXPECT_CALL(new_stat_callback_2, Call());
  IOHandler handler;
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 10, _))
      .WillOnce(SaveArg<2>(&handler));

  req_entry1->SetReply(reply, 100);

  // If OnNewStat called when no fetch is in progress, fetch and
  // cancel refresh timer
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 0, _));
  boost::shared_ptr<IReqEntry> req_entry3;
  EXPECT_CALL(*ds_tracker0, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry3),
                      Return(true)));

  MockFunction<void()> new_stat_callback_3;
  stat_keeper_->OnNewStat(boost::bind(GetMockCall(new_stat_callback_3),
                                      &new_stat_callback_3));

  // Handler will be called with an error, should have no effect
  handler(boost::system::error_code(
      boost::system::errc::operation_canceled,
      boost::system::system_category()));

  // Callback called and timer rescheduled once reply is received
  EXPECT_CALL(new_stat_callback_3, Call());
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 10, _));

  req_entry3->SetReply(reply, 100);

  // Re-run will trigger new fetch
  EXPECT_CALL(*asio_policy_, SetDeadlineTimer(timer, 0, _));
  boost::shared_ptr<IReqEntry> req_entry4;
  EXPECT_CALL(*ds_tracker0, AddRequestEntry(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_entry4),
                      Return(true)));
  stat_keeper_->Run();

  Mock::VerifyAndClear(tracker_mapper_);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
