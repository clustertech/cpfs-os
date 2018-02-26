/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/ds_locker_impl.hpp"

#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/ds_locker.hpp"
#include "server/ms/topology_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::Throw;

namespace cpfs {
namespace server {
namespace ms {
namespace {

void ReplyEntries(boost::shared_ptr<IReqEntry> entries[],
                  unsigned num_entries) {
  Sleep(0.2)();
  bool failed = false;
  // Skip empty entries, fail the first entry, and reply to the rest
  for (unsigned i = 0; i < num_entries; ++i) {
    if (!entries[i])
      continue;
    if (!failed) {
      failed = true;
      entries[i]->set_reply_error(true);
    }
    entries[i]->SetReply(DataReplyFim::MakePtr(), i + 1);
  }
}

TEST(DSLockerTest, Basic) {
  MockBaseMetaServer server;
  MockITrackerMapper* tracker_mapper;
  server.set_tracker_mapper(tracker_mapper = new MockITrackerMapper);
  MockITopologyMgr* topology_mgr;
  server.set_topology_mgr(topology_mgr = new MockITopologyMgr);
  boost::shared_ptr<MockIReqTracker> trackers[kNumDSPerGroup * 2];
  boost::shared_ptr<MockIFimSocket> sockets[kNumDSPerGroup * 2];
  boost::shared_ptr<IReqEntry> entries[kNumDSPerGroup * 2];
  for (unsigned i = 0; i < kNumDSPerGroup * 2; ++i) {
    trackers[i] = boost::make_shared<MockIReqTracker>();
    sockets[i] = boost::make_shared<MockIFimSocket>();
  }
  EXPECT_CALL(*topology_mgr, GetDSGState(3, _))
      .WillOnce(Return(kDSGReady));
  EXPECT_CALL(*topology_mgr, GetDSGState(1, _))
      .WillOnce(DoAll(SetArgPointee<1>(1),  // DS1-2 degraded
                      Return(kDSGDegraded)));
  for (unsigned i = 0; i < kNumDSPerGroup; ++i) {
    EXPECT_CALL(*tracker_mapper, GetDSTracker(3, i))
        .WillRepeatedly(Return(trackers[i]));
    EXPECT_CALL(*tracker_mapper, GetDSTracker(1, i))
        .WillRepeatedly(Return(trackers[kNumDSPerGroup + i]));
    EXPECT_CALL(*trackers[i], GetFimSocket())
        .WillRepeatedly(Return(i ? sockets[i] :
                               boost::shared_ptr<IFimSocket>()));
    EXPECT_CALL(*trackers[kNumDSPerGroup + i], GetFimSocket())
        .WillRepeatedly(Return(sockets[kNumDSPerGroup + i]));
    EXPECT_CALL(*trackers[i], name())
        .WillRepeatedly(Return("DS mock"));
    EXPECT_CALL(*trackers[kNumDSPerGroup + i], name())
        .WillRepeatedly(Return("DS mock"));
  }
  for (unsigned i = 1; i < kNumDSPerGroup * 2; ++i) {
    if (i == kNumDSPerGroup) {  // DS1-1 lock failed
      EXPECT_CALL(*trackers[i], AddRequestEntry(_, _))
          .WillOnce(Return(false));
    } else if (i != kNumDSPerGroup + 1) {  // DS1-2 recovering
      EXPECT_CALL(*trackers[i], AddRequestEntry(_, _))
          .WillOnce(DoAll(SaveArg<0>(&entries[i]),
                          Return(true)));
    }
  }
  boost::thread replier(boost::bind(&ReplyEntries,
                                    entries, kNumDSPerGroup * 2));

  boost::scoped_ptr<IDSLocker> ds_locker(MakeDSLocker(&server));
  std::vector<GroupId> group_ids;
  group_ids.push_back(3);
  group_ids.push_back(1);
  {
    std::vector<boost::shared_ptr<IMetaDSLock> > locks;
    ds_locker->Lock(5, group_ids, &locks);
    for (unsigned i = 1; i < kNumDSPerGroup * 2; ++i) {
      if (i == kNumDSPerGroup || i == kNumDSPerGroup + 1)
        continue;
      DSInodeLockFim& fim =
          dynamic_cast<DSInodeLockFim&>(*entries[i]->request());
      EXPECT_EQ(5U, fim->inode);
      EXPECT_TRUE(fim->lock);
    }

    // Prepare for unlocks
    for (unsigned i = 2; i < kNumDSPerGroup * 2; ++i) {
      if (i == kNumDSPerGroup || i == kNumDSPerGroup + 1)
        continue;
      EXPECT_CALL(*trackers[i], AddRequestEntry(_, _))
          .WillOnce(DoAll(SaveArg<0>(&entries[i]),
                          Return(true)));
    }

    // Explicit release for one of them
    locks[0]->PrepareRelease();
  }

  for (unsigned i = 0; i < kNumDSPerGroup * 2; ++i)
    Mock::VerifyAndClear(trackers[i].get());
  Mock::VerifyAndClear(tracker_mapper);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
