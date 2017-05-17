/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/dsg_alloc_impl.hpp"

#include <algorithm>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/dsg_alloc.hpp"
#include "server/ms/stat_keeper_mock.hpp"
#include "server/ms/topology_mock.hpp"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class DSGAllocatorTest : public ::testing::Test {
 protected:
  MockBaseMetaServer server_;
  boost::scoped_ptr<IDSGAllocator> allocator_;
  MockIStatKeeper* stat_keeper_;
  MockITopologyMgr* topology_mgr_;

  DSGAllocatorTest()
      : allocator_(MakeDSGAllocator(&server_))  {
    server_.set_stat_keeper(stat_keeper_ = new MockIStatKeeper);
    server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
  }

  bool AllInRange(std::vector<GroupId> groups, GroupId max_val) {
    for (unsigned i = 0; i < groups.size(); ++i)
      if (groups[i] >= max_val)
        return false;
    return true;
  }

  bool AllDifferent(std::vector<GroupId> groups) {
    std::sort(groups.begin(), groups.end());
    return unique(groups.begin(), groups.end()) == groups.end();
  }
};

TEST_F(DSGAllocatorTest, Api) {
  allocator_->Advise("foo", 1, 0);
  EXPECT_CALL(*topology_mgr_, num_groups()).WillOnce(Return(10));
  EXPECT_CALL(*topology_mgr_, DSGReady(_))
      .WillRepeatedly(Return(true));
  AllDSSpaceStat all_stat;
  EXPECT_CALL(*stat_keeper_, GetLastStat())
      .WillOnce(Return(all_stat));
  EXPECT_EQ(0U, allocator_->Allocate(0).size());
}

TEST_F(DSGAllocatorTest, AllocateByRandom) {
  EXPECT_CALL(*topology_mgr_, num_groups())
      .WillOnce(Return(10));
  EXPECT_CALL(*topology_mgr_, DSGReady(_))
      .WillRepeatedly(Return(true));
  AllDSSpaceStat all_stat;
  EXPECT_CALL(*stat_keeper_, GetLastStat())
      .WillRepeatedly(Return(all_stat));
  std::vector<GroupId> ret = allocator_->Allocate(2);
  EXPECT_EQ(2U, ret.size());
  EXPECT_NE(ret[0], ret[1]);
  EXPECT_LT(ret[0], 10U);
  EXPECT_LT(ret[1], 10U);
}

TEST_F(DSGAllocatorTest, AllocateBySpace) {
  EXPECT_CALL(*topology_mgr_, num_groups())
      .WillRepeatedly(Return(10));
  AllDSSpaceStat all_stat;
  for (int i = 0; i < 3; ++i) {
    DSGSpaceStat dsg_stat;
    {
      DSSpaceStat stat;
      stat.free_space = 200;
      stat.online = true;
      dsg_stat.push_back(stat);
    }
    {
      DSSpaceStat stat;
      stat.free_space = 100;
      stat.online = true;
      dsg_stat.push_back(stat);
    }
    all_stat.push_back(dsg_stat);
  }
  DSGSpaceStat dsgn_stat;
  {
    DSSpaceStat stat;
    stat.free_space = 10000;
    stat.online = true;
    dsgn_stat.push_back(stat);
  }
  all_stat.push_back(dsgn_stat);
  // The group is not ready yet
  all_stat.push_back(DSGSpaceStat());
  EXPECT_CALL(*stat_keeper_, GetLastStat())
      .WillRepeatedly(Return(all_stat));
  EXPECT_EQ(1U, allocator_->Allocate(1).size());
  for (int i = 0; i < 100; ++i) {
    std::vector<GroupId> ret = allocator_->Allocate(2);
    EXPECT_EQ(2U, ret.size());
    EXPECT_TRUE(AllDifferent(ret));
    EXPECT_TRUE(AllInRange(ret, all_stat.size()));
  }
  for (int i = 0; i < 100; ++i) {
    std::vector<GroupId> ret = allocator_->Allocate(3);
    EXPECT_EQ(3U, ret.size());
    EXPECT_TRUE(AllDifferent(ret));
    EXPECT_TRUE(AllInRange(ret, all_stat.size()));
  }
  // Extreme cases
  EXPECT_EQ(0U, allocator_->Allocate(0).size());
  EXPECT_EQ(4U, allocator_->Allocate(1000).size());
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
