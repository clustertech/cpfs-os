/* Copyright 2013 ClusterTech Ltd */
#include "ds_iface.hpp"

#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"

using ::testing::Return;

namespace cpfs {
namespace {

TEST(FileCoordManagerTest, EmptySegment) {
  GroupId gids[1] = { 5 };
  FileCoordManager coord_mgr(7, gids, 1);
  EXPECT_EQ(7U, coord_mgr.inode());
  std::vector<Segment> segments;
  coord_mgr.GetSegments(0, 0, &segments);
  EXPECT_EQ(0U, segments.size());
  coord_mgr.GetSegments(100, 0, &segments);
  EXPECT_EQ(0U, segments.size());
}

void CheckSegment(const Segment& res, std::string msg,
                  std::size_t file_off, std::size_t data_size,
                  GroupId dsg, std::size_t dsg_off,
                  GroupRole dsr_data, GroupRole dsr_checksum) {
  msg = "When testing for: " + msg;
  EXPECT_EQ(file_off, res.file_off) << msg;
  EXPECT_EQ(data_size, res.data_size) << msg;
  EXPECT_EQ(dsg, res.dsg) << msg;
  EXPECT_EQ(dsg_off, res.dsg_off) << msg;
  EXPECT_EQ(dsr_data, res.dsr_data) << msg;
  EXPECT_EQ(dsr_checksum, res.dsr_checksum) << msg;
}

TEST(FileCoordManagerTest, SingleSegmentSingleGroup) {
  GroupId gids[] = { 7 };
  FileCoordManager coord_mgr(kNumDSPerGroup - 1, gids, 1);
  std::vector<Segment> segments;

  // First segment
  coord_mgr.GetSegments(0, 100, &segments);
  EXPECT_EQ(1U, segments.size());
  CheckSegment(segments[0], "S0",
               0, 100, 7, 0, kNumDSPerGroup - 1, kNumDSPerGroup - 2);
  // Second segment
  coord_mgr.GetSegments(kSegmentSize, 100, &segments);
  EXPECT_EQ(1U, segments.size());
  CheckSegment(segments[0], "S1",
               kSegmentSize, 100, 7, 32768, 0, kNumDSPerGroup - 2);
  // kNumDSPerGroup-th segment, with offset
  coord_mgr.GetSegments((kNumDSPerGroup - 1) * kSegmentSize + 2, 1000,
                        &segments);
  EXPECT_EQ(1U, segments.size());
  CheckSegment(segments[0], "SE",
               (kNumDSPerGroup - 1) * kSegmentSize + 2, 1000,
               7, 131074,
               kNumDSPerGroup - 2, (2 * kNumDSPerGroup - 3) % kNumDSPerGroup);
}

TEST(FileCoordManagerTest, MultiGroup) {
  GroupId gids[] = { 7, 5 };
  FileCoordManager coord_mgr(kNumDSPerGroup, gids, 2);
  std::vector<Segment> segments;

  // First segment
  coord_mgr.GetSegments(0, 1000, &segments);
  EXPECT_EQ(1U, segments.size());
  CheckSegment(segments[0], "G0", 0, 1000, 7, 0, 0, kNumDSPerGroup - 1);
  // kNumDSPerGroup-th segment, with offset
  coord_mgr.GetSegments((kNumDSPerGroup - 1) * kSegmentSize + 2, 1000,
                        &segments);
  EXPECT_EQ(1U, segments.size());
  CheckSegment(segments[0], "G1",
               (kNumDSPerGroup - 1) * kSegmentSize + 2, 1000,
               5, 2, 0, kNumDSPerGroup - 1);
  // (2 * kNumDSPerGroup - 1)-th segment, with offset
  coord_mgr.GetSegments((2 * kNumDSPerGroup - 2) * kSegmentSize + 2, 1000,
                        &segments);
  EXPECT_EQ(1U, segments.size());
  CheckSegment(segments[0], "G2",
               (2 * kNumDSPerGroup - 2) * kSegmentSize + 2, 1000,
               7, (kNumDSPerGroup - 1) * kSegmentSize + 2,
               kNumDSPerGroup - 1, kNumDSPerGroup - 2);
}

TEST(FileCoordManagerTest, MultiSegment) {
  GroupId gids[] = { 7 };
  FileCoordManager coord_mgr(kNumDSPerGroup, gids, 1);
  std::vector<Segment> segments;

  // First to second segment
  coord_mgr.GetSegments(kSegmentSize - 8, 100, &segments);
  EXPECT_EQ(2U, segments.size());
  CheckSegment(segments[0], "S01-0",
               kSegmentSize - 8, 8, 7, 32760, 0, kNumDSPerGroup - 1);
  CheckSegment(segments[1], "S01-1",
               kSegmentSize, 92, 7, kSegmentSize, 1, kNumDSPerGroup - 1);
  // (kNumDSPerGroup - 1)-th to kNumDSPerGroup-th segment
  coord_mgr.GetSegments((kNumDSPerGroup - 1) * kSegmentSize - 2, 100,
                        &segments);
  EXPECT_EQ(2U, segments.size());
  CheckSegment(segments[0], "SEN-E",  // E = End of row
               (kNumDSPerGroup - 1) * kSegmentSize - 2, 2,
               7, (kNumDSPerGroup - 1) * kSegmentSize - 2,
               kNumDSPerGroup - 2, kNumDSPerGroup - 1);
  CheckSegment(segments[1], "SEN-N",  // N = Next row
               (kNumDSPerGroup - 1) * kSegmentSize, 98,
               7, (kNumDSPerGroup - 1) * kSegmentSize,
               kNumDSPerGroup - 1, kNumDSPerGroup - 2);
  // First to third segment
  coord_mgr.GetSegments(kSegmentSize - 8, kSegmentSize + 100, &segments);
  EXPECT_EQ(3U, segments.size());
  CheckSegment(segments[0], "S012-0",
               kSegmentSize - 8, 8, 7, kSegmentSize - 8, 0, kNumDSPerGroup - 1);
  CheckSegment(segments[1], "S012-0",
               kSegmentSize, kSegmentSize, 7, kSegmentSize,
               1, kNumDSPerGroup - 1);
  EXPECT_EQ(65536U, segments[2].file_off);
  EXPECT_EQ(92U, segments[2].data_size);
  if (kNumDSPerGroup > 3) {
    EXPECT_EQ(2U, segments[2].dsr_data);
    EXPECT_EQ(kNumDSPerGroup - 1, segments[2].dsr_checksum);
  }
}

TEST(FileCoordManagerTest, GetAllGroupEnd) {
  GroupId gids[] = { 7, 5 };
  FileCoordManager coord_mgr(kNumDSPerGroup, gids, 2);
  std::vector<Segment> group_ends;

  coord_mgr.GetAllGroupEnd(0, &group_ends);
  ASSERT_EQ(2U, group_ends.size());
  ASSERT_EQ(7U, group_ends[0].dsg);
  ASSERT_EQ(0U, group_ends[0].dsg_off);
  ASSERT_EQ(5U, group_ends[1].dsg);
  ASSERT_EQ(0U, group_ends[1].dsg_off);

  coord_mgr.GetAllGroupEnd((kNumDSPerGroup - 1) * kSegmentSize + 9,
                           &group_ends);
  ASSERT_EQ(2U, group_ends.size());
  ASSERT_EQ(7U, group_ends[0].dsg);
  ASSERT_EQ((kNumDSPerGroup - 1) * kSegmentSize, group_ends[0].dsg_off);
  ASSERT_EQ(5U, group_ends[1].dsg);
  ASSERT_EQ(9U, group_ends[1].dsg_off);
}

TEST(DSIfaceTest, DataOffset) {
  ASSERT_EQ(0U, DataToDsgOffset(1, 1, 0));
  ASSERT_EQ(0U, DsgToDataOffset(0U));
  ASSERT_EQ(100U, DsgToDataOffset(100));

  ASSERT_EQ(kSegmentSize, DataToDsgOffset(1, 2, 0));
  ASSERT_EQ(kSegmentSize, DataToDsgOffset(kNumDSPerGroup - 1, 0, 0));
  ASSERT_EQ(100U, DsgToDataOffset(kSegmentSize + 100));

  ASSERT_EQ(kNumDSPerGroup * kSegmentSize, DataToDsgOffset(1, 1, kSegmentSize));
  ASSERT_EQ(kSegmentSize + 100U,
            DsgToDataOffset(kNumDSPerGroup * kSegmentSize + 100));

  ASSERT_EQ(2 * kNumDSPerGroup * kSegmentSize,
            DataToDsgOffset(1, 1, 2 * kSegmentSize));
  ASSERT_EQ(2 * kSegmentSize + 100U,
            DsgToDataOffset(2 * kNumDSPerGroup * kSegmentSize + 100));
}

TEST(DSIfaceTest, ChecksumOffset) {
  ASSERT_EQ(0U, ChecksumToDsgOffset(1, kNumDSPerGroup, 0));
  ASSERT_EQ(0U, DsgToChecksumOffset(0));
  ASSERT_EQ(0U, DsgToChecksumUsage(0));
  ASSERT_EQ(100U, DsgToChecksumOffset(100));
  ASSERT_EQ(100U, DsgToChecksumUsage(100));
  ASSERT_EQ(100U, DsgToChecksumOffset(kSegmentSize + 100));
  ASSERT_EQ(kSegmentSize, DsgToChecksumUsage(kSegmentSize + 100));

  ASSERT_EQ((kNumDSPerGroup - 1) * kSegmentSize,
            ChecksumToDsgOffset(1, kNumDSPerGroup - 1, 0));
  ASSERT_EQ((kNumDSPerGroup - 1) * kSegmentSize,
            ChecksumToDsgOffset(2, 0, 0));
  ASSERT_EQ(100U, DsgToChecksumOffset((kNumDSPerGroup - 1) * kSegmentSize
                                      + 100));

  ASSERT_EQ(kNumDSPerGroup * (kNumDSPerGroup - 1) * kSegmentSize,
            ChecksumToDsgOffset(1, kNumDSPerGroup, kSegmentSize));
  ASSERT_EQ(kSegmentSize + 100U,
            DsgToChecksumOffset(kNumDSPerGroup * (kNumDSPerGroup - 1)
                                * kSegmentSize + 100));
}

TEST(DSIfaceTest, RoleDsgOffset) {
  std::size_t dsg_off;

  // Base case: dsg_off = 0, first DS
  EXPECT_EQ(0U, RoleDsgToChecksumUsage(kNumDSPerGroup, 0, 0));
  dsg_off = 0;
  EXPECT_EQ(0U, RoleDsgToDataOffset(kNumDSPerGroup, 0, &dsg_off));
  EXPECT_EQ(0U, dsg_off);

  // Another DS
  EXPECT_EQ(0U, RoleDsgToChecksumUsage(kNumDSPerGroup, 1, 0));
  dsg_off = 0;
  EXPECT_EQ(0U, RoleDsgToDataOffset(kNumDSPerGroup, 1, &dsg_off));
  EXPECT_EQ(kSegmentSize, dsg_off);

  // dsg_off = 100, inode-offseted, checksum
  EXPECT_EQ(100U, RoleDsgToChecksumUsage(1, 0, 100));

  // dsg_off = kSegmentSize
  EXPECT_EQ(0U, RoleDsgToChecksumUsage(kNumDSPerGroup, 0, kSegmentSize));
  dsg_off = kSegmentSize;
  EXPECT_EQ(kSegmentSize, RoleDsgToDataOffset(kNumDSPerGroup, 0, &dsg_off));
  EXPECT_EQ(kSegmentSize * kNumDSPerGroup, dsg_off);

  // dsg_off = kSegmentSize + 100, inode-offseted, data
  EXPECT_EQ(0U, RoleDsgToChecksumUsage(2, 0, kSegmentSize + 100));
  dsg_off = kSegmentSize + 100;
  EXPECT_EQ(0U, RoleDsgToDataOffset(2, 0, &dsg_off));
  EXPECT_EQ(kSegmentSize * (kNumDSPerGroup - 2), dsg_off);

  // dsg_off = kSegmentSize + 100, inode-offseted, checksum
  EXPECT_EQ(kSegmentSize, RoleDsgToChecksumUsage(1, 0, kSegmentSize + 100));
  dsg_off = kSegmentSize + 100;
  EXPECT_EQ(0U, RoleDsgToDataOffset(1, 0, &dsg_off));
  EXPECT_EQ(kSegmentSize * (kNumDSPerGroup - 1), dsg_off);

  // dsg_off = kChecksumGroupSize - 1
  EXPECT_EQ(0U, RoleDsgToChecksumUsage(kNumDSPerGroup, 0,
                                          kChecksumGroupSize - 1));
  dsg_off = kChecksumGroupSize - 1;
  EXPECT_EQ(kSegmentSize, RoleDsgToDataOffset(kNumDSPerGroup, 0, &dsg_off));
  EXPECT_EQ(kSegmentSize * kNumDSPerGroup, dsg_off);

  // dsg_off = kChecksumGroupSize
  EXPECT_EQ(0U, RoleDsgToChecksumUsage(kNumDSPerGroup, 0,
                                        kChecksumGroupSize));
  dsg_off = kChecksumGroupSize;
  EXPECT_EQ(kSegmentSize, RoleDsgToDataOffset(kNumDSPerGroup, 0, &dsg_off));
  EXPECT_EQ(kSegmentSize * kNumDSPerGroup, dsg_off);

  // dsg_off = kChecksumGroupSize + 1, already served checksum
  EXPECT_EQ(kSegmentSize, RoleDsgToChecksumUsage(kNumDSPerGroup,
                                                  kNumDSPerGroup - 1,
                                                  kChecksumGroupSize + 1));
  dsg_off = kChecksumGroupSize + 1;
  EXPECT_EQ(1U, RoleDsgToDataOffset(kNumDSPerGroup, kNumDSPerGroup - 1,
                                    &dsg_off));
  EXPECT_EQ(kChecksumGroupSize + 1, dsg_off);
}

TEST(DSIfaceTest, DSFimRedirector) {
  boost::shared_ptr<MockIReqTracker> req_tracker(new MockIReqTracker);
  MockITrackerMapper tracker_mapper;

  EXPECT_CALL(tracker_mapper, GetDSTracker(1, 0))
      .WillOnce(Return(req_tracker));

  FIM_PTR<ReadFim> fim1 = ReadFim::MakePtr();
  (*fim1)->checksum_role = 0;
  EXPECT_EQ(req_tracker, DSFimRedirector(fim1, 1, &tracker_mapper));

  EXPECT_CALL(tracker_mapper, GetDSTracker(1, 1))
      .WillOnce(Return(req_tracker));

  FIM_PTR<WriteFim> fim2 = WriteFim::MakePtr();
  (*fim2)->checksum_role = 1;
  EXPECT_EQ(req_tracker, DSFimRedirector(fim2, 1, &tracker_mapper));

  EXPECT_CALL(tracker_mapper, GetDSTracker(0, 2))
      .WillOnce(Return(req_tracker));

  FIM_PTR<TruncateDataFim> fim3 = TruncateDataFim::MakePtr();
  (*fim3)->checksum_role = 2;
  EXPECT_EQ(req_tracker, DSFimRedirector(fim3, 0, &tracker_mapper));

  FIM_PTR<ChecksumUpdateFim> fim4 = ChecksumUpdateFim::MakePtr();
  EXPECT_FALSE(DSFimRedirector(fim4, 0, &tracker_mapper));
}

}  // namespace
}  // namespace cpfs
