/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement classes for DS interface.
 */

#include "ds_iface.hpp"

#include <algorithm>
#include <cstddef>
#include <vector>

#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "tracker_mapper.hpp"

namespace cpfs {

// We split file contents into checksum groups of length
// kNumDSPerGroup - 1 segments, so that each checksum group is handled
// by one DSG.  Each DS in the DS group involved will either hold one
// segment of data or one segment of checksum for it.

class IReqTracker;

FileCoordManager::FileCoordManager(InodeNum inode, const GroupId* groups,
                                   unsigned num_groups) : inode_(inode) {
  for (unsigned i = 0; i < num_groups; ++i)
    groups_.push_back(groups[i]);
}

InodeNum FileCoordManager::inode() {
  return inode_;
}

namespace {

/**
 * Calculate the DSG offset given a file offset as in the filesystem
 * seen by clients.
 *
 * @param off The file offset
 *
 * @param num_groups Number of DS groups associated with the file
 *
 * @param dsg_idx_ret Where to return the DSG index to use
 */
std::size_t FSToDsgOffset(std::size_t off, unsigned num_groups,
                          unsigned* dsg_idx_ret) {
  // To do the conversion, we deduct from off the number of bytes in
  // checksum groups.  We first count the number of checksum group
  // before the one containing off.
  std::size_t num_cgs = (off / kSegmentSize) / (kNumDSPerGroup - 1);
  *dsg_idx_ret = num_cgs % num_groups;
  // Now we find the number of checksum groups that the one holding
  // off should have been assigned before.  Note that it would be
  // assigned once per num_groups checksum groups.
  std::size_t my_cgs_before = num_cgs / num_groups;
  // Deducting the num_cgs checksum groups other than those in the
  // my_cgs_before checksum groups, we get the DSG offset.
  std::size_t adj_cg = num_cgs - my_cgs_before;
  return off - adj_cg * kChecksumGroupSize;
}

}  // namespace

void FileCoordManager::GetSegments(std::size_t off, std::size_t size,
                                   std::vector<Segment>* segment_ret) {
  segment_ret->clear();
  if (size == 0U)
    return;
  // size + off % kSegmentSize is the number of bytes involved
  // including bytes before off in the first segment, the formula (x -
  // 1) / kSegmentSize + 1 is a ceiling division knowing x > 0.  This
  // is used instead of push_back to avoid the memory reallocations
  // involved.
  segment_ret->resize((size + off % kSegmentSize - 1) / kSegmentSize + 1);
  // First calculate the first segment
  Segment* curr = &(*segment_ret)[0];
  std::size_t seg_off = off % kSegmentSize;
  curr->SetFile(off, std::min(size, kSegmentSize - seg_off));
  unsigned dsg_idx;
  std::size_t dsg_off = FSToDsgOffset(off, groups_.size(), &dsg_idx);
  std::size_t dsg_segment = dsg_off / kSegmentSize;
  curr->SetRoles(groups_[dsg_idx], dsg_off, dsg_segment, inode_);
  // Make it look as if the request is aligned, for the loop
  off -= seg_off;
  size += seg_off;
  dsg_off -= seg_off;
  std::size_t dsg_off_start = dsg_off - dsg_off % kChecksumGroupSize;
  // Main loop to get all remaining segments.  All we need is to step
  // the offsets by kSegmentSize, reduce the remaining size, move to
  // the next group whenever the DSG offset is found to be checksum
  // group aligned, and move back to the first group once all groups
  // have been used.
  while (size > kSegmentSize) {
    ++curr;
    off += kSegmentSize;
    size -= kSegmentSize;
    curr->SetFile(off, std::min(size, kSegmentSize));
    dsg_off += kSegmentSize;
    if (dsg_off % kChecksumGroupSize == 0) {  // A checksum group just completed
      ++dsg_idx;
      if (dsg_idx == groups_.size()) {  // All DSG completed a row
        dsg_idx = 0;
        dsg_off_start += kChecksumGroupSize;
      }
      dsg_off = dsg_off_start;
    }
    curr->SetRoles(groups_[dsg_idx], dsg_off, ++dsg_segment, inode_);
  }
}

void FileCoordManager::GetAllGroupEnd(std::size_t size,
                                      std::vector<Segment>* dsg_off_ret) {
  dsg_off_ret->clear();
  dsg_off_ret->resize(groups_.size());
  unsigned dsg_idx;
  // Convert to DS group local offset
  std::size_t dsg_off = FSToDsgOffset(size, groups_.size(), &dsg_idx);
  std::size_t dsg_off_start = dsg_off - dsg_off % kChecksumGroupSize;
  // These DS groups have completed final row
  for (unsigned i = 0; i < dsg_idx; ++i) {
    (*dsg_off_ret)[i].SetFile(size, 0);
    (*dsg_off_ret)[i].SetRoles(groups_[i], dsg_off_start + kChecksumGroupSize,
                               dsg_off_start / kSegmentSize + kNumDSPerGroup,
                               inode_);
  }
  // This is the DS group containing at offset "size"
  (*dsg_off_ret)[dsg_idx].SetFile(size, 0);
  (*dsg_off_ret)[dsg_idx].SetRoles(
      groups_[dsg_idx], dsg_off, dsg_off / kSegmentSize, inode_);
  // These DS groups have empty final row
  for (unsigned i = dsg_idx + 1; i < groups_.size(); ++i) {
    (*dsg_off_ret)[i].SetFile(size, 0);
    (*dsg_off_ret)[i].SetRoles(groups_[i], dsg_off_start,
                               dsg_off_start / kSegmentSize, inode_);
  }
}

boost::shared_ptr<IReqTracker> DSFimRedirector(
    FIM_PTR<IFim> fim, GroupId group,
    ITrackerMapper* tracker_mapper) {
  GroupRole role;
  switch (fim->type()) {
    case kReadFim:
      role = static_cast<ReadFim&>(*fim)->checksum_role;
      break;
    case kWriteFim:
      role = static_cast<WriteFim&>(*fim)->checksum_role;
      break;
    case kTruncateDataFim:
      role = static_cast<TruncateDataFim&>(*fim)->checksum_role;
      break;
    default:
      return boost::shared_ptr<IReqTracker>();
  }
  return tracker_mapper->GetDSTracker(group, role);
}

}  // namespace cpfs
