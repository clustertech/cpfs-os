#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define classes for interfacing with DS.
 */

#include <cstddef>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"

namespace cpfs {

class IReqTracker;
class ITrackerMapper;

// Client file offsets are converted first to per-DSG offset, and then
// to offsets local to a DS, in its data and checksum files.

// To avoid overloading space and CPU of a DS when small files
// dominate, the allocation of segments to DS is different for
// different inodes, depending on inode numbers.  In particular, inode
// number 1 starts with role 1 in all DSGs it is allocated, inode
// number 2 starts with role 2, and so on.

// We handle these by using a notion of "adjusted role".  Depending on
// inode number, adjust role 0 is the role to hold segment 0, adjusted
// role 1 is the role to hold segment 1, etc.  So Adj_role is the role
// subtracting the inode number modulo 5.  Then the data for each DSG
// is arranged like this:

// Adj_role  0  1  2  3  4
// Segments  0  1  2  3  4
//           5  6  7  8  9
//          10 11 12 13 14
//          15 16 17 18 19
//          20 21 22 23 24
//          25 26 27 28 29
//          30 31 32 33 34
//          35 36 37 38 39

// The checksum file is arranged like this:

// Adj_role   0     1     2     3     4
// Segments 16-19 12-15  8-11  4-7   0-3
//          36-39 32-35 28-31 24-27 20-23

// These are the basis where we derive the formulae in
// Segment::SetRoles(), *To*Offset() and GetDsgOffsetInfo().

/**
 * Subtract an offset from a role number.
 *
 * N.B.: This function eases dealing with computations involving
 * signed and unsigned numbers when working with role numbers, mostly
 * because behavior of % operator with negative operands is
 * implementation defined in C++, and because when % is applied
 * between signed and unsigned numbers both will be "promoted" to
 * unsigned.
 *
 * @param role The role to subtract from
 *
 * @param role_off Role offset to subtract, should be in [0, kNumDSPerGroup]
 *
 * @return role - role_off under kNumDSPerGroup modulo arithematics
 */
inline GroupRole _SubtractRole(GroupRole role, int role_off) {
  int role_int = int(role) - role_off;
  if (role_int < 0)
    role_int += kNumDSPerGroup;
  return role_int;
}

/**
 * Represent a segment, i.e., a range of data kept in the servers.
 */
struct Segment {
  std::size_t file_off; /**< The file offset of this range */
  std::size_t data_size; /**< The size of the range. */
  GroupId dsg; /**< The DS group holding the segment */
  std::size_t dsg_off; /**< The offset restricted to DSG holding the segment */
  GroupRole dsr_data; /**< The DS group role holding the segment */
  GroupRole dsr_checksum; /**< The DS group role holding the checksum */

  /**
   * Set the file-related fields.
   *
   * @param file_off The file offset
   *
   * @param data_size The amount of data
   */
  void SetFile(std::size_t file_off, std::size_t data_size) {
    this->file_off = file_off;
    this->data_size = data_size;
  }

  /**
   * Set the DSG offset and calculate the data and checksum server
   * associated.
   *
   * @param dsg The DSG managing the segment
   *
   * @param dsg_off The offset within the DSG
   *
   * @param dsg_segment The segment number within the DSG, should be
   * dsg_off / kSegmentSize (but clients may get it more easily)
   *
   * @param inode The inode number, used to find the DS roles for data
   * and checksum of the segment
   */
  void SetRoles(GroupId dsg, std::size_t dsg_off,
                std::size_t dsg_segment, InodeNum inode) {
    this->dsg = dsg;
    this->dsg_off = dsg_off;
    dsr_data = (dsg_segment + inode) % kNumDSPerGroup;
    const int kNumDS1 = kNumDSPerGroup - 1;
    // Checksum role for the (hypothetical) inode 0 segment 0 is
    // kNumDSPerGroup - 1.  It increments when inode number
    // increments, and decrements once every (kNumDSPerGroup - 1)
    // segments, modulo kNumDSPerGroup.  So the formula is
    // conceptually
    //
    // (inode - 1 - dsg_segment / kNumDS1) % kNumDSPerGroup.
    //
    // The code below is slightly more complex, to ensure that we not
    // to use % on negative numbers.  Here inode - 1 is always
    // non-negative because inode 0 is not used.
    dsr_checksum = _SubtractRole((inode - 1) % kNumDSPerGroup,
                                 dsg_segment / kNumDS1 % kNumDSPerGroup);
  }
};

/**
 * Perform transformations to handle file offsets and file
 * coordinates.
 */
class FileCoordManager {
 public:
  /**
   * @param inode The inode number of this file.
   *
   * @param groups The DS groups holding the file.
   *
   * @param num_groups The number of DS groups holding the file.
   */
  FileCoordManager(InodeNum inode, const GroupId* groups, unsigned num_groups);

  /**
   * Get the inode number represented by the handle.
   */
  InodeNum inode();

  /**
   * Get the segment descriptions for a range within the file.
   *
   * @param off The offset of the start of the range.
   *
   * @param size The size of the range.
   *
   * @param segment_ret Where to return the segments to.  It will be
   * cleared before being filled.
   */
  void GetSegments(std::size_t off, std::size_t size,
                   std::vector<Segment>* segment_ret);

  /**
   * Get end offsets for all DSGs for a file of specific size.
   *
   * @param size The size of file.
   *
   * @param dsg_off_ret Where to return DSG ids and DSG offsets
   */
  void GetAllGroupEnd(std::size_t size, std::vector<Segment>* dsg_off_ret);

 private:
  InodeNum inode_; /**< The inode number of the file */
  std::vector<GroupId> groups_; /**< The server groups used by the file */
};

/**
 * Get adjusted role number.
 */
inline GroupRole DSAdjustedRole(GroupRole role, InodeNum inode) {
  return _SubtractRole(role, inode % kNumDSPerGroup);
}

/**
 * Get segment start of an offset.
 *
 * @param off The offset
 *
 * @return The offset of the start of the segment containing the offset
 */
inline std::size_t DSToSegmentStart(std::size_t off) {
  return off & ~(kSegmentSize - 1);
}

/**
 * Convert DS data file offsets to DSG offsets.
 *
 * @param inode The inode number
 *
 * @param role The group role
 *
 * @param data_off The data file offset, assumed to align by kSegmentSize
 *
 * @return The DSG offset
 */
inline std::size_t DataToDsgOffset(
    InodeNum inode, GroupRole role, std::size_t data_off) {
  // This is clear from the Adj_role comment at the beginning of this file
  return data_off * kNumDSPerGroup + DSAdjustedRole(role, inode) * kSegmentSize;
}

/**
 * Convert DSG offsets to offset in DS data file.
 *
 * @param dsg_off The DSG offset
 *
 * @return The data offset
 */
inline std::size_t DsgToDataOffset(std::size_t dsg_off) {
  // This is clear from the Adj_role comment at the beginning of this file
  return DSToSegmentStart(dsg_off / kNumDSPerGroup) + dsg_off % kSegmentSize;
}

/**
 * Convert a DS checksum file offset to the first DSG offset protected
 * by that checksum offset.
 *
 * @param inode The inode number
 *
 * @param role The group role
 *
 * @param checksum_off The checksum file offset, assumed to align by
 * kChecksumGroupSize
 *
 * @return The DSG offset
 */
inline std::size_t ChecksumToDsgOffset(
    InodeNum inode, GroupRole role, std::size_t checksum_off) {
  // Refer to the Adj_role comment at the beginning of the file.  The
  // first term is the end of the checksum group before checksum_off.
  // The remainder is (kNumDSPerGroup - 1) segments per checksum
  // group, counted by adjusted role number in reverse order (so we
  // have the subtraction from kNumDSPerGroup - 1).
  return checksum_off * (kNumDSPerGroup * (kNumDSPerGroup - 1))
      + (kNumDSPerGroup - 1 - DSAdjustedRole(role, inode))
      * ((kNumDSPerGroup - 1) * kSegmentSize);
}

/**
 * Convert DSG offsets to offset in DS checksum file.
 *
 * @param dsg_off The DSG offset
 *
 * @return The checksum offset
 */
inline std::size_t DsgToChecksumOffset(std::size_t dsg_off) {
  // This is clear from the Adj_role comment at the beginning of this
  // file, noting that each kNumDSPerGroup checksum group uses the
  // same segment offsets in the checksum file.  The first term
  // calculate the start of this segment offset, the second term is
  // the remainder due to misalignment.
  return DSToSegmentStart(dsg_off / (kNumDSPerGroup * (kNumDSPerGroup - 1)))
      + dsg_off % kSegmentSize;
}

/**
 * Get the offset used by the checksum role for a DSG offset for
 * storing checksum data up to that DSG offset.
 *
 * @param dsg_off The DSG offset
 *
 * @return The last offset in checksum file used
 */
inline std::size_t DsgToChecksumUsage(std::size_t dsg_off) {
  // This is similar to DsgToChecksumOffset(), except that if the
  // number of segments in the last checksum group is more than 0, the
  // checksum file up to the end of the segment containing dsg_off is
  // already used, so we have to adjust for that.
  std::size_t dsg_segment = dsg_off / kSegmentSize;
  std::size_t ret = dsg_segment / ((kNumDSPerGroup - 1) * kNumDSPerGroup)
      * kSegmentSize;
  if (dsg_segment % (kNumDSPerGroup - 1) > 0)  // Previous usage
    return ret + kSegmentSize;
  return ret + dsg_off % kSegmentSize;  // This usage
}

/**
 * Get the offset used to hold data at or after a DSG offset in a role
 * holding an inode.
 *
 * @param inode The inode
 *
 * @param role The group role
 *
 * @param dsg_off The DSG offset.  On return, set to the DSG offset
 * corresponding to data_off_ret
 *
 * @return The offset in data file holding dsg_off.  If the role is
 * not responsible for that data of that DSG offset, return that for
 * the first location after that DSG offset for which the role is
 * responsible
 */
inline std::size_t RoleDsgToDataOffset(
    InodeNum inode, GroupRole role, std::size_t* dsg_off) {
  // Refer to the Adj_role comment at the beginning of the file.  We
  // first find the data offset of the start of the segment containing
  // dsg_off.
  std::size_t dsg_segment = *dsg_off / kSegmentSize;
  std::size_t ret = (dsg_segment / kNumDSPerGroup) * kSegmentSize;
  // Then we check whether data of dsg_off is held by role, and if
  // not, whether previous data in the same checksum group has already
  // been handled by role.
  GroupRole adj_role = DSAdjustedRole(role, inode);
  GroupRole data_server = dsg_segment % kNumDSPerGroup;
  if (adj_role == data_server)  // dsg_off is mine, no need to modify dsg_off
    return ret + *dsg_off % kSegmentSize;
  if (adj_role < data_server)  // My turn ended
    ret += kSegmentSize;
  // Now we calculate the corresponding DSG offset at ret, which is
  // simple because we know that ret is segment aligned (because the
  // return statement above is not executed).
  *dsg_off = ret * kNumDSPerGroup + adj_role * kSegmentSize;
  return ret;
}

/**
 * Get the offset used by a group role for storing data of an inode up
 * to a DSG offset.
 *
 * @param inode The inode
 *
 * @param role The group role
 *
 * @param dsg_off The DSG offset
 *
 * @return The last offset in checksum file used
 */
inline std::size_t RoleDsgToChecksumUsage(
    InodeNum inode, GroupRole role, std::size_t dsg_off) {
  // Refer to the Adj_role comment at the beginning of the file.  We
  // first find the checksum offset of the start of the segment
  // containing dsg_off.
  std::size_t dsg_segment = dsg_off / kSegmentSize;
  std::size_t ret = dsg_segment / ((kNumDSPerGroup - 1) * kNumDSPerGroup)
      * kSegmentSize;
  // Then we check whether checksum of dsg_off is held by role, and if
  // not, whether previous checksums in the same checksum group has
  // already been handled by role.
  GroupRole adj_role = DSAdjustedRole(role, inode);
  GroupRole  checksum_server
      = _SubtractRole(kNumDSPerGroup - 1,
                      dsg_segment / (kNumDSPerGroup - 1) % kNumDSPerGroup);
  if (adj_role < checksum_server)  // Not yet my turn to hold checksum
    return ret;
  if (adj_role > checksum_server)  // My turn to hold checksum ended
    return ret + kSegmentSize;
  // We now know role does handle the checksum.  We check whether the
  // the segment has already been used before dsg_off.  If so we
  // return the end of the segment, otherwise we return just the part
  // used by this dsg_off.
  if (dsg_segment % (kNumDSPerGroup - 1) > 0)  // Previous usage
    return ret + kSegmentSize;
  return ret + dsg_off % kSegmentSize;  // This usage
}

/**
 * Redirector function for redirecting Fims heading for the DS.
 *
 * @param fim The Fim to redirect
 *
 * @param group The DS group of the Fim
 *
 * @param tracker_mapper The tracker mapper for finding request trackers
 *
 * @return The target tracker for redirection
 */
boost::shared_ptr<IReqTracker> DSFimRedirector(
    FIM_PTR<IFim> fim, GroupId group,
    ITrackerMapper* tracker_mapper);

}  // namespace cpfs
