#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define inode allocation classes.
 */

#include <stdint.h>

#include <string>
#include <vector>

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ms {

class IStatKeeper;

/**
 * Server group allocator for MS to create new files.
 */
class IDSGAllocator {
 public:
  virtual ~IDSGAllocator() {}

  /**
   * Allocate a number of groups.  The ordering of the groups returned
   * is randomized.  The number of groups returned may be less than
   * that requested for administrative reasons, e.g., there are not so
   * many groups.
   *
   * @param num_groups The number of groups to allocate.
   *
   * @return The new group.
   */
  virtual std::vector<GroupId> Allocate(GroupId num_groups) = 0;

  /**
   * Advise the group allocation.  Not used currently, but may later
   * be used to help the allocator to improve the allocation, e.g.,
   * spread files to less crowded groups.
   *
   * @param item The item that is being advised (e.g., "blk_avail",
   * "blk_used", etc.).
   *
   * @param group_id The id of the group with the value.
   *
   * @param value The value.
   */
  virtual void Advise(std::string item, GroupId group_id, int64_t value) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
