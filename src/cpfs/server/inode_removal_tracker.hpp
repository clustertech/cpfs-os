#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the IInodeRemovalTracker interface.
 */

#include <string>
#include <vector>

#include "common.hpp"

namespace cpfs {
namespace server {

/**
 * Record inodes removal.
 *
 * Normally, inodes are recorded in memory with RecordRemoved(), and
 * will be dropped with ExpireRemoved() after a certain period.  But
 * if immediate persisting is enabled (using SetPersistRemoved()), the
 * memory records are written to a file, and further RecordRemoved()
 * calls will directly write the result to the file, while
 * ExpireRemoved() is essentially a no-op.  Thus if the tracker has
 * immediate persisting enabled at the time of lost connection, it
 * allows the removed inodes to be listed upon HA recovery.
 *
 * Note that all the operations are not sync'ed to the disk
 * immediately.  This can lead to files not removed in the case that
 * two servers simultaneously fails.  But this is not considered
 * sufficiently important to degrade performance with sync operations.
 */
class IInodeRemovalTracker {
 public:
  virtual ~IInodeRemovalTracker() {}
  /**
   * @param inode Inode number
   */
  virtual void RecordRemoved(InodeNum inode) = 0;

  /**
   * Enable or disable immediate persisting.  When set enabled, all
   * records previously stored in memory will be written to disk.
   * Future invocation of RecordRemoved() will persist record to disk
   * immediately, and ExpireRemoved() is a no-op.  When set disabled,
   * the persisted file is removed.
   *
   * @param enabled To enable or disable
   */
  virtual void SetPersistRemoved(bool enabled) = 0;

  /**
   * Get inodes removal records persisted on disk.
   *
   * @return List of inodes
   */
  virtual std::vector<InodeNum> GetRemovedInodes() = 0;

  /**
   * Expire inode removal records recorded in memory.  No-op if
   * immediate persisting is enabled.
   *
   * @param age The maximum age of recently removed file and directory
   */
  virtual void ExpireRemoved(int age) = 0;
};

}  // namespace server
}  // namespace cpfs
