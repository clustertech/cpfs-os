#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the IInodeSrc interface.
 */

#include <stdexcept>
#include <string>
#include <vector>

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Interface for inode source, to allocate inodes and mark them as
 * used.  The system records the last inode used, so that during after
 * restart the previously used inode number are not reused.  But
 * allocated inodes are not automatically become used.  Instead, it
 * becomes used only after a call to NotifyInodeUsed(), with the
 * assertion that the inode now appears in the MS store.  This way,
 * the last used count does not need to be persisted every time an
 * inode is allocated or used (as the store provides additional
 * information).  Accordingly, the InodeSrc also needs to know it when
 * an inode is removed from the store, so that the count information
 * can be immediately recorded if needed.  The inode source has
 * minimal knowledge of the MS store (in particular, where the files
 * for each inode is stored if they are currently used), so that the
 * above optimization is possible.
 */
class IInodeSrc {
 public:
  virtual ~IInodeSrc() {}

  /**
   * Initialize the source by reading the persisted file.  Must be
   * called before any other method is called.
   */
  virtual void Init() = 0;

  /**
   * Setup the object for Allocate() call.  After the call, all
   * NotifyUsed() calls have arguments coming from Allocate() calls of
   * this object.
   */
  virtual void SetupAllocation() = 0;

  /**
   * Allocate an inode number.  This should only be called after a
   * SetupAllocation() call.
   *
   * @param parent The parent inode number
   *
   * @param parent_hex Whether the new inode number allocated should use the
   * same parent directory as the given parent inode. If false, the prefix
   * HEX will be assigned randomly.
   *
   * @return The inode number.
   */
  virtual InodeNum Allocate(InodeNum parent, bool parent_hex) = 0;

  /**
   * Notify that an inode is used.  This must always be accompanied
   * with a modification of the MS store that creates the inode file,
   * and if the inode file is removed later, the NotifyRemoved()
   * method must be called.
   *
   * @param inode The inode number.
   */
  virtual void NotifyUsed(InodeNum inode) = 0;

  /**
   * Notify that the object representing an inode is removed.
   *
   * @param inode The inode number.
   */
  virtual void NotifyRemoved(InodeNum inode) = 0;

  /**
   * Get the last inode used.
   *
   * @return The ordered array of last inode used
   */
  virtual std::vector<InodeNum> GetLastUsed() = 0;

  /**
   * Set the last inode used.
   *
   * @param inodes The ordered array of last inode used
   */
  virtual void SetLastUsed(const std::vector<InodeNum>& inodes) = 0;
};

/**
 * Represent the condition that an expected inode link cannot be found.
 */
class NonexistentILink : public std::runtime_error {
 public:
  /**
   * @param msg The message to show in case the error slips to the
   * user.
   */
  explicit NonexistentILink(std::string msg = "") : std::runtime_error(msg) {}
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
