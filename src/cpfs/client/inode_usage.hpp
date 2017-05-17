#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define InodeUsageSet for tracking inode operations.
 */

#include <boost/scoped_ptr.hpp>

#include "common.hpp"

namespace cpfs {
namespace client {

/**
 * Interface for a guard against inode usage information modifications.
 * This stops such modifications for an inode until it is destroyed.
 */
class IInodeUsageGuard {
 public:
  virtual ~IInodeUsageGuard() {}
};

/**
 * Possible return value of IInodeUsageSet::UpdateClosed() to indicate
 * that there is no change in the access mode of the inode.  Otherwise
 * the value is a boolean, indicating whether access is retained.
 */
const int kClientAccessUnchanged = 2;

/**
 * Interface for tracking inode usage, when an inode is being Open()
 * or Close().  It tracks inodes opened by clients of an FC.
 */
class IInodeUsageSet {
 public:
  virtual ~IInodeUsageSet() {}

  /**
   * Increment the use count for an inode.
   *
   * @param inode The inode
   *
   * @param is_write Whether the open flag contains O_WRONLY or O_RDWR
   *
   * @param guard If provided, where to return a guard temporarily
   * preventing modifications of the usage information of the same
   * inode (and, in the present implementation, inodes of the same
   * class)
   */
  virtual void UpdateOpened(
      InodeNum inode,
      bool is_write,
      boost::scoped_ptr<IInodeUsageGuard>* guard = 0) = 0;

  /**
   * Try to start a locked Setattr operation.  It succeeds if the
   * inode is not dirty.  In such case, the guard is set, so that one
   * can notify the end of the locked Setattr operation by resetting
   * the guard.  The guard can be omitted, in which case
   * StopLockedSetattr() can be called for the purpose.
   *
   * @param inode The inode
   *
   * @param guard Where to return the guard
   *
   * @return Whether the operation succeed
   */
  virtual bool StartLockedSetattr(
      InodeNum inode, boost::scoped_ptr<IInodeUsageGuard>* guard) = 0;

  /**
   * Stop a locked Setattr operation.  This should be called only if
   * after a successful StartLockedSetattr() where guard == 0.
   *
   * @param inode The inode
   */
  virtual void StopLockedSetattr(InodeNum inode) = 0;

  /**
   * Set an inode as dirty, after waiting until there is no lock
   * setattr operations in progress.  It is assumed that the caller
   * have called UpdateOpened() before, so that the internal data for
   * the inode in the IInodeUsageSet already exists and will not be
   * freed.
   *
   * @param inode The inode
   */
  virtual void SetDirty(InodeNum inode) = 0;

  /**
   * Decrement the use count for an inode.
   *
   * @param inode The inode
   *
   * @param is_write Whether the open flag contains O_WRONLY or O_RDWR
   *
   * @param clean_ret Where to return whether the inode has been
   * modified since this inode has become opened
   *
   * @param guard If provided, where to return a guard temporarily
   * preventing modifications of the usage information of the same
   * inode (and, in the present implementation, inodes of the same
   * class)
   *
   * @return 0 if the file is no longer being used.  1 if the client
   * switch from a writer to a non-writer.  If otherwise, i.e., the
   * file is still being used, and no switch from writer to non-writer
   * occurred, return kClientAccessUnchanged
   */
  virtual int UpdateClosed(
      InodeNum inode,
      bool is_write,
      bool* clean_ret,
      boost::scoped_ptr<IInodeUsageGuard>* guard = 0) = 0;
};

}  // namespace client
}  // namespace cpfs
