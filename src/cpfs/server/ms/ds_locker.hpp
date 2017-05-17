#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Declare IDSLocker, an interface for DS locking in the
 * meta-server.  This is splited from worker mainly to make the
 * unit test workflow more manageable.
 */

#include <vector>

#include "common.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {
namespace server {
namespace ms {

/**
 * Interface for acquiring DS locks.  It can be used to acquire locks,
 * and once completed, destruction of the lock object will ensure that
 * the lock is unlocked.  They are used by DSLocker to ensure that
 * no locks are leaked.
 */
class IMetaDSLock {
 public:
  virtual ~IMetaDSLock() {}

  /**
   * Prepare for acquiring the lock, but don't wait for completion of
   * the operation.
   */
  virtual void PrepareAcquire() = 0;

  /**
   * Wait for existing acquire operation has complete.  If there is no
   * acquire operation in progress, do nothing.
   */
  virtual void WaitAcquireCompletion() = 0;

  /**
   * Prepare for releasing the lock, but don't wait for completion of
   * the operation.
   */
  virtual void PrepareRelease() = 0;
};

/**
 * Interface for DS locking within meta server.
 */
class IDSLocker {
 public:
  virtual ~IDSLocker() {}

  /**
   * Lock the inode in all DS storing it. Exception is thrown if lock fails.
   *
   * @param inode The inode to lock
   *
   * @param group_ids The group IDs of the groups storing the inode
   *
   * @param locks_ret Whether to return locks
   */
  virtual void Lock(InodeNum inode,
                    const std::vector<GroupId>& group_ids,
                    std::vector<boost::shared_ptr<IMetaDSLock> >*
                    locks_ret) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
