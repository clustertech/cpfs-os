#pragma once

/* Copyright 2017 ClusterTech Ltd */

/**
 * @file
 *
 * Define DSG operation state class.
 */

#include <vector>

#include <boost/thread/shared_mutex.hpp>

#include "common.hpp"

namespace cpfs {

class IOpCompletionCheckerSet;

namespace server {
namespace ms {

/**
 * DSG operation states manager.
 */
class IDSGOpStateMgr {
 public:
  virtual ~IDSGOpStateMgr() {}

  /**
   * @param checker_set The completion checker set for DS operations
   */
  virtual void set_completion_checker_set(
      IOpCompletionCheckerSet* checker_set) = 0;
  /**
   * @return Completion checker set for DS operations
   */
  virtual IOpCompletionCheckerSet* completion_checker_set() = 0;

  /**
   * Prepare for reading of DSG operations state.
   *
   * @param group The group to read the operation state
   *
   * @param lock The lock to keep others from writing to the DSG ops state
   */
  virtual void ReadLock(
      GroupId group, boost::shared_lock<boost::shared_mutex>* lock) = 0;

  /**
   * Set the inodes to be resyncing.
   *
   * @param group The group to set the pending inodes
   *
   * @param inodes The inodes resyncing, cleared after the call
   */
  virtual void set_dsg_inodes_resyncing(
      GroupId group, const std::vector<InodeNum>& inodes) = 0;

  /**
   * Return whether an inode is to be resync'ed
   *
   * This function should be called with the LockDSOpState read lock
   * held (see ReadLockDSOpState()).
   *
   * @param group The group to check for inode resync
   *
   * @param inode The inode to check
   *
   * @return Whether the inode is to be resync'ed
   */
  virtual bool is_dsg_inode_resyncing(GroupId group, InodeNum inode) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
