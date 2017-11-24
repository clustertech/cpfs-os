#pragma once

/* Copyright 2017 ClusterTech Ltd */

/**
 * @file
 *
 * Define DSG operation state class.
 */

#include <vector>

#include <boost/function.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "common.hpp"
#include "op_completion.hpp"

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
   * Notify that an inode operation is being started.
   *
   * Here "operation" means something that resync needs to wait for.
   * At present it means truncation.
   *
   * @param inode The inode having the operation
   *
   * @param op The operation, in the sense of IOpCompletionChecker
   */
  virtual void RegisterInodeOp(InodeNum inode, const void* op) = 0;

  /**
   * Notify that an inode operation is completed.
   *
   * Here "operation" means something that resync needs to wait for.
   * At present it means truncation.
   *
   * @param inode The inode having the operation
   *
   * @param op The operation, in the sense of IOpCompletionChecker
   */
  virtual void CompleteInodeOp(InodeNum inode, const void* op) = 0;

  /**
   * Run a callback when operations of an inode have all completed.
   *
   * @param inodes The inodes to wait for
   *
   * @param callback The callback to run
   */
  virtual void OnInodesCompleteOp(const std::vector<InodeNum> inodes,
                                  OpCompletionCallback callback) = 0;

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
   * Set the inodes as resyncing.
   *
   * @param group The group to set the pending inodes
   *
   * @param inodes The inodes resyncing, cleared after the call
   */
  virtual void SetDsgInodesResyncing(
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
