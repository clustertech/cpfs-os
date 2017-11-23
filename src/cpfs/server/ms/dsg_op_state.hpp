#pragma once

/* Copyright 2017 ClusterTech Ltd */

/**
 * @file
 *
 * Define DSG operation state class.
 */

#include <vector>

#include <boost/function.hpp>

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
   * Wait until the inode is not resyncing in all groups before the
   * registration.  Care has been taken to allow this to be called
   * concurrently with SetDsgInodesResyncing / OnInodesCompleteOp
   * pairs: SetDsgInodesResyncing may run at nearly any time, but if
   * if RegisterInodeOp is completed before that, it is guaranteed
   * that the subsequent OnInodesCompleteOp can see this registration.
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
   * Set the inodes as resyncing.
   *
   * @param group The group to set the pending inodes
   *
   * @param inodes The inodes resyncing, cleared after the call
   */
  virtual void SetDsgInodesResyncing(
      GroupId group, const std::vector<InodeNum>& inodes) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
