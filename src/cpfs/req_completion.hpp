#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define request/operation completion checking facilities.
 */

#include <vector>

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"

namespace cpfs {

class IFimSocket;

/**
 * Callback method type.
 */
typedef boost::function<void()> ReqCompletionCallback;

/**
 * Allow a handler to be executed if all registered operations are completed.
 *
 * The name ReqCompletionChecker reflects the usual usage that the
 * operations are requests being replicated.  But the class may be
 * used for other operations as well.
 */
class IReqCompletionChecker {
 public:
  virtual ~IReqCompletionChecker() {}

  /**
   * Register an operation to be checked for completion.
   *
   * @param op An object representing the operation to register
   */
  virtual void RegisterOp(const void* op) = 0;

  /**
   * Signal the completion of an operation.
   *
   * @param op The operation completed
   */
  virtual void CompleteOp(const void* op) = 0;

  /**
   * Register a callback to run once all previously registered ops complete.
   *
   * If there is no operations registered, the method will run
   * synchronously.  Otherwise it will run by the thread calling the
   * AddReply() method of the request tracker causing it to become
   * true.
   *
   * @param callback The callback to call when that happen
   */
  virtual void OnCompleteAll(ReqCompletionCallback callback) = 0;

  /**
   * Check whether all registered requests have been completed.
   */
  virtual bool AllCompleted() = 0;
};

/**
 * Represent a set of IReqCompletionChecker's indexed by inode numbers
 * created as necessary.  The completion checkers may be removed after
 * they are no longer used.
 */
class IReqCompletionCheckerSet {
 public:
  virtual ~IReqCompletionCheckerSet() {}

  /**
   * Get a completion checker.  When a copy of the returned shared
   * pointer of completion checker still exists, the completion
   * checker won't be released.
   *
   * @param inode The inode number
   */
  virtual boost::shared_ptr<IReqCompletionChecker> Get(InodeNum inode) = 0;

  /**
   * Complete an operation.
   *
   * Notify operation completion for the checker of an inode, and
   * remove the completion checker if it is safe to do so.
   *
   * @param inode The inode number
   *
   * @param op The operation completed
   */
  virtual void CompleteOp(InodeNum inode, const void* op) = 0;

  /**
   * Register a callback to run once all previously registered
   * requests of an inode is replied.  If there is no request
   * registered, the method will run synchronously.  Otherwise it will
   * run by the thread calling the AddReply() method of the request
   * tracker causing it to become true.
   *
   * @param inode The inode
   *
   * @param callback The callback to call when that happen
   */
  virtual void OnCompleteAll(InodeNum inode,
                             ReqCompletionCallback callback) = 0;

  /**
   * Register a callback to run once all previously registered
   * requests of all inodes are replied.  If there is no request
   * registered, the method will run synchronously.  Otherwise it will
   * run by the thread calling the AddReply() method of the request
   * tracker causing it to become true.
   *
   * @param callback The callback to call when that happen
   */
  virtual void OnCompleteAllGlobal(ReqCompletionCallback callback) = 0;

  /**
   * Register a callback to run once all previously registered
   * requests of all inodes in a subset are replied.  If there is no
   * request registered, the method will run synchronously.  Otherwise
   * it will run by the thread calling the AddReply() method of the
   * request tracker causing it to become true.
   *
   * @param subset The subset of inodes to wait
   *
   * @param callback The callback to call when that happen
   */
  virtual void OnCompleteAllSubset(
      const std::vector<InodeNum>& subset, ReqCompletionCallback callback) = 0;
};

}  // namespace cpfs
