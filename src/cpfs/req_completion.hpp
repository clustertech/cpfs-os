#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define request completion checking facilities.
 */

#include <vector>

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "req_entry.hpp"

namespace cpfs {

class IFimSocket;

/**
 * Callback method type.
 */
typedef boost::function<void()> ReqCompletionCallback;

/**
 * Allow a handler to be executed if all registered requests are
 * replied.
 */
class IReqCompletionChecker {
 public:
  virtual ~IReqCompletionChecker() {}

  /**
   * Register a request to be checked for completion.
   *
   * @param req_entry The request to register
   */
  virtual void RegisterReq(const boost::shared_ptr<IReqEntry>& req_entry) = 0;

  /**
   * Signal the completion of a request.
   *
   * @param req_entry The request to signal completion
   */
  virtual void CompleteReq(const boost::shared_ptr<IReqEntry>& req_entry) = 0;

  /**
   * Register a callback to run once all previously registered
   * requests are replied.  If there is no request registered, the
   * method will run synchronously.  Otherwise it will run by the
   * thread calling the AddReply() method of the request tracker
   * causing it to become true.
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
   * Get a callback suitable to be passed to AddRequest() method of
   * the request tracker for a request to support completion checking.
   * The existence of a copy of such callback does not stop the
   * completion checker from being removed, so one must call Get() to
   * get hold of a reference to the completion checker before calling
   * this.  When the method is called, it optionally sends a final
   * reply to the originator, and marks the request as completed.
   *
   * @param inode The inode number
   *
   * @param originator The originator of the request.  If not empty,
   * this socket will receive a final reply when the callback is
   * called
   */
  virtual ReqAckCallback GetReqAckCallback(
      InodeNum inode,
      const boost::shared_ptr<IFimSocket>& originator) = 0;

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
