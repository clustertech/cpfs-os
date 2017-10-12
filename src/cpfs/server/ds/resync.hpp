#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the interfaces for classes handling DS resync operations.
 */

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_processor.hpp"
#include "shaped_sender.hpp"

namespace cpfs {

class IReqTracker;

namespace server {
namespace ds {

class BaseDataServer;

/**
 * Actually send the resync Fims to the target.
 */
class IResyncSender {
 public:
  virtual ~IResyncSender() {}

  /**
   * Run the sender.
   */
  virtual void Run() = 0;

  /**
   * Change how to create shaped senders.  Must be called before Run().
   *
   * @param s_sender_maker The sender maker to use
   */
  virtual void SetShapedSenderMaker(ShapedSenderMaker s_sender_maker) = 0;

  // The following are more private functions exported mostly for unit tests

  /**
   * Send the inode directory requests.  The store is listed to find
   * all inodes kept by the DS, and a DSResyncDirFim is sent for each
   * of them telling the receiver the availability of that inode in
   * the DS, and request for a reply.  The reply allows these Fims to
   * be shaped, to avoid overwhelming the target with a huge number of
   * Fims.
   *
   * @param target_tracker The tracker used to send the requests
   */
  virtual void SendDirFims(boost::shared_ptr<IReqTracker> target_tracker) = 0;

  /**
   * Request for the next resync phase and wait for reply.
   *
   * @param target_tracker The tracker used to send the requests
   *
   * @return Number of inodes to resync
   */
  virtual size_t ReadResyncPhase(
      boost::shared_ptr<IReqTracker> target_tracker) = 0;

  /**
   * Send data removal requests.
   *
   * @param target_tracker The tracker used to send the requests
   */
  virtual void SendDataRemoval(
      boost::shared_ptr<IReqTracker> target_tracker) = 0;

  /**
   * Send all resync requests to resync a replacement DS.
   *
   * @param target_tracker The tracker used to send the requests
   */
  virtual void SendAllResync(boost::shared_ptr<IReqTracker> target_tracker) = 0;
};

/**
 * Type for changing the behavior of the ResyncMgr, used only in
 * unit tests.
 */
typedef boost::function<
  IResyncSender* (BaseDataServer* server, GroupRole target)
> ResyncSenderMaker;

/**
 * Interface for a class to create threads that send DS resync Fims to
 * a previously failed DS.  Such threads can be started multiple times
 * to do multiple resync operations, although at the same time there
 * can only be at most one started thread.  The thread is terminated
 * once the resync operation is completed, or if the target DS Fim
 * socket is removed.
 */
class IResyncMgr {
 public:
  virtual ~IResyncMgr() {}

  /**
   * Start the resync sender sending data to a failed role.  If
   * already started, do nothing.
   *
   * @param target The failed role
   */
  virtual void Start(GroupRole target) = 0;

  /**
   * Check whether the sender is started.
   *
   * @param target_ret The target of the active resync when Start() is
   * called
   *
   * @return Whether the sender is started
   */
  virtual bool IsStarted(GroupRole* target_ret) = 0;

  /**
   * Change how to do the actual work of resync.  Must be called
   * before calling other methods.
   *
   * @param sender_maker The sender maker to use
   */
  virtual void SetResyncSenderMaker(ResyncSenderMaker sender_maker) = 0;

  /**
   * Change how to create shaped senders.  Must be called before
   * calling other methods.
   *
   * @param s_sender_maker The sender maker to use
   */
  virtual void SetShapedSenderMaker(ShapedSenderMaker s_sender_maker) = 0;
};

/**
 * Function for DS resync completion handlers.
 */
typedef boost::function<void(bool success)> ResyncCompleteHandler;

/**
 * Interface for FimProcessor for handling DS resync Fims.
 */
class IResyncFimProcessor : public IFimProcessor {
 public:
  /**
   * Initialize the DS resync process.
   *
   * @param completion_handler Function to call once the resync
   * operation completes
   */
  virtual void AsyncResync(ResyncCompleteHandler completion_handler) = 0;
};

}  // namespace ds
}  // namespace server
}  // namespace cpfs
