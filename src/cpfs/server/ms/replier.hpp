#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IReplier.
 */

#include <boost/function.hpp>
#include <boost/noncopyable.hpp>

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFim;
class IFimSocket;
struct OpContext;

namespace server {
namespace ms {

class BaseMetaServer;

/** Type for callback for work once on completion */
typedef boost::function<void()> ReplierCallback;

/**
 * The replier to use for meta-directory.  This performs replication
 * for successful modification requests on an active MS if a standby
 * is available, defers reply until replication is completed in this
 * case, recording reply in recent reply set for standby MS, and
 * logging error messages when replying errors to replicated messages.
 */
class IReplier : boost::noncopyable {
 public:
  virtual ~IReplier() {}

  /**
   * Arrange to reply to a request.  Optionally, a callback can be
   * attached, so that recent active_complete_callback's are re-run by
   * a newly elected active after a failover.  This ensures that the
   * work needed are actually performed completely even upon failures.
   *
   * @param req The request being replied
   *
   * @param reply The reply to make
   *
   * @param peer The peer sending the request
   *
   * @param op_context The operation context of the operation
   * generating the reply
   *
   * @param active_complete_callback A callback function to be run by the
   * active MS at least once, and will be called with no mutex locked.
   * If empty callback, no callback to run.
   */
  virtual void DoReply(const FIM_PTR<IFim>& req,
                       const FIM_PTR<IFim>& reply,
                       const boost::shared_ptr<IFimSocket>& peer,
                       const OpContext* op_context,
                       ReplierCallback active_complete_callback) = 0;

  /**
   * Used by MS Cleaner to expire active complete callbacks stored.
   * Callbacks are stored when DoReply() is called.
   *
   * @param age The maximum age of callbacks
   */
  virtual void ExpireCallbacks(int age) = 0;

  /**
   * Used by FailoverMgr to run active complete callbacks during
   * failover.  It is legal to call only if the calling thread does
   * not hold any mutex.
   */
  virtual void RedoCallbacks() = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
