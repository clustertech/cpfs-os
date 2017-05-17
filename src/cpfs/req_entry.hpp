#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IReqEntry to represent requests to be queued to request
 * trackers.
 */

#include <stdint.h>

#include <boost/atomic.hpp>

#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "mutex_util.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFim;
class IReqEntry;

/**
 * Generate request ID.  This class is thread-safe: multiple threads
 * may call the GetId() method simultaneously.  The SetClientNum()
 * method may be called simultaneously as well, but only the first
 * call has effect (the other either fail or succeed with no effect,
 * depending on whether a new client number is set).  The intention is
 * that multiple request trackers can use one request ID generator so
 * as to ensure that all generated IDs are unique.
 */
class ReqIdGenerator {
 public:
  ReqIdGenerator();

  /**
   * Set the client number.  This affects the initial request ID
   * returned.  If kNotClient, not a client.  If set to a different
   * client number previous, throw a std::runtime_error.
   *
   * @param client_num The new client number
   */
  void SetClientNum(ClientNum client_num);

  /**
   * Generate a request ID.  This will block if SetClientNum() is
   * never called.  Otherwise it always generate a new ID.
   */
  ReqId GenReqId();

 private:
  MUTEX_TYPE data_mutex_; /**< Protect fields below, not used in fast path */
  boost::condition inited_; /**< SetClientNum() is called */
  ClientNum client_num_; /**< The client number */
  boost::atomic<bool> initialized_;  /**< Whether SetClientNum() is called */
  boost::atomic<ReqId> last_id_; /**< Next Id to generate */
};

/**
 * Type for acknowledgement callback.  The callback is called when the
 * reply of a request is added, by the thread calling
 * IReqTracker::AddReply.
 */
typedef boost::function<void(const boost::shared_ptr<IReqEntry>&)>
ReqAckCallback;

/**
 * Interface for a request tracker entry.
 */
class IReqEntry : public boost::enable_shared_from_this<IReqEntry> {
 public:
  virtual ~IReqEntry() {}

  /**
   * @return The request ID of the entry
   */
  virtual ReqId req_id() const = 0;

  /**
   * Get the stored request.  Not all requests are stored: if a
   * request has been sent once, and the request tracker believes that
   * the request will never be resent, the request is no longer
   * stored.  In that case this function returns the empty pointer.
   *
   * @return The stored request
   */
  virtual FIM_PTR<IFim> request() const = 0;

  /**
   * @return The reply
   */
  virtual const FIM_PTR<IFim>& reply() const = 0;

  /**
   * @return The reply timestamp number
   */
  virtual uint64_t reply_no() const = 0;

  /**
   * Wait until the request is replied.
   */
  virtual const FIM_PTR<cpfs::IFim>& WaitReply() = 0;

  /**
   * Add a callback to be called on acknowledgement.  Multiple
   * callbacks may be added to the same entry.  The caller must ensure
   * that the entry lock obtained via the AddRequest() or
   * AddRequestEntry() method of the request tracker is still held
   * when adding an ack callback.
   *
   * @param callback The acknowledgement callback, to be called when
   * AckRequest is called on the same request ID
   *
   * @param final Whether the callback should be triggered only when
   * the final reply arrives
   */
  virtual void OnAck(ReqAckCallback callback, bool final = false) = 0;

  // API for ReqTracker

  /**
   * @param lock The lock to hold the record mutex
   */
  virtual void FillLock(boost::unique_lock<MUTEX_TYPE>* lock) = 0;

  /**
   * @param sent Whether to mark the entry as sent
   */
  virtual void set_sent(bool sent) = 0;

  /**
   * @return Whether the entry is marked as sent
   */
  virtual bool sent() const = 0;

  /**
   * @param reply_error Reply error state
   */
  virtual void set_reply_error(bool reply_error) = 0;

  /**
   * Set the reply object of the request entry, thereby acknowledging
   * it.  The waiters are woken up, and ack callback is invoked.  It
   * is legal to call only if the calling thread does not hold any
   * mutex.
   *
   * @param reply The reply object.
   *
   * @param reply_no The reply timestamp number
   */
  virtual void SetReply(
      const FIM_PTR<IFim>& reply, uint64_t reply_no) = 0;

  /**
   * Hook to run before queueing the request Fim.
   */
  virtual void PreQueueHook() = 0;

  /**
   * Whether Fim entry is treated as persistent across FimSockets.  It
   * affects the processing as follows.  (1) The request tracker drops
   * the stored request of non-persistent entries once it is sent the
   * first time.  (2) On missing FimSocket, non-persistent entries are
   * dropped while persistent entries are left for later resends.  (3)
   * Persistent entries requires a final Fim to be supplied via
   * AddReply() before they are removed, non-persistent entries allows
   * any reply Fim ignoring whether it is final or not.  In case (2),
   * FimSocket is determined to be missing if (a) the entry is added
   * when no FimSocket is set and it is not expecting new FimSocket,
   * or (b) the entry is there when an existing FimSocket is removed,
   * or (c) the entry is there and no FimSocket is set when the the
   * tracker is set to not expecting new FimSocket.
   *
   * @return Whether resending is possible
   */
  virtual bool IsPersistent() const = 0;

  /**
   * Unset the stored request, allowing the system to free up memory
   * if the Fim is no longer used.
   */
  virtual void UnsetRequest() = 0;

  /**
   * Dump stored information to log.
   */
  virtual void Dump() const = 0;
};

}  // namespace cpfs
