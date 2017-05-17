#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define request tracker related facilities.
 */

#include <stdint.h>

#include <string>

#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "mutex_util.hpp"

namespace cpfs {

class IFim;
class IFimSocket;
class IReqEntry;
class IReqLimiter;
class IReqTracker;
class ReqIdGenerator;

/**
 * Functor type to get redirection target given a Fim.
 */
typedef boost::function<boost::shared_ptr<IReqTracker>(
    FIM_PTR<IFim>)> ReqRedirector;

/**
 * Interface for request tracker.  A request tracker contains set of
 * Fim objects that are awaiting full acknowledgement, and provide
 * interfaces for replies to be matched with requests.
 */
class IReqTracker : public boost::enable_shared_from_this<IReqTracker> {
 public:
  virtual ~IReqTracker() {}

  /**
   * @return A human readable description of the request tracker
   */
  virtual std::string name() const = 0;

  /**
   * Set max_send to use when creating new ReqLimiter's.  If never
   * called, this value defaults to 256M.  Should be called only
   * before GetReqLimiter() is first called.
   *
   * @param max_send max_send to use
   */
  virtual void set_limiter_max_send(uint64_t max_send) = 0;

  /**
   * @return The Request ID generator
   */
  virtual ReqIdGenerator* req_id_gen() = 0;

  /**
   * @return The client number.
   */
  virtual ClientNum peer_client_num() = 0;

  /**
   * Set the Fim socket for sending Fim's.
   *
   * @param fim_socket The Fim socket.  A null value means the
   * connection is not ready yet
   *
   * @param plug Whether to automatically do Plug(), in case
   * fim_socket is not empty.  If fim_socket is empty it is always
   * unplugged
   */
  virtual void SetFimSocket(const boost::shared_ptr<IFimSocket>& fim_socket,
                            bool plug = true) = 0;

  /**
   * @return The Fim socket.  A null value means the connection is not
   * ready yet.
   */
  virtual boost::shared_ptr<cpfs::IFimSocket> GetFimSocket() = 0;

  /**
   * @param expecting Whether the request tracker is expecting new
   * FimSocket to be set soon.  Initially the request tracker is not
   * expecting new FimSockets
   */
  virtual void SetExpectingFimSocket(bool expecting) = 0;

  /**
   * Add a request to the tracker.  A request ID will be allocated and
   * set to the Fim, and the Fim socket will be invoked for the
   * request (if it is not null).  Otherwise it will wait in the
   * request tracker and is sent when the FimSocket is set.
   *
   * @param request The request to add
   *
   * @param lock Lock that will be filled to prevent request entry
   * returned to be manipulated until released.  If 0, use an internal
   * lock that will be released on return of the function
   *
   * @return A request entry for waiting the completion of the request
   */
  virtual boost::shared_ptr<IReqEntry> AddRequest(
      const FIM_PTR<IFim>& request,
      boost::unique_lock<MUTEX_TYPE>* lock = 0) = 0;

  /**
   * Like AddRequest, but use a transient entry instead.
   */
  virtual boost::shared_ptr<IReqEntry> AddTransientRequest(
      const FIM_PTR<IFim>& request,
      boost::unique_lock<MUTEX_TYPE>* lock = 0) = 0;

  /**
   * Add a request entry to the tracker.
   *
   * @param entry The request entry
   *
   * @param lock Lock that will be filled to prevent request entry
   * passed to be manipulated until released.  If 0, use an internal
   * lock that will be released on return of the function
   *
   * @return Whether the request entry is added
   */
  virtual bool AddRequestEntry(
      const boost::shared_ptr<IReqEntry>& entry,
      boost::unique_lock<MUTEX_TYPE>* lock = 0) = 0;

  /**
   * Get an active request entry corresponding to a request ID.
   *
   * @param req_id The request ID
   *
   * @return The request entry
   */
  virtual boost::shared_ptr<IReqEntry> GetRequestEntry(ReqId req_id) const = 0;

  /**
   * Add a reply.  Find a matching request entry, run ack callback,
   * notifies waiters and remove request tracker entry if found to be
   * desirable.
   *
   * The actual logic would remove the entry if the request tracker is
   * held in an MS / DS.  It would also remove the entry if the final
   * flag is set in the reply.
   *
   * If a matching request entry is not found, a message is logged,
   * and no further processing is done.
   *
   * @param reply The reply Fim to add.
   */
  virtual void AddReply(const FIM_PTR<IFim>& reply) = 0;

  /**
   * Resend initially replied requests in the order of replies
   * received.
   *
   * @return Whether request(s) is sent
   */
  virtual bool ResendReplied() = 0;

  /**
   * Redirect one existing request.  Does nothing if the request is
   * not found.  The caller should ensure that the request is actually
   * stored in the entry.
   *
   * @param req_id The request ID of the request to redirect
   *
   * @param target The target tracker to redirect the request to
   */
  virtual void RedirectRequest(
      ReqId req_id, boost::shared_ptr<IReqTracker> target) = 0;

  /**
   * Redirect existing requests of the tracker to other request
   * trackers.  This migrates each request entry to its target request
   * tracker.  The redirection is done in the same order as Plug(),
   * where already replied Fims are redirected before Fims that have
   * never been replied.  Useful when starting DS degraded mode.
   *
   * @param redirector Callback to find the tracker to redirect to.
   * The Fim is redirected to the returned tracker.  If the returned
   * tracker is the same as the current tracker, or if it returns an
   * empty tracker, the Fim is dropped, emitting an error message
   *
   * @return Whether any request is redirected or dropped
   */
  virtual bool RedirectRequests(ReqRedirector redirector) = 0;

  /**
   * Set whether requests are sent to the FimSocket during
   * AddRequest().  If plug == true, the FimSocket should already be
   * set before calling Plug().  Previously queued data that is unsent
   * to the same FimSocket (e.g., by ResentReplied()) will be resent
   * if plug == true and a FimSocket is available.  If data is resent,
   * previously replied requests (if not already resent previously)
   * are resent first, in order of the reception of replies.  Other
   * requests are resent in order of ascending request IDs.
   *
   * @param plug Whether to send requests on AddRequest()
   */
  virtual void Plug(bool plug = true) = 0;

  /**
   * Get a request limiter for the tracker.  This will create a new
   * limiter if one has not been created yet, and will be saved for
   * all future calls.
   */
  virtual IReqLimiter* GetReqLimiter() = 0;

  /**
   * Dump all pending request entries to log.
   */
  virtual void DumpPendingRequests() const = 0;

  /**
   * Shutdown the tracker. Pending request entries will fail and be notified.
   */
  virtual void Shutdown() = 0;
};

}  // namespace cpfs
