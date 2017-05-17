/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of request entry.
 */

#include "req_entry_impl.hpp"

#include <stdint.h>

#include <cstddef>
#include <stdexcept>
#include <vector>

#include <boost/atomic.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/weak_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_tracker.hpp"

namespace cpfs {

namespace {

/**
 * Used for returning empty request and responses.  Once set, the
 * request and responses will not be reset, and thus can be returned
 * via const refs.  But before then, we need a stable object for the
 * purpose.
 */
const FIM_PTR<IFim> empty_ifim;

/**
 * A base class for request tracker entry classes.
 */
class BaseReqEntry : public IReqEntry {
 public:
  /**
   * @param tracker The request tracker using the entry
   *
   * @param request The Fim object in the entry
   *
   * @param etype The entry type printed upon dumping
   */
  BaseReqEntry(const boost::weak_ptr<IReqTracker>& tracker,
               const FIM_PTR<IFim>& request,
               char etype = 'B')
      : etype_(etype), tracker_(tracker),
        data_mutex_(MUTEX_INIT),
        req_id_(request->req_id()), request_(request),
        sent_(false), reply_error_(false), reply_no_(0) {}

  ReqId req_id() const {
    return req_id_;
  }

  FIM_PTR<IFim> request() const {
    return request_;
  }

  const FIM_PTR<IFim>& reply() const {
    return reply_ ? reply_ : empty_ifim;
  }

  uint64_t reply_no() const {
    return reply_no_;
  }

  void set_sent(bool sent) {
    sent_ = sent;
  }

  bool sent() const {
    return sent_;
  }

  void set_reply_error(bool reply_error) {
    reply_error_ = reply_error;
  }

  void SetReply(const FIM_PTR<IFim>& reply, uint64_t reply_no) {
    std::vector<ReqAckCallback> callbacks;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      reply_no_ = reply_no;
      if (!reply)
        set_reply_error(true);
      else if (reply->type() != kFinalReplyFim)
        reply_ = reply;
      else if (!reply_)
        throw std::runtime_error("Final reply Fim received before init reply");
      callbacks = callbacks_;
      callbacks_.clear();  // Remove reference
      if (!reply || reply->type() == kFinalReplyFim || reply->is_final()) {
        callbacks.insert(callbacks.end(),
                         f_callbacks_.begin(), f_callbacks_.end());
        f_callbacks_.clear();
      }
      if (callbacks.empty()) {
        waiting_.notify_all();  // Save once lock in common case
        return;
      }
    }
    // Call callback without holding the lock, but before notifying others
    for (std::size_t i = 0; i < callbacks.size(); ++i)
      callbacks[i](shared_from_this());
    MUTEX_LOCK_GUARD(data_mutex_);
    waiting_.notify_all();
  }

  const FIM_PTR<IFim>& WaitReply() {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    if (reply_error_)
      throw std::runtime_error("WaitReply failed");
    if (!reply_)
      MUTEX_WAIT(lock, waiting_);
    if (reply_error_)
      throw std::runtime_error("WaitReply failed");
    return reply_ ? reply_ : empty_ifim;
  }

  void FillLock(boost::unique_lock<MUTEX_TYPE>* lock) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, my_lock);
    lock->swap(my_lock);
  }

  void OnAck(ReqAckCallback callback, bool final) {
    if (final)
      f_callbacks_.push_back(callback);
    else
      callbacks_.push_back(callback);
  }

  void PreQueueHook() {}

  void UnsetRequest() {
    request_.reset();
  }

  void Dump() const {
    MUTEX_LOCK_GUARD(data_mutex_);
    LOG(notice, Fim, "Request entry: type ", PVal(etype_),
        ", request Id ", PHex(req_id_));
    LOG(notice, Fim, "  request ", PVal(request_), ", sent = ", PVal(sent_));
    LOG(notice, Fim, "  reply ", PVal(reply_), ", err = ", PVal(reply_error_));
    if (!callbacks_.empty() || !f_callbacks_.empty())
      LOG(notice, Fim, "  Number of callbacks (reg/fin): ",
          PVal(callbacks_.size()), "/", PVal(f_callbacks_.size()));
  }

 protected:
  /**
   * Assign request ID for the request.
   */
  void AssignReqId() {
    req_id_ = tracker_.lock()->req_id_gen()->GenReqId();
    request_->set_req_id(req_id_);
  }

 private:
  char etype_; /**< Entry type */
  boost::weak_ptr<IReqTracker> tracker_; /**< The tracker using the entry */
  mutable MUTEX_TYPE data_mutex_;  /**< Protect the entry */
  ReqId req_id_; /**< The request ID */
  FIM_PTR<IFim> request_;  /**< The stored request object */
  FIM_PTR<IFim> reply_;  /**< The reply object */
  std::vector<ReqAckCallback> callbacks_;  /**< callbacks on reply */
  std::vector<ReqAckCallback> f_callbacks_;  /**< callbacks on final reply */
  boost::condition_variable waiting_;  /**< The condition to wait for */
  bool sent_; /**< Whether the request is marked as sent */
  bool reply_error_;  /**< Whether reply is failed */
  uint64_t reply_no_;  /**< The reply timestamp number */
};

/**
 * Default request entry class.  This is used for request entries that
 * create an ID and would wait in the queue for resends if FimSocket
 * is not found.
 */
class DefaultReqEntry : public BaseReqEntry {
 public:
  /**
   * @param tracker The request tracker using the entry
   *
   * @param request The Fim object in the entry.
   */
  DefaultReqEntry(const boost::weak_ptr<IReqTracker>& tracker,
                  const FIM_PTR<IFim>& request) :
      BaseReqEntry(tracker, request, 'D') {}

  void PreQueueHook() {
    AssignReqId();
  }

  bool IsPersistent() const {
    return true;
  }
};

/**
 * Request entry class for transient requests.  This is used for
 * request entries that create an ID and would simply be skipped if
 * FimSocket is not found.
 */
class TransientReqEntry : public BaseReqEntry {
 public:
  /**
   * @param tracker The request tracker using the entry
   *
   * @param request The Fim object in the entry.
   */
  TransientReqEntry(const boost::weak_ptr<IReqTracker>& tracker,
                    const FIM_PTR<IFim>& request) :
      BaseReqEntry(tracker, request, 'T') {}

  void PreQueueHook() {
    AssignReqId();
  }

  bool IsPersistent() const {
    return false;
  }
};

/**
 * Request entry class for replication requests.  This is used for
 * request entries that reuse the request ID and would simply be
 * skipped if FimSocket is not found.
 */
class ReplReqEntry : public BaseReqEntry {
 public:
  /**
   * @param tracker The request tracker using the entry
   *
   * @param request The Fim object in the entry.
   */
  ReplReqEntry(const boost::weak_ptr<IReqTracker>& tracker,
               const FIM_PTR<IFim>& request) :
      BaseReqEntry(tracker, request, 'R') {}

  bool IsPersistent() const {
    return false;
  }
};

}  // namespace

boost::shared_ptr<IReqEntry> MakeDefaultReqEntry(
    const boost::weak_ptr<IReqTracker>& tracker,
    const FIM_PTR<IFim>& request) {
  return boost::make_shared<DefaultReqEntry>(tracker, request);
}

boost::shared_ptr<IReqEntry> MakeTransientReqEntry(
    const boost::weak_ptr<IReqTracker>& tracker,
    const FIM_PTR<IFim>& request) {
  return boost::make_shared<TransientReqEntry>(tracker, request);
}

/**
 * Create a request entry for replication requests.  This is used for
 * request entries that reuse the request ID and would simply be
 * skipped if FimSocket is not found.
 */
boost::shared_ptr<IReqEntry> MakeReplReqEntry(
    const boost::weak_ptr<IReqTracker>& tracker,
    const FIM_PTR<IFim>& request) {
  return boost::make_shared<ReplReqEntry>(tracker, request);
}

ReqIdGenerator::ReqIdGenerator()
    : data_mutex_(MUTEX_INIT),
      client_num_(kUnknownClient), initialized_(false) {}

void ReqIdGenerator::SetClientNum(ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (initialized_) {
    if (client_num != client_num_)
      throw std::runtime_error("SetClientNum failed: set previously");
    return;
  }
  client_num_ = client_num;
  last_id_ = (uint64_t(client_num) << (64U - kClientBits));
  initialized_ = true;  // Do this last to make fast path in GenReqId() correct
  inited_.notify_all();
}

ReqId ReqIdGenerator::GenReqId() {
  if (!initialized_) {  // Block, fast (initialized) path won't lock
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    while (!initialized_)
      inited_.wait(lock);
  }
  return ++last_id_;
}

}  // namespace cpfs
