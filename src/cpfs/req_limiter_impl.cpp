/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of request limiter.
 */

#include "req_limiter_impl.hpp"

#include <stdint.h>

#include <algorithm>
#include <list>

#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/weak_ptr.hpp>

#include "fim.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_limiter.hpp"
#include "req_tracker.hpp"

namespace cpfs {

namespace {

/**
 * Implement the IReqLimiter interface.
 */
class ReqLimiter : public IReqLimiter {
 public:
  /**
   * @param tracker The tracker that the limiter forward requests to
   *
   *
   * @param max_send Maximum number of bytes to send without initial reply
   */
  ReqLimiter(const boost::weak_ptr<IReqTracker>& tracker, uint64_t max_send)
      : max_send_(max_send), data_mutex_(MUTEX_INIT),
        tracker_(tracker), bytes_to_reply_(0) {}

  bool Send(const boost::shared_ptr<IReqEntry>& entry,
            boost::unique_lock<MUTEX_TYPE>* lock);

 private:
  uint64_t max_send_; /**< Maximum number of bytes to send without reply */
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  boost::weak_ptr<IReqTracker> tracker_; /**< Tracker to forward entries to */
  uint64_t bytes_to_reply_; /**< Number of bytes waiting for reply */
  std::list<boost::condition_variable*> waiting_; /**< Waiting queue */

  void ReqAcked(uint64_t size);

  void TryWake_() {
    if (!waiting_.empty())
      waiting_.front()->notify_one();
  }
};

bool ReqLimiter::Send(const boost::shared_ptr<IReqEntry>& entry,
                      boost::unique_lock<MUTEX_TYPE>* lock) {
  MUTEX_LOCK(boost::unique_lock, data_mutex_, data_lock);
  boost::shared_ptr<IReqTracker> tracker = tracker_.lock();
  if (!tracker)
    return false;
  // Max with 4096 to bound number of outstanding Fims, to account for
  // Fim processing overheads.  This is also needed for test 402 to
  // run, otherwise too many Fims are accumulated to be redirected
  // before the next failover
  uint64_t size = std::max(uint64_t(entry->request()->len()), uint64_t(4096));
  if (!waiting_.empty() || (bytes_to_reply_ + size > max_send_ && size > 0)) {
    boost::condition_variable wake;
    waiting_.push_back(&wake);
    do {
      MUTEX_WAIT(data_lock, wake);
    } while (bytes_to_reply_ + size > max_send_ && size > 0);
    // &wake is at front, since only waiting_.front() ever gets notified
    waiting_.pop_front();
  }
  bool ret;
  {
    boost::unique_lock<MUTEX_TYPE> tracker_lock;
    if (!lock)
      lock = &tracker_lock;
    ret = tracker->AddRequestEntry(entry, lock);
    if (ret) {
      entry->OnAck(boost::bind(&ReqLimiter::ReqAcked, this, size));
      bytes_to_reply_ += size;
    }
  }
  TryWake_();
  return ret;
}

void ReqLimiter::ReqAcked(uint64_t size) {
  MUTEX_LOCK_GUARD(data_mutex_);
  bytes_to_reply_ -= size;
  TryWake_();
}

}  // namespace

IReqLimiter* MakeReqLimiter(boost::weak_ptr<IReqTracker> tracker,
                            uint64_t max_send) {
  return new ReqLimiter(tracker, max_send);
}

}  // namespace cpfs
