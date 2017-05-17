#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define event facility.
 */

#include <boost/thread/condition.hpp>

#include "mutex_util.hpp"

namespace cpfs {

/**
 * A one-off use facility to convert asynchronous API to synchronous.
 * Users would create an Event as a local variable, create callback
 * functions using boost::bind to call its Invoke() method, and then
 * call Wait() immediately to wait until that happens.  After that the
 * event variable is no longer useful
 */
class Event {
 public:
  Event() : data_mutex_(MUTEX_INIT), invoked_(false) {}

  /**
   * Invoke the event.  Repeated calls has no effect.
   */
  void Invoke() {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (invoked_)
      return;
    invoked_ = true;
    wake_.notify_one();
  }

  /**
   * Wait until the event is invoked.  If already invoked, it returns
   * immediately.
   */
  void Wait() {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    while (!invoked_)
      wake_.wait(lock);
  }

 private:
  MUTEX_TYPE data_mutex_; /**< Protect fields below */
  boost::condition wake_; /**< Condition to wait for */
  bool invoked_; /**< Whether Invoke() has been called */
};

}  // namespace cpfs
