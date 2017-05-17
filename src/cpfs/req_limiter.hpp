#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IReqLimiter to limit the amount of outstanding requests.
 */

#include <boost/thread/mutex.hpp>

#include "mutex_util.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFim;
class IReqEntry;

/**
 * A request limiter can be used to send requests via an underlying
 * request tracker.  The requests are forwarded to the tracker, where
 * the amount of data that has not been initial-replied is tallied.
 * Once this amount exceed a threshold, sending will block, wait until
 * some of the requests are replied, before proceeding to send the
 * request to the tracker.
 */
class IReqLimiter {
 public:
  virtual ~IReqLimiter() {}

  /**
   * Send an entry using the limiter.
   *
   * @param entry The request entry
   *
   * @param lock Lock that will be filled to prevent request entry
   * passed to be manipulated until released.  If 0, use an internal
   * lock that will be released on return of the function
   *
   * @return Whether the request entry is added
   */
  virtual bool Send(const boost::shared_ptr<IReqEntry>& entry,
                    boost::unique_lock<MUTEX_TYPE>* lock = 0) = 0;
};

}  // namespace cpfs
