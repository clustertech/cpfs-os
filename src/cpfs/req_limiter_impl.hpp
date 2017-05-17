#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of request limiter.
 */

#include <stdint.h>

namespace boost {

template <class Y> class weak_ptr;

}  // namespace boost

namespace cpfs {

class IReqLimiter;
class IReqTracker;

/**
 * Create a request limiter.
 *
 * @param tracker The tracker that the limiter forward requests to
 *
 * @param max_send Maximum number of bytes to send without initial reply
 */
IReqLimiter* MakeReqLimiter(boost::weak_ptr<IReqTracker> tracker,
                            uint64_t max_send = 256 * 1024 * 1024);

}  // namespace cpfs
