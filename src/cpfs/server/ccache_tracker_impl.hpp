#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the ICCacheTracker interface.
 */

namespace cpfs {
namespace server {

class ICCacheTracker;

/**
 * Create an instance of ICCacheTracker.  The implementation is thread
 * safe.
 */
ICCacheTracker* MakeICacheTracker();

}  // namespace server
}  // namespace cpfs
