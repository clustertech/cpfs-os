#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICCacheTracker interface.
 */

#include <cstddef>

#include <boost/function.hpp>

#include "common.hpp"

namespace cpfs {
namespace server {

/**
 * Function to run on cache expiry.
 */
typedef boost::function<void(InodeNum, ClientNum)> CCacheExpiryFunc;

/**
 * Cache invalidation function to be used when making Worker's.
 * Useful only for unit tests of subclasses: They can avoid calling
 * the actual argument to the ccache_tracker, but instead check that
 * the CacheInvalFunc is called as expected.
 *
 * @param func The function to set.
 */
typedef boost::function<void(InodeNum, bool)> CacheInvalFunc;

/**
 * Interface for servers to keep track of what is stored in client
 * caches.
 */
class ICCacheTracker {
 public:
  virtual ~ICCacheTracker() {}

  /**
   * Return number of entries.  Mostly for test.
   */
  virtual std::size_t Size() const = 0;

  /**
   * Remove all records for a client.
   *
   * @param client_num The client number.
   */
  virtual void RemoveClient(ClientNum client_num) = 0;

  /**
   * Set status of a cache entry for a client on an inode.
   *
   * @param client_num The client number.
   *
   * @param inode_num The inode number.
   */
  virtual void SetCache(InodeNum inode_num, ClientNum client_num) = 0;

  /**
   * Remove cache entries for an inode.
   *
   * @param inode_num The inode number.
   *
   * @param except A client number that is excempted from
   * invalidation.  May use kNotClient if no client is to be
   * excempted.
   *
   * @param expiry_func What to do when a cache entry is to be expired.
   */
  virtual void InvGetClients(InodeNum inode_num,
                             ClientNum except,
                             CCacheExpiryFunc expiry_func
                             = CCacheExpiryFunc()) = 0;

  /**
   * Expire old cache entries.
   *
   * @param min_age Minimum age of cache entries to expire, in seconds.
   *
   * @param expiry_func What to do when a cache entry is to be expired.
   */
  virtual void ExpireCache(int min_age,
                           CCacheExpiryFunc expiry_func
                           = CCacheExpiryFunc()) = 0;
};

}  // namespace server
}  // namespace cpfs
