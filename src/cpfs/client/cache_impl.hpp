#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of interface for client-side cache
 * management.
 */

struct fuse_chan;

namespace cpfs {
namespace client {

class ICacheInvPolicy;
class ICacheMgr;

/**
 * Get a client cache manager.
 *
 * @param chan The FUSE low-level channel to use
 *
 * @param inv_policy The cache invalidation policy to use.  This
 * object will become owned by the cache manager
 */
ICacheMgr* MakeCacheMgr(fuse_chan* chan, ICacheInvPolicy* inv_policy);

}  // namespace client
}  // namespace cpfs
