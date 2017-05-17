#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IDegradedCache interface and
 * IDataRecoveryMgr interface.
 */

namespace cpfs {
namespace server {
namespace ds {

class IDegradedCache;
class IDataRecoveryMgr;

/**
 * Create an implementation of IDegradedCache.
 *
 * @param num_cached_segments Number of segments to cache
 *
 * @return The degrade cache manager created
 */
IDegradedCache* MakeDegradedCache(unsigned num_cached_segments);

/**
 * Create an implementation of IDataRecoveryMgr.
 *
 * @return The data recovery manager created
 */
IDataRecoveryMgr* MakeDataRecoveryMgr();

}  // namespace ds
}  // namespace server
}  // namespace cpfs
