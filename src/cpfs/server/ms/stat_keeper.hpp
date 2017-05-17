#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the IStatKeeper interface.
 */
#include <vector>

#include <boost/function.hpp>

#include "service.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Type to describe disk usage statistics for one DS.
 */
struct DSSpaceStat {
  DSSpaceStat() : online(false), total_space(0), free_space(0) {}
  bool online; /**< Whether the DS is online */
  uint64_t total_space; /**< Total space of the DS */
  uint64_t free_space; /**< Free space of the DS */
};

/**
 * Type to describe disk usage statistics for one DS group.  Will
 * contain kNumDSPerGroup elements, one per DS in the group.
 */
typedef std::vector<DSSpaceStat> DSGSpaceStat;

/**
 * Type to describe disk usage statistics for all DS groups.
 */
typedef std::vector<DSGSpaceStat> AllDSSpaceStat;

/**
 * Type for callback with disk usage statistics of all DS groups.
 */
typedef boost::function<void(const AllDSSpaceStat&)> SpaceStatCallback;

/**
 * Keep a thread to get DS group statistics from time to time.
 * Provide API to get the last fetched stats, and to explicitly
 * trigger space stats fetching.
 */
class IStatKeeper {
 public:
  virtual ~IStatKeeper() {}

  /**
   * Run the keeper.
   */
  virtual void Run() = 0;

  /**
   * Register a global callback, to be called for all future
   * completion of stat fetches.
   *
   * @param callback The new global callback
   */
  virtual void OnAllStat(SpaceStatCallback callback) = 0;

  /**
   * Get the last space stats fetched.
   *
   * @return The last space stats fetched.  If there has been no stat
   * collected yet, return an empty vector
   */
  virtual AllDSSpaceStat GetLastStat() = 0;

  /**
   * Explicitly trigger space stats fetching, cache the result, and
   * run a callback once completed.  Previous fetching of space stats
   * are ignored, including those in progress.  The result will be
   * available for future run of GetLastStat().
   *
   * @param callback The callback.  It will be called by the main
   * communication thread of the MS once the fetching completes
   */
  virtual void OnNewStat(SpaceStatCallback callback) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
