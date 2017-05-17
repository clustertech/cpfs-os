#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of TimeKeeper.  TimeKeeper keeps the time
 * last update epoch time and persist to disk after an configurable
 * interval, so that the time value could be restored later. The
 * implementation is thread-safe.
 */

#include <stdint.h>

#include <string>

namespace cpfs {

class ITimeKeeper;

/**
 * Create the TimeKeeper
 *
 * @param path Path to persist the time
 *
 * @param attr The xattr name for time persist
 *
 * @param min_interval Min persist interval to avoid frequent disk write
 */
ITimeKeeper* MakeTimeKeeper(const std::string& path,
                            const std::string& attr,
                            uint64_t min_interval);

}  // namespace cpfs
