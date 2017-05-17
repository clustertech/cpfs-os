#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define TimeKeeper. TimeKeeper keeps the time last update epoch time
 * and persist to disk after an configurable interval, so that the time value
 * could be restored later. The implementation is thread-safe.
 */

#include <stdint.h>

#include <string>

namespace cpfs {

/**
 * Interface for TimeKeeper.
 */
class ITimeKeeper {
 public:
  virtual ~ITimeKeeper() {}
  /**
   * Start the time keeper
   */
  virtual void Start() = 0;
  /**
   * Stop the time keeper
   */
  virtual void Stop() = 0;
  /**
   * Update the time. If TimerKeeper is not started, update will fail. Note
   * that update will succeed with a warning logged if clock was set back.
   *
   * @return Return true if update is successful
   */
  virtual bool Update() = 0;
  /**
   * Get the last update time.
   *
   * @return Time in epoch seconds
   */
  virtual uint64_t GetLastUpdate() const = 0;
};

}  // namespace cpfs
