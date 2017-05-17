#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define and implement functions to perform profiling easily.
 */

#include <stdint.h>

#include <boost/date_time/posix_time/posix_time.hpp>

namespace cpfs {

/**
 * For getting time since creation of the stop watch.
 */
class StopWatch {
 public:
  /**
   * @param start Whether to automatically start the timer
   */
  explicit StopWatch(bool start = true) {
    if (start)
      Start();
  }

  /**
   * Start / restart the stop watch.
   */
  void Start() {
    start_time_ = Now();
  }

  /**
   * Read the elapsed time since creation of the stop watch, in
   * microseconds.
   */
  uint64_t ReadTimerMicro() {
    return (Now() - start_time_).total_microseconds();
  }

 private:
  boost::posix_time::ptime start_time_; /**< When the stopwatch is created */

  /**
   * Get time now.
   *
   * @return The time
   */
  boost::posix_time::ptime Now() {
    return boost::posix_time::microsec_clock::universal_time();
  }
};

}  // namespace cpfs
