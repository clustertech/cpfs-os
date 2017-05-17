#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define PeriodicTimer.
 */

#include <boost/function.hpp>  // IWYU pragma: keep

#include "asio_common.hpp"

namespace cpfs {

/**
 * Type for periodic timer callback.
 */
typedef boost::function<bool()> PeriodicTimerCallback;

/**
 * Interface for PeriodicTimer. PeriodicTimer runs a callback periodically
 * if not reset or stopped, defer next callback if reset is called, and
 * stop any further callback if stopped or timer is destroyed.
 */
class IPeriodicTimer {
 public:
  virtual ~IPeriodicTimer() {}

  /**
   * Reset the timer
   *
   * @param tolerance Skip the reset if the last has been tolerance
   * * period or less before
   */
  virtual void Reset(double tolerance = 0.0) = 0;

  /**
   * Stop the timer, no further callback is called
   */
  virtual void Stop() = 0;

  /**
   * Set the callback to call when timeout occurs. The callback function
   * returns false to indicate that the timer should be stopped, no further
   * callback will be invoked
   */
  virtual void OnTimeout(PeriodicTimerCallback callback) = 0;

  /**
   * Duplicate the timer to another IOService.  The resulting timer
   * will have the same callback and started status as this timer,
   * although expiry will be counted from now in case it is started.
   *
   * @param io_service The IOService to duplicate to
   *
   * @return The duplicated timer
   */
  virtual boost::shared_ptr<IPeriodicTimer>
  Duplicate(IOService* io_service) = 0;
};

/**
 * Type for changing the behavior of creating IPeriodicTimer
 * instances.  The first argument is the IOService used to do the
 * waiting.  The second argument is the number of seconds between
 * invocations.
 */
typedef boost::function<
  boost::shared_ptr<IPeriodicTimer>(IOService* io_service, double timeout)
> PeriodicTimerMaker;

}  // namespace cpfs
