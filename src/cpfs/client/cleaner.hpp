#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define client::ICleaner to periodically clean up client data
 * structures.
 */

#include "periodic_timer.hpp"

namespace cpfs {
namespace client {

/**
 * Interface for a thread to cleanup the client.  The thread is
 * started by the client io service runner, and thus there is no need
 * for a Start() method.
 */
class ICleaner {
 public:
  virtual ~ICleaner() {}

  /**
   * Set PeriodicTimerMaker to use.  Must be called before calling
   * other methods.
   *
   * @param maker The PeriodicTimerMaker to use
   */
  virtual void SetPeriodicTimerMaker(PeriodicTimerMaker maker) = 0;

  /**
   * Arrange to start triggering the client cleaner.
   */
  virtual void Init() = 0;
};

}  // namespace client
}  // namespace cpfs
