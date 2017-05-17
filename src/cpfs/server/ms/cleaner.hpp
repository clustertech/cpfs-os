#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICleaner interface.
 */

#include "periodic_timer.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Interface for a thread to cleanup the MetaServer.
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
   * Arrange to start triggering the meta cleaner.
   */
  virtual void Init() = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
