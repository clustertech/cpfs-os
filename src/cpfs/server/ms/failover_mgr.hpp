#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the FailoverMgr for switching stand-by server to active,
 * notifying connected FCs when ready.
 */

#include "common.hpp"
#include "periodic_timer.hpp"

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;

/**
 * Interface for FailoverMgr.
 */
class IFailoverMgr {
 public:
  virtual ~IFailoverMgr() {}

  /**
   * Set periodic timer maker to use.  Must be called before calling
   * other methods.
   */
  virtual void SetPeriodicTimerMaker(PeriodicTimerMaker maker) = 0;

  /**
   * Start failover. If no re-connecting FC is connected within timeout,
   * server is switched to active and all connected FCs will be notified,
   * the whole system becomes fully functional.
   *
   * @param timeout The inactivity timeout for failover
   */
  virtual void Start(double timeout) = 0;

  /**
   * Track that one more FC has completed the reconfirmation
   */
  virtual void AddReconfirmDone(ClientNum client_num) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
