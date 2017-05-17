#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IStartupMgr.
 */

#include "common.hpp"
#include "periodic_timer.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Interface of class used for determining the MS startup behavior.
 * These includes whether a DSG should be started degraded, and
 * whether a joining DS should be deferred until DSG startup is
 * completed.
 *
 * The class provides mainly setters and getters, but the values set
 * in setters are in general not used by the same process.  Instead,
 * it is persisted to disk so that getters can obtain them in future
 * invocations of the same program when started as a MS.
 *
 * The class also provides a periodic checker during startup.  It
 * force-starts DSGs if all DSGs can be started (possibly degraded)
 * but it isn't yet because it is open, if it finds this state in two
 * consecutive checks.
 */
class IStartupMgr {
 public:
  virtual ~IStartupMgr() {}

  /**
   * @param maker The periodic timer maker to use.
   */
  virtual void SetPeriodicTimerMaker(PeriodicTimerMaker maker) = 0;

  /**
   * Initial the manager using persisted data.
   */
  virtual void Init() = 0;

  /**
   * Reset the manager with the provided data
   *
   * @param data
   */
  virtual void Reset(const char* data) = 0;

  /**
   * @param group The group ID of a DSG
   *
   * @param failed_ret Where to return the failed DS
   *
   * @return Whether the DSG is initially degraded
   */
  virtual bool dsg_degraded(GroupId group, GroupRole* failed_ret) = 0;

  /**
   * @param group The group ID of a DSG
   *
   * @param degraded Whether the DSG is initially degraded
   *
   * @param failed The role ID of the failed DS in the DSG, if degraded
   */
  virtual void set_dsg_degraded(GroupId group, bool degraded,
                                GroupRole failed) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
