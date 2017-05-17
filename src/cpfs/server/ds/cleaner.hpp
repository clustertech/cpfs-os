#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICleaner interface.
 */

#include "common.hpp"
#include "periodic_timer.hpp"

namespace cpfs {
namespace server {
namespace ds {

/**
 * Interface for a thread to cleanup the DataServer.
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
   * Set the amount of time to wait before removing inodes (again) in
   * the store.
   *
   * @param remove_time The number of seconds to wait
   */
  virtual void SetMinInodeRemoveTime(unsigned remove_time) = 0;

  /**
   * Arrange to start triggering the data cleaner.
   */
  virtual void Init() = 0;

  /**
   * Shutdown the data cleaner.  This will immediately do the work for
   * all RemoveInodeLater() calls.
   */
  virtual void Shutdown() = 0;

  /**
   * Add an inode to attempt removal later.  This is called by MS to
   * arrange retry of inode removal after a certain amount of time, to
   * undo the effect of late checksum writes.
   *
   * @param inode The inode to attempt removal
   */
  virtual void RemoveInodeLater(InodeNum inode) = 0;
};

}  // namespace ds
}  // namespace server
}  // namespace cpfs
