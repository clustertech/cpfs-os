#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for turning the process to a linux daemon
 */

namespace cpfs {

/**
 * Utility for turning the process to a linux daemon
 */
class IDaemonizer {
 public:
  virtual ~IDaemonizer() {}
  /**
   * Daemonize the process
   */
  virtual void Daemonize() = 0;
};

}  // namespace cpfs
