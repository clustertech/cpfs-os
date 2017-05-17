#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs:IStatusDumper interface for dumping the current
 * status of daemons.  The status dumper is also responsible for
 * reopening the log file when SIGHUP is received.
 */

namespace cpfs {

class IAsioPolicy;

/**
 * Interface for allowing status dumping for a server.
 */
class IStatusDumpable {
 public:
  virtual ~IStatusDumpable() {}

  /**
   * Dump the current server status.
   */
  virtual void Dump() = 0;
};

/**
 * Interface for dumping current status of daemons.
 */
class IStatusDumper {
 public:
  virtual ~IStatusDumper() {}

  /**
   * Set the AsioPolicy to use.  Once set, the signal handler will be
   * installed to call Dump, until the object is destroyed.
   *
   * @param asio_policy The AsioPolicy to use
   */
  virtual void SetAsioPolicy(IAsioPolicy* asio_policy) = 0;
};

}  // namespace cpfs
