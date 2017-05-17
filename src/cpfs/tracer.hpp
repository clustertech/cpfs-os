#pragma once

/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Define Tracer. A facility for logging recent operations
 */

#include <string>
#include <vector>

#include "common.hpp"

namespace cpfs {

/**
 * Facility for logging recent operations
 */
class ITracer {
 public:
  virtual ~ITracer() {}
  /**
   * Log the trace information
   *
   * @param method The method executing
   *
   * @param inode The inode number associated
   *
   * @param client The client number associated
   */
  virtual void Log(const char* method,
                   InodeNum inode,
                   ClientNum client) = 0;

  /**
   * Dump all logs
   *
   * @return Vector of trace information in string
   */
  virtual std::vector<std::string> DumpAll() const = 0;
};

}  // namespace cpfs
