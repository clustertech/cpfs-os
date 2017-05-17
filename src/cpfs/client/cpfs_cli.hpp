#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include <string>
#include <vector>

/**
 * @file
 *
 * Define ICpfsCLI
 */

namespace cpfs {
namespace client {

/**
 * Implement the CpfsCLI
 */
class ICpfsCLI {
 public:
  virtual ~ICpfsCLI() {}
  /**
   * Initialize the CLI. If CPFS is not ready but a meta server is already
   * running, prompt will be shown to ask whether force start is necessary
   *
   * @param force_start Whether to force start the MS when connect rejected
   *
   * @return Whether initialization is completed
   */
  virtual bool Init(bool force_start) = 0;
  /**
   * Prompt an interactive shell and handle commands from user
   */
  virtual void Prompt() = 0;
  /**
   * Handle status query
   *
   * @param nodes The nodes to query, all status are returned if empty.
   */
  virtual bool HandleQueryStatus(const std::vector<std::string>& nodes) = 0;
  /**
   * Handle detail info query
   *
   * @param nodes The nodes to query, all info are returned if empty.
   */
  virtual bool HandleQueryInfo(const std::vector<std::string>& nodes) = 0;
  /**
   * Handle config list
   *
   * @param configs The configs to query, all configs are returned if empty
   */
  virtual bool HandleConfigList(const std::vector<std::string>& configs) = 0;
  /**
   * Handle config change
   *
   * @param target The target role
   *
   * @param key The configuration key
   *
   * @param value The configuration value
   */
  virtual bool HandleConfigChange(const std::string& target,
                                  const std::string& key,
                                  const std::string& value) = 0;
  /**
   * Handle system shutdown
   */
  virtual bool HandleSystemShutdown() = 0;
};

}  // namespace client
}  // namespace cpfs
