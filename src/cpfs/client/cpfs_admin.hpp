#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICpfsAdmin for performing cluster administration
 */

#include <list>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include "admin_info.hpp"
#include "common.hpp"

namespace cpfs {
namespace client {

/**
 * Cluster state information for a CPFS
 */
struct ClusterInfo {
  std::string ms_state;  /**< The state name of the active MS */
  std::string ms_role;  /**< The role of MS */
  std::vector<std::string> dsg_states;  /**< The state names of all DSG */
  /** List of node info attributes */
  std::vector<
      std::map<std::string, std::string> > node_infos;
};

/**
 * List of DS Disk usage information
 */
typedef std::vector<std::map<std::string, std::string> >  DiskInfoList;

/**
 * List of config items
 */
typedef std::vector<std::pair<std::string, std::string> > ConfigList;

/**
 * Implement the CpfsAdmin
 */
class ICpfsAdmin {
 public:
  virtual ~ICpfsAdmin() {}
  /**
   * Connect to the remote CPFS servers. The connection can be successful or
   * fail due to unreachable or inactive meta servers.
   *
   * @param timeout The maximum time to wait before connection is completed
   *
   * @param reject_info Record meta server that rejected the connection.
   * Useful for force start without MS election
   *
   * @return Whether CpfsAdmin has connected to remote CPFS successfully
   */
  virtual bool Init(double timeout, std::vector<bool>* reject_info) = 0;
  /**
   * Query status of the cluster
   */
  virtual ClusterInfo QueryStatus() = 0;
  /**
   * Query cluster disk info
   */
  virtual DiskInfoList QueryDiskInfo() = 0;
  /**
   * List available configs
   */
  virtual ConfigList ListConfig() = 0;
  /**
   * Change the given configuration item
   *
   * @param target The target role
   *
   * @param name The config name
   *
   * @param value The config value
   */
  virtual bool ChangeConfig(const std::string& target,
                            const std::string& name,
                            const std::string& value) = 0;
  /**
   * Shutdown the CPFS system
   */
  virtual bool SystemShutdown() = 0;
  /**
   * Forcibly start the specified MS server
   *
   * @param server_index The meta server to start forcibly
   */
  virtual bool ForceStart(unsigned server_index) = 0;
};

}  // namespace client
}  // namespace cpfs
