#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define types supporting CPFS administration
 */

#include <stdint.h>

#include <cstring>
#include <string>

#include <boost/format.hpp>

#include "common.hpp"

namespace cpfs {

/** Length of the node alias */
const std::size_t kNodeNameLen = 16;
/** Length of the UUID */
const std::size_t kUUIDLen = 36;

/**
 * Node state information for a CPFS node.
 */
struct NodeInfo {
  uint32_t ip;  /**< The ipv4 address */
  uint16_t port;  /**< The port listening on */
  uint16_t pid;  /**< The process ID of the node */
  /**
   * The type of the node:
   * 'M' for MS, 'D' for DS and 'F' for FC, 'U' otherwise
   */
  char type;
  uint8_t terminating;  /**< Whether the node is terminating */
  char alias[kNodeNameLen];  /**< Alias for the node. e.g. FC 10, DS 0-1, MS1 */
  char uuid[40U];  /**< The UUID for the node */

  /**
   * Create a NodeInfo
   */
  NodeInfo() : ip(0), port(0), pid(0), type('U'), terminating(false) {
    std::memset(alias, '\0', kNodeNameLen);
    std::memset(uuid, '\0', 40U);
  }

  /**
   * Set the UUID for node
   */
  void SetUUID(const char* to_set) {
    std::strncpy(uuid, to_set, kUUIDLen);
    uuid[kUUIDLen] = '\0';
  }

  /**
   * Set the alias for DS node
   */
  void SetAlias(GroupId g, GroupRole r) {
    type = 'D';
    std::string name = (boost::format("DS %d-%d") % g % r).str();
    std::strncpy(alias, name.c_str(), kNodeNameLen);
    alias[kNodeNameLen - 1] = '\0';
  }

  /**
   * Set the alias for FC
   */
  void SetAlias(ClientNum fc) {
    type = 'F';
    std::string name = (boost::format("FC %d") % fc).str();
    std::strncpy(alias, name.c_str(), kNodeNameLen);
    alias[kNodeNameLen - 1] = '\0';
  }

  /**
   * Set the alias for MS
   */
  void SetAlias(uint8_t ms) {
    type = 'M';
    std::string name = (boost::format("MS %d") % ms).str();
    std::strncpy(alias, name.c_str(), kNodeNameLen);
    alias[kNodeNameLen - 1] = '\0';
  }

  /**
   * Compare the NodeInfo
   */
  bool operator == (const NodeInfo& info) const {
    return ip == info.ip && port == info.port &&
           pid == info.pid && type == info.type &&
           std::string(alias) == std::string(info.alias);
  }
};

/**
 * DS space usage information
 */
struct DSDiskInfo {
  char alias[kNodeNameLen];  /**< The alias name for DS node, e.g. DS 0-1 */
  uint64_t total;  /**< The total disk space */
  uint64_t free;  /**< The free disk space */

  /**
   * Create a DSDiskInfo
   */
  DSDiskInfo() {
    std::memset(alias, '\0', kNodeNameLen);
  }

  /**
   * Set the alias for DS
   */
  void SetAlias(GroupId g, GroupRole r) {
    std::string name = (boost::format("DS %d-%d") % g % r).str();
    std::strncpy(alias, name.c_str(), kNodeNameLen);
    alias[kNodeNameLen - 1] = '\0';
  }
};

/** Length of the config name */
const std::size_t kConfigNameLen = 32;

/** Length of the config value */
const std::size_t kConfigValueLen = 256;

/** Tag to mark the begin of configuration items of a node */
const char* const kNodeCfg = "_node-cfg_";

/**
 * Data structure for a single configuration item
 */
struct ConfigItem {
  char name[kConfigNameLen];  /**< The name for the config */
  char value[kConfigValueLen];  /**< The value for the config */

  /**
   * Create a ConfigItem
   */
  ConfigItem() {
    std::memset(this->name, '\0', kConfigNameLen);
    std::memset(this->value, '\0', kConfigValueLen);
  }

  /**
   * Create a ConfigItem from name and value
   *
   * @param name The config name
   *
   * @param value The config value
   */
  ConfigItem(const std::string& name, const std::string& value) {
    std::strncpy(this->name, name.c_str(), kConfigNameLen);
    this->name[kConfigNameLen - 1] = '\0';
    std::strncpy(this->value, value.c_str(), kConfigValueLen);
    this->value[kConfigValueLen - 1] = '\0';
  }
};

}  // namespace cpfs
