#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define IServerInfo for storing and getting data in xattr of data directory.
 */

#include <string>
#include <vector>

namespace cpfs {
namespace server {

/**
 * Interface for ServerInfo
 */
class IServerInfo {
 public:
  virtual ~IServerInfo() {}

  /**
   * @return Server info keys stored that one can Get()
   */
  virtual std::vector<std::string> List() const = 0;

  /**
   * Get the value by key.
   *
   * @param key The key
   *
   * @param def_value (optional) The default value if key is not found
   */
  virtual std::string Get(const std::string& key,
                          const std::string& def_value = "") const = 0;

  /**
   * Set value by key.
   *
   * @param key The key
   *
   * @param value The value
   */
  virtual void Set(const std::string& key, const std::string& value) = 0;

  /**
   * Remove value by key.
   *
   * @param key The key
   */
  virtual void Remove(const std::string& key) = 0;
};

}  // namespace server
}  // namespace cpfs
