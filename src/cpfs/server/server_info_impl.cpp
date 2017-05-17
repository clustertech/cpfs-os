/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IServerInfo
 */

#include "server/server_info.hpp"

#include <sys/xattr.h>

#include <stdexcept>
#include <string>
#include <vector>

#include "store_util.hpp"

namespace cpfs {
namespace server {
namespace {

const char* kAttrPrefix = "user.";  /**< Prefix for xattr */
const unsigned kMaxAttrLen = 1024;  /**< Maximum length for xattr value */

/**
 * Implementation of ServerInfo
 */
class ServerInfo : public IServerInfo {
 public:
  /**
   * @param data_path The data directory path
   */
  explicit ServerInfo(const std::string& data_path);
  std::vector<std::string> List() const;
  std::string Get(const std::string& key, const std::string& def_value) const;
  void Set(const std::string& key, const std::string& value);
  void Remove(const std::string& key);

 private:
  const std::string data_path_;

  std::string GetFullKey(const std::string& key) const {
    return kAttrPrefix + key;
  }
};

ServerInfo::ServerInfo(const std::string& data_path) : data_path_(data_path) {}

std::vector<std::string> ServerInfo::List() const {
  return LListUserXattr(data_path_.c_str());
}

std::string ServerInfo::Get(const std::string& key,
                            const std::string& def_value) const {
  try {
    return LGetUserXattr(data_path_.c_str(), key);
  } catch (std::runtime_error) {
    return def_value;
  }
}

void ServerInfo::Set(const std::string& key, const std::string& value) {
  LSetUserXattr(data_path_.c_str(), key, value);
}

void ServerInfo::Remove(const std::string& key) {
  if (lremovexattr(data_path_.c_str(), (kAttrPrefix + key).c_str()) != 0)
    throw std::runtime_error("Cannot remove server info for " + key);
}

}  // namespace

IServerInfo* MakeServerInfo(const std::string& data_path) {
  return new ServerInfo(data_path);
}

}  // namespace server
}  // namespace cpfs
