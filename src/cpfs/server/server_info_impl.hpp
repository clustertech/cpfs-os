#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IServerInfo
 */

#include <string>

namespace cpfs {
namespace server {

class IServerInfo;

/**
 * Get a ServerInfo
 *
 * @param data_path Path to data directory
 */
IServerInfo* MakeServerInfo(const std::string& data_path);

}  // namespace server
}  // namespace cpfs
