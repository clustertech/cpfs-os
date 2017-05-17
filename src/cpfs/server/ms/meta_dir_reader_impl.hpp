#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IMetaDirReader interface.
 */
#include <string>

namespace cpfs {
namespace server {
namespace ms {

class IMetaDirReader;

/**
 * Create a Resync Manager.
 *
 * @param data_path Path to the meta-server data directory
 */
IMetaDirReader* MakeMetaDirReader(const std::string& data_path);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
