#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IInodeRemovalTracker interface.
 */

#include <string>

namespace cpfs {
namespace server {

class IInodeRemovalTracker;

/**
 * Create a InodeRemovalTracker.
 *
 * @param data_path Path to the meta-server data directory
 */
IInodeRemovalTracker* MakeInodeRemovalTracker(const std::string& data_path);

}  // namespace server
}  // namespace cpfs
