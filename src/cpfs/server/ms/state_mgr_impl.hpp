#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IStateMgr.
 */

#include <string>

namespace cpfs {
namespace server {
namespace ms {

class IHACounter;
class IStateMgr;

/**
 * Create the HA counter
 *
 * @param data_path The path to the root meta directory
 *
 * @param role The role of the server
 */
IHACounter* MakeHACounter(const std::string& data_path,
                          const std::string& role);

/**
 * Create the StateMgr
 */
IStateMgr* MakeStateMgr();

}  // namespace ms
}  // namespace server
}  // namespace cpfs
