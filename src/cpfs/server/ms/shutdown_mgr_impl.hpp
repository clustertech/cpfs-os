#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the cpfs::ShutdownMgr
 */

#include "periodic_timer.hpp"

namespace cpfs {

class IShutdownMgr;

namespace server {
namespace ms {

class BaseMetaServer;

/**
 * Create a shutdown manager for Meta Server
 *
 * @param server Meta server
 *
 * @param maker The periodic timer maker
 */
IShutdownMgr* MakeShutdownMgr(BaseMetaServer* server, PeriodicTimerMaker maker);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
