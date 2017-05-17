#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IStartupMgr.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IStartupMgr;

/**
 * Create a StartupMgr.
 *
 * @param server The server with the startup manager
 */
IStartupMgr* MakeStartupMgr(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
