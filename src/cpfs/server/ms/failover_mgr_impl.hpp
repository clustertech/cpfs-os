#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of FailoverMgr for switching stand-by
 * server to active, notifying connected FCs when ready.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IFailoverMgr;

/**
 * Get a Failover manager.
 *
 * @param server The meta server
 */
IFailoverMgr* MakeFailoverMgr(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
