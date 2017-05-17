#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the interface of a TopologyMgr
 * managing the topology information of the system.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class ITopologyMgr;

/**
 * Create a topology manager.
 *
 * @param server The meta server that the connector operate on
 */
ITopologyMgr* MakeTopologyMgr(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
