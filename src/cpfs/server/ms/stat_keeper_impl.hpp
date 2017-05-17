#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IStatKeeper.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IStatKeeper;

/**
 * Create a stat keeper.
 *
 * @param server The server object using the keeper
 */
IStatKeeper* MakeStatKeeper(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
