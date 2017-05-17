#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the Resync Manager
 */

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IResyncMgr;

/**
 * Create a Resync Manager.
 *
 * @param server The meta server
 *
 * @param max_send Number of resync fim to send before reply is received
 */
IResyncMgr* MakeResyncMgr(BaseMetaServer* server,
                          unsigned max_send = kMaxNumResyncFim);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
