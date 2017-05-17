#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of cpfs::server::ms::IConnMgr.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IConnMgr;

/**
 * Create a ConnMgr.
 *
 * @param server The meta server that the connector operate on.
 */
IConnMgr* MakeConnMgr(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
