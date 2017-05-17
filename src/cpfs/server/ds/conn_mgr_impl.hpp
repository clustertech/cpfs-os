#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of cpfs::server::ds::IConnMgr for
 * initializing connections to MS and DSx for DS.
 */

namespace cpfs {
namespace server {
namespace ds {

class BaseDataServer;
class IConnMgr;

/**
 * Create a ConnMgr
 *
 * @param server The data server that the connector operate on.
 */
IConnMgr* MakeConnMgr(BaseDataServer* server);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
