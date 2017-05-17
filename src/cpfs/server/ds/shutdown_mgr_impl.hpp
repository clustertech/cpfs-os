#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the ShutdownMgr for DS
 */

namespace cpfs {

class IShutdownMgr;

namespace server {
namespace ds {

class BaseDataServer;

/**
 * Create a shutdown manager for Data Server
 *
 * @param server Data server
 */
IShutdownMgr* MakeShutdownMgr(BaseDataServer* server);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
