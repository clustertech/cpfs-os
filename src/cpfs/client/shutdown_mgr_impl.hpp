#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IShutdownMgr for client
 */

namespace cpfs {

class IShutdownMgr;

namespace client {

class BaseFSClient;

/**
 * Create a shutdown manager for Client
 *
 * @param client The FC
 */
IShutdownMgr* MakeShutdownMgr(BaseFSClient* client);

}  // namespace client
}  // namespace cpfs
