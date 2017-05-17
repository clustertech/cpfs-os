#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the inode allocation classes.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IDSGAllocator;

/**
 * Create an allocator.
 *
 * @param server The meta server
 */
IDSGAllocator* MakeDSGAllocator(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
