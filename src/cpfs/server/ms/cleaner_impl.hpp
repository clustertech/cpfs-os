#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICleaner interface.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class ICleaner;

/**
 * Create an implementation of the ICleaner interface.
 *
 * @param server The meta server to clean up
 */
ICleaner* MakeCleaner(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
