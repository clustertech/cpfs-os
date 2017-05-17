#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IReplier.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IReplier;

/**
 * Create an implementation of the meta server replier.  The returned
 * object is thread safe.
 *
 * @param server The meta server using the replier
 *
 * @return The created meta server replier
 */
IReplier* MakeReplier(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
