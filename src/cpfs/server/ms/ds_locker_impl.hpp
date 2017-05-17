#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IDSLocker, an interface for DS
 * locking in the meta-server.  This is splited from worker
 * mainly to make the unit test workflow more manageable.
 */

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IDSLocker;

/**
 * Create an implementation of the IDSLocker interface.
 *
 * @param server The server using the locker
 */
IDSLocker* MakeDSLocker(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
