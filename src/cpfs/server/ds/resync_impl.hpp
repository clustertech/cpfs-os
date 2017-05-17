#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the interfaces for classes handling DS
 * resync operations.
 */

#include "server/ds/resync.hpp"

namespace cpfs {
namespace server {
namespace ds {

class BaseDataServer;

/**
 * Create an implementation of IResyncMgr.
 */
extern ResyncSenderMaker kResyncSenderMaker;

/**
 * Create an implementation of IResyncMgr.
 *
 * @param server The data server using the resync manager
 *
 * @return The DS resync manager
 */
IResyncMgr* MakeResyncMgr(BaseDataServer* server);

/**
 * Create a Fim processor to handle DS resync Fims sent from peer to
 * rebuild the DS previously failed.
 *
 * @param server The data server using the Fim processor
 *
 * @return The Fim processor
 */
IResyncFimProcessor* MakeResyncFimProcessor(BaseDataServer* server);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
