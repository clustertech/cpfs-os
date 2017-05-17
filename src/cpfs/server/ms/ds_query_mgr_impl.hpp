#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of DSQueryMgr.
 */

namespace cpfs {

namespace server {

namespace ms {

class BaseMetaServer;
class IDSQueryMgr;

/**
 * Get the DSQueryMgr
 *
 * @param meta_server The meta server
 */
IDSQueryMgr* MakeDSQueryMgr(BaseMetaServer* meta_server);

}  // namespace ms

}  // namespace server

}  // namespace cpfs
