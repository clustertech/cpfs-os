#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define initial Fim processors for the meta data server.
 */

namespace cpfs {

class IFimProcessor;

namespace server {
namespace ms {

class BaseMetaServer;

/**
 * Create a FimProcessor for use during initial state when the meta
 * server receives a connection.
 *
 * @param meta_server The meta data server
 */
IFimProcessor* MakeInitFimProcessor(BaseMetaServer* meta_server);

/**
 * Create a FimProcessor for processing inter-MS specific Fims.  The
 * replication Fims are handled by a meta-worker and is not dealt with
 * here.
 *
 * @param meta_server The meta data server
 */
IFimProcessor* MakeMSCtrlFimProcessor(BaseMetaServer* meta_server);

/**
 * Create a FimProcessor for processing Fims sent from DS to MS.
 *
 * @param meta_server The meta data server
 */
IFimProcessor* MakeDSCtrlFimProcessor(BaseMetaServer* meta_server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
