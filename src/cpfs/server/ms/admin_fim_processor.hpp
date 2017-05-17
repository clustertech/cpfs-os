#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of AdminFimProcessor.
 */

namespace cpfs {

class IFimProcessor;

namespace server {

namespace ms {

class BaseMetaServer;

/**
 * Get the AdminFimProcessor
 *
 * @param meta_server The meta data server
 */
IFimProcessor* MakeAdminFimProcessor(BaseMetaServer* meta_server);

}  // namespace ms

}  // namespace server

}  // namespace cpfs
