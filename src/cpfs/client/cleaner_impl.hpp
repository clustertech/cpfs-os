#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define client::ICleaner to periodically clean up client data
 * structures.
 */

namespace cpfs {
namespace client {

class BaseFSClient;
class ICleaner;

/**
 * Create an implementation of the ICleaner interface.
 *
 * @param client The client to clean up
 */
ICleaner* MakeCleaner(BaseFSClient* client);

}  // namespace client
}  // namespace cpfs
