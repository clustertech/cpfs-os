#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of interfaces for handling user and group
 * IDs in meta-data server.
 */

namespace cpfs {
namespace server {
namespace ms {

class IUgidHandler;

/**
 * Create an instance implementing the IUgidHandler interface.  The
 * parameters are only interesting for unit tests.
 *
 * @param cache_time Number of seconds for cached group list entries to be used
 */
IUgidHandler* MakeUgidHandler(int cache_time = 10);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
