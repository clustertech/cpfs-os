#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IStore interface.
 */

#include <string>

namespace cpfs {
namespace server {
namespace ms {

class IStore;

/**
 * Create a MS store object.  The implementation utilize the
 * guarantees described in fims.hpp to ensure the effectiveness of
 * locking.  It is also assumed that the data directory will not be
 * modified in any other ways, e.g., from another program.
 *
 * @param data_path The data directory.
 */
IStore* MakeStore(std::string data_path);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
