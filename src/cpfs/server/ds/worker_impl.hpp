#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Declare function to create an IWorker for the data server.
 */

namespace cpfs {
namespace server {

class IWorker;

namespace ds {

/**
 * Create a data-server worker.  The implementation is not thread
 * safe: it is expected that at a time only one thread will run its
 * methods.
 */
IWorker* MakeWorker();

}  // namespace ds
}  // namespace server
}  // namespace cpfs
