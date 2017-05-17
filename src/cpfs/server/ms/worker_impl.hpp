#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Declare function to create an IWorker for the MS.
 */

namespace cpfs {
namespace server {

class IWorker;

namespace ms {

/**
 * Create a MS worker.
 */
IWorker* MakeWorker();

}  // namespace ms
}  // namespace server
}  // namespace cpfs
