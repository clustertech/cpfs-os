#pragma once

/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * The cpfs Access API entry point
 */

namespace cpfs {

namespace client {

class BaseAPIClient;

/**
 * Create the API client, integrating all parts needed
 *
 * @param num_threads Number of worker threads to use
 */
BaseAPIClient* MakeAPIClient(unsigned num_threads);

}  // namespace client
}  // namespace cpfs
