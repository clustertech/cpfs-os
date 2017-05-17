#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * The cpfs FC entry point
 */

namespace cpfs {

class IService;

namespace client {

/**
 * Create a service, integrating all parts needed for running an FC.
 *
 * @param argc The number of command line arguments.
 *
 * @param argv The command line arguments.
 */
IService* MakeFSClient(int argc, char** argv);

}  // namespace client
}  // namespace cpfs
