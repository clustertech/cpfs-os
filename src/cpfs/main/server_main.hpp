#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * The cpfs DS/MS server entry point
 */

namespace cpfs {

class IDaemonizer;
class IService;
struct ConfigMgr;

namespace main {

/**
 * Parse command line options for use by MakeServer()
 *
 * @param argc The number of arguments.
 *
 * @param argv The arguments.
 *
 * @param configs The config items parsed.
 *
 * @return Whether options are parsed successfully
 */
bool ParseOpts(int argc, char* argv[], ConfigMgr* configs);

/**
 * Parse command line options and create the MS/DS server.
 *
 * @param argc The number of arguments.
 *
 * @param argv The arguments.
 *
 * @param daemonizer The daemonizer.
 *
 * @return The server instance to run
 */
IService* MakeServer(int argc, char* argv[], IDaemonizer* daemonizer);

}  // namespace main
}  // namespace cpfs
