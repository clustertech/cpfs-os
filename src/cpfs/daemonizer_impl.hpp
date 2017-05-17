#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for daemon process
 */

namespace cpfs {

class IDaemonizer;

/**
 * Get a daemon
 */
IDaemonizer* MakeDaemonizer();

}
