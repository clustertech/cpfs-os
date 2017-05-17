#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of PosixFS.
 */

namespace cpfs {

class IPosixFS;

/**
 * Create an IPosixFS object.
 */
IPosixFS* MakePosixFS();

}  // namespace cpfs
