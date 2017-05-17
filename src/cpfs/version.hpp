#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Template for generating version.hpp
 */

/**
 * Namespace for CPFS.
 */
namespace cpfs {

/**
 * CPFS version number.
 */
const char CPFS_VERSION[] =
#include "version.ipp"
"";

}
