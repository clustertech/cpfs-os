#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of InodeUsageSet for tracking inode
 * operations.
 */

namespace cpfs {
namespace client {

class IInodeUsageSet;

/**
 * Get an Inode usage tracker for FC.  To promote concurrency, this
 * class is internally organized as an array of basic usage set, each
 * governing some of the inodes.  A common mutex is locked whenever
 * two operations are done of inodes of the same class.  So increasing
 * the number of classes increase concurrency at expense of more
 * memory usage.
 *
 * @param num_classes Number of classes
 */
IInodeUsageSet* MakeInodeUsageSet(unsigned num_classes = 256);

}  // namespace client
}  // namespace cpfs
