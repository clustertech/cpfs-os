#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of InodeUsage for tracking inode
 * operations in MS.
 */

namespace cpfs {
namespace server {
namespace ms {

class IInodeUsage;

/**
 * Get an Inode usage tracker for MS
 */
IInodeUsage* MakeInodeUsage();

}  // namespace ms
}  // namespace server
}  // namespace cpfs
