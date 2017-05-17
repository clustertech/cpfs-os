#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IDurableRange for tracking usage of inode ranges.
 */

#include <vector>

#include "common.hpp"

namespace cpfs {
namespace server {

/**
 * Interface for a durable list of "recently used" inode ranges.
 *
 * Unlike other persistent data structures, one can be sure to be
 * updated on the disk correctly (i.e., fsync or equivalent operation
 * has been applied to the disk file / directory as needed).  What
 * "recently" means depend on the frequency in which Latch() is called
 * (for details see its API doc).
 *
 * For efficiency, ranges instead of inode number are stored.  Each
 * range contains (2 ^ order) contiguous inode numbers, identified by
 * the starting inode number.
 */
class IDurableRange {
 public:
  virtual ~IDurableRange() {}

  /**
   * Load list from file.
   *
   * @return Whether the file is found and loaded
   */
  virtual bool Load() = 0;

  /**
   * Set whether files opened by Add() should use O_SYNC.
   *
   * @param conservative Whether to use O_SYNC
   */
  virtual void SetConservative(bool conservative) = 0;

  /**
   * Add inodes to be tracked.  Up to four inodes can be added at
   * once.
   *
   * @param inode1 The first inode
   *
   * @param inode2 The second inode
   *
   * @param inode3 The third inode
   *
   * @param inode4 The fourth inode
   */
  virtual void Add(InodeNum inode1, InodeNum inode2 = InodeNum(-1),
                   InodeNum inode3 = InodeNum(-1),
                   InodeNum inode4 = InodeNum(-1)) = 0;

  /**
   * Latch added ranges.  Ranges that are previously latched will be
   * removed, ranges that have previously been added becomes latched.
   * The file content will always be the union of latched and newly
   * added ranges.
   */
  virtual void Latch() = 0;

  /**
   * Clear both the added and latched ranges.
   */
  virtual void Clear() = 0;

  /**
   * @return Sorted list of added and latched ranges.  Each range is
   * identified by the first Inode number in the range
   */
  virtual std::vector<InodeNum> Get() = 0;
};

}  // namespace server
}  // namespace cpfs
