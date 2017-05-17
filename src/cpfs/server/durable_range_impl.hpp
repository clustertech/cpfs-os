#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the ICCacheTracker interface.
 */

namespace cpfs {
namespace server {

class IDurableRange;

/**
 * Create an instance of IDurableRange.  The implementation is thread
 * safe.  Care is taken to ensure that whenever Add() or Latch()
 * returns, the file on disk is updated; while Add() will not block if
 * a disk update is not needed.
 *
 * @param filename Name of file to persist data to
 *
 * @param order The order: each range is of size 2^order
 *
 * @return The instance
 */
IDurableRange* MakeDurableRange(const char* filename, int order);

}  // namespace server
}  // namespace cpfs
