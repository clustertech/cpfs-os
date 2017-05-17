#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the IDirIterator interface for iterating entries in a directory,
 * with support of filtering them by ctime.
 */

#include <stdint.h>

#include <sys/stat.h>

#include <string>

namespace cpfs {

/**
 * Interface for iterating directory.
 */
class IDirIterator {
 public:
  virtual ~IDirIterator() {}

  /**
   * Get next entry in the directory
   *
   * @param name The name of entry retrieved
   *
   * @param is_dir_ret Where to return whether the entry is a directory
   *
   * @param stbuf_ret If set, points to a buffer which will receive
   * information of the entry
   *
   * @return True if an entry is retrieved
   */
  virtual bool GetNext(std::string* name, bool* is_dir_ret,
                       struct stat* stbuf_ret = 0) = 0;

  /**
   * Set ctime filter. Only entry with ctime greater than specified
   * will be returned by GetNext()
   *
   * @param ctim The ctime lower bound
   */
  virtual void SetFilterCTime(uint64_t ctim) = 0;

  /**
   * @return Whether the directory in question is missing
   */
  virtual bool missing() const = 0;
};

}  // namespace cpfs
