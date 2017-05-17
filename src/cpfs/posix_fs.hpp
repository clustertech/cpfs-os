#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IPosixFS.
 */

#include <stddef.h>

#include <sys/types.h>

namespace cpfs {

/**
 * Interface for PosixFS.  PosixFS is an accessor to a POSIX
 * filesystem.  It defines some of the POSIX interfaces, so that it
 * can be mocked.  We normally want to use the real thing when testing
 * accesses to the FS, so this API is seldom used: it is used only in
 * cases where triggering of certain parts of the system requires
 * something that we cannot do with a standard FS.  And as such, this
 * API does not contain many functions.
 */
class IPosixFS {
 public:
  virtual ~IPosixFS() {}

  /**
   * The pread system call.
   *
   * @param fd The file descriptor
   *
   * @param buf The buffer to read into
   *
   * @param count The number of bytes to read
   *
   * @param offset The offset of the file to start reading
   *
   * @return The number of bytes read, or -1 in case of error
   */
  virtual ssize_t Pread(int fd, void* buf, size_t count, off_t offset) = 0;

  /**
   * The pwrite system call.
   *
   * @param fd The file descriptor
   *
   * @param buf The buffer to write
   *
   * @param count The number of bytes to write
   *
   * @param offset The offset of the file to start writing
   *
   * @return The number of bytes written, or -1 in case of error
   */
  virtual ssize_t Pwrite(int fd, const void* buf,
                         size_t count, off_t offset) = 0;

  /**
   * The lsetxattr system call.
   *
   * @param path The path to the file to have extended attribute changed
   *
   * @param name The name of the extended attribute to change
   *
   * @param value The new attribute
   *
   * @param size The length of value
   *
   * @param flags Flags controlling whether to restrict to creation or
   * modification of attributes
   */
  virtual ssize_t Lsetxattr(const char* path, const char* name,
                            const void* value, size_t size, int flags) = 0;

  /**
   * The sync system call.
   */
  virtual void Sync() = 0;
};

}  // namespace cpfs
