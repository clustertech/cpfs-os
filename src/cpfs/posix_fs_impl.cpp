/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of PosixFS.
 */

#include <stddef.h>
#include <unistd.h>

#include <attr/xattr.h>

#include "posix_fs.hpp"

namespace cpfs {
namespace {

/**
 * Implementation for IPosixFS interface.
 */
class PosixFS : public IPosixFS {
 public:
  ssize_t Pread(int fd, void* buf, size_t count, off_t offset) {
    return pread(fd, buf, count, offset);
  }

  ssize_t Pwrite(int fd, const void* buf, size_t count, off_t offset) {
    return pwrite(fd, buf, count, offset);
  }

  ssize_t Lsetxattr(const char* path, const char* name,
                    const void* value, size_t size, int flags) {
    return lsetxattr(path, name, value, size, flags);
  }

  void Sync() {
    sync();
  }
};

}  // namespace

IPosixFS* MakePosixFS() {
  return new PosixFS();
}

}  // namespace cpfs
