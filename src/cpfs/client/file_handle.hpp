#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * File handle for FCs.
 */

#include <stdint.h>

#include <boost/atomic.hpp>
#include <boost/scoped_ptr.hpp>

#include "common.hpp"
#include "ds_iface.hpp"
#include "fim.hpp"

namespace cpfs {
namespace client {

/**
 * Represent a file handle
 */
struct FileHandle {
  boost::scoped_ptr<FileCoordManager> coord_mgr; /**< Coordinate manager */
  /** Cached error number to report when there is a chance */
  boost::atomic<int> err_no;
};

/**
 * Create a FUSE file handle for CPFS.  The groups are initialized
 * using an explicitly passed array.
 *
 * @param inode The inode number.
 *
 * @param groups The DS group assignment.
 *
 * @param num_groups The number of DS groups assigned.
 *
 * @return The handle
 */
inline int64_t MakeFH(InodeNum inode,
                      const GroupId* groups, unsigned num_groups) {
  FileHandle* fh = new FileHandle;
  fh->coord_mgr.reset(new FileCoordManager(inode, groups, num_groups));
  fh->err_no = 0;
  return reinterpret_cast<int64_t>(fh);
}

/**
 * Create a FUSE file handle for CPFS.  The groups are initialized
 * using group ids found in the tail buffer of a message.
 *
 * @param inode The inode number.
 *
 * @param group_fim The Fim containing the DS group assignment.
 *
 * @return The handle
 */
inline int64_t MakeFH(InodeNum inode, const FIM_PTR<IFim> group_fim) {
  const GroupId* groups =
      reinterpret_cast<const GroupId*>(group_fim->tail_buf());
  unsigned num_groups = group_fim->tail_buf_size() / sizeof(GroupId);
  return MakeFH(inode, groups, num_groups);
}

/**
 * Get file handle object from FUSE file handle
 */
inline FileHandle* FH2FileHandle(int64_t fh) {
  return reinterpret_cast<FileHandle*>(fh);
}

/**
 * Get the FileCoordManager object stored in a CPFS FUSE handle.
 *
 * @param fh The handle
 *
 * @return The FileCoordManager
 */
inline FileCoordManager* FHFileCoordManager(int64_t fh) {
  return FH2FileHandle(fh)->coord_mgr.get();
}

/**
 * Get and reset deferred error stored in a CPFS FUSE handle.
 *
 * @param fh The handle
 *
 * @param clear Whether to clear the error number after getting it
 *
 * @return The stored error
 */
inline int FHGetErrno(int64_t fh, bool clear) {
  FileHandle* handle = FH2FileHandle(fh);
  if (clear)
    return handle->err_no.exchange(0);
  else
    return handle->err_no;
}

/**
 * Store a deferred error in a CPFS FUSE handle.  Not set again if
 * some other error has already been set.
 *
 * @param fh The handle
 *
 * @param err_no The error
 */
inline void FHSetErrno(int64_t fh, int err_no) {
  FileHandle* handle = FH2FileHandle(fh);
  int expected = 0;
  handle->err_no.compare_exchange_strong(expected, err_no);
}

/**
 * Deallocate a CPFS FUSE handle.
 *
 * @param fh The handle
 */
inline void DeleteFH(int64_t fh) {
  delete FH2FileHandle(fh);
}

}  // namespace client
}  // namespace cpfs
