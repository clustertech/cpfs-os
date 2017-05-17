#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define interface for client-side cache management.
 */

#include <fuse/fuse_lowlevel.h>

#include <sys/types.h>

#include <cstddef>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common.hpp"
#include "mutex_util.hpp"

struct fuse_chan;

namespace boost {
class mutex;
}

namespace cpfs {
namespace client {

/**
 * A policy class to take the relavant parts of the FUSE policy in an
 * interface for ease to work with in cache invalidator.
 */
class ICacheInvPolicy {
 public:
  virtual ~ICacheInvPolicy() {}

  /**
   * Make the FUSE notify_inval_inode call.
   *
   * @param ch The channel through which to send the invalidation
   * @param ino The inode number
   * @param off The offset in the inode where to start invalidating
   * or negative to invalidate attributes only
   * @param len the amount of cache to invalidate or 0 for all
   * @return zero for success, -errno for failure
   */
  virtual int NotifyInvalInode(fuse_chan* ch, fuse_ino_t ino, off_t off,
                               off_t len) = 0;

  /**
   * Make the FUSE notify_inval_entry call.
   *
   * @param ch the channel through which to send the invalidation
   * @param parent inode number
   * @param name file name
   * @param namelen strlen() of file name
   * @return zero for success, -errno for failure
   */
  virtual int NotifyInvalEntry(fuse_chan *ch, fuse_ino_t parent,
                               const char *name, std::size_t namelen) = 0;
};

/**
 * Forward virtual calls to actual FUSE method policy.
 */
template <typename TFuseMethodPolicy>
class CacheInvPolicy : public ICacheInvPolicy {
 public:
  /**
   * @param policy The FUSE policy to forward calls to
   */
  explicit CacheInvPolicy(TFuseMethodPolicy* policy) : policy_(policy) {}

  int NotifyInvalInode(fuse_chan* ch, fuse_ino_t ino, off_t off, off_t len) {
    return policy_->NotifyInvalInode(ch, ino, off, len);
  }

  int NotifyInvalEntry(fuse_chan *ch, fuse_ino_t parent, const char *name,
                       std::size_t namelen) {
    return policy_->NotifyInvalEntry(ch, parent, name, namelen);
  }

 private:
  TFuseMethodPolicy* policy_;
};

/**
 * TODO(Joseph): A dummy implementation for the cache policy. The dependency
 * to FUSE will be removed in later commits.
 *
 * Null implementation for the cache policy
 */
class NullCachePolicy : public ICacheInvPolicy {
 public:
  int NotifyInvalInode(fuse_chan* ch, fuse_ino_t ino, off_t off, off_t len) {
    (void) ch;
    (void) ino;
    (void) off;
    (void) len;
    return 0;
  }

  int NotifyInvalEntry(fuse_chan *ch, fuse_ino_t parent, const char *name,
                       std::size_t namelen) {
    (void) ch;
    (void) parent;
    (void) name;
    (void) namelen;
    return 0;
  }
};

/**
 * Interface for cache invalidation record.  Allow one to query
 * whether an inode has been invalidated since the time when the
 * record is created.  This is useful for determining whether the
 * inode obtained should be cached in the kernel cache.
 */
class ICacheInvRecord {
 public:
  virtual ~ICacheInvRecord() {}

  /**
   * Get the mutex to lock to temporarily prevent inode invalidations.
   * The InodeInvalidated() method should only be called with this
   * mutex locked, and it should only be released after the action
   * using the value returned by InodeInvalidated().
   */
  virtual MUTEX_TYPE* GetMutex() = 0;

  /**
   * Return whether an inode has been invalidated.
   *
   * @param inode The inode to check
   *
   * @param page_only Whether to check only for page invalidation
   *
   * @return Whether the inode has been invalidated
   */
  virtual bool InodeInvalidated(InodeNum inode, bool page_only) const = 0;
};

/**
 * Interface for FUSE client-side cache managers.  It is responsible
 * for interacting with the kernel inode and page cache.
 * Implementation of the interface should be thread-safe: between
 * calls to Init() and Shutdown() of one thread, multiple threads can
 * concurrently call any other methods here in any timing / order
 * without corruption of the manager.
 */
class ICacheMgr {
 public:
  virtual ~ICacheMgr() {}

  /**
   * Set the fuse channel used in cache manager.
   */
  virtual void SetFuseChannel(fuse_chan* chan) = 0;

  /**
   * Initialize the cache manager.  This is where threads for the
   * manager are started.
   */
  virtual void Init() = 0;

  /**
   * Revert the initialization in Init().
   */
  virtual void Shutdown() = 0;

  /**
   * Prepare for a Lookup operation.
   *
   * @return An inode invalidation record, to be used for checking
   * whether an invalidation has occurred.
   */
  virtual boost::shared_ptr<ICacheInvRecord> StartLookup() = 0;

  /**
   * Add a directory entry that needs to be invalidated during
   * InvalidateInode(inode, true).  The entry is removed by CleanMgr()
   * if it has expired.  The add time is updated if the entry is
   * already added.
   *
   * The client may request skipping of the regular mutex locking.
   * Then the client is responsible for locking the mutex, normally
   * obtained from ICacheInvRecord::GetMutex(), before calling
   * AddEntry().  This allows AddEntry() and other operations on the
   * invalidation record in the same critical section.
   *
   * @param inode The inode.
   *
   * @param name The file name of the directory entry.
   *
   * @param skip_lock Whether locking should be skipped
   */
  virtual void AddEntry(InodeNum inode, const std::string& name,
                        bool skip_lock = false) = 0;

  /**
   * Register an inode that needs to be invalidated during
   * InvalidateAll().  The registration is removed by CleanMgr() if it
   * has expired.  If the inode is already added the registration time
   * is updated.  Calling RegisterInode() is not needed if AddEntry()
   * is also called, since the work is a subset of AddEntry().  The
   * client code should not be holding a lock when using
   * RegisterInode().
   *
   * @param inode The inode.
   */
  virtual void RegisterInode(InodeNum inode) = 0;

  /**
   * Invalidate the stat buffer of an inode, and possibly its page
   * cache, in a separated thread.
   *
   * @param inode The inode to have stat buffer / page cache
   * invalidated.
   *
   * @param clear_pages Whether the page cache is invalidated as well.
   */
  virtual void InvalidateInode(InodeNum inode, bool clear_pages) = 0;

  /**
   * Invalidate all inodes registered and entries added from the
   * kernel cache.  The actual invalidation is done in the thread used
   * to handle InvalidateInode() requests.
   */
  virtual void InvalidateAll() = 0;

  /**
   * Clean the manager.  This removes entries added by AddEntry() and
   * unregister inodes registered by RegisterInode() if they are added
   * / registered more than min_age ago.
   *
   */
  virtual void CleanMgr(int min_age) = 0;
};

}  // namespace client
}  // namespace cpfs
