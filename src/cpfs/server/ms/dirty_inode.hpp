#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IDirtyInodeMgr for tracking inodes with volatile and dirty
 * attributes, and IAttrUpdater to update them.
 *
 * Because FCs defer the MS updates of inode attributes when writing
 * to files, the inode mtime and size in MS may be outdated.  The
 * dirty inode manager records inodes for which attributes in MS
 * cannot be trusted.  When the MS receives Lookup or Getattr requests
 * for these inodes, the AttrUpdater is invoked to get updated
 * attributes from DS for the inode.  This is also done when the FCs
 * no longer open the file for writing, when the MS updates its own
 * record of inode attributes.
 */

#include <stdint.h>

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "finfo.hpp"
#include "mutex_util.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Information we store
 */
struct DirtyInodeInfo {
  DirtyInodeInfo() : active(false), clean(true), cleaning(false), gen(0) {}
  bool active; /**< Whether the volatile inode is still active */
  bool clean; /**< Whether the inode is never marked modified */
  bool cleaning; /**< Whether the inode is being cleaned */
  uint64_t gen; /**< The generation number */
};

/**
 * Type for dirty inode information
 */
typedef boost::unordered_map<InodeNum, DirtyInodeInfo> DirtyInodeMap;

/**
 * Interface for tracking the need to query DS for inode attributes.
 *
 * Whenever the MS receives an open request from FC for writing an
 * inode, the inode becomes "active" in the dirty inode manager (as
 * oppose to "inactive", when no FC opened the file for writing).
 * This is maintained by having FC notifying the MS about any change
 * in whether it has opened an inode for writing.
 *
 * When the FC no longer needs to write to the file, it advises the MS
 * about whether the file has been written into, making the inode
 * "unclean".  If an inode is unclean when it becomes inactive, the MS
 * sends attribute update requests to DSs to clean it up.  Upon
 * completion, the dirty inode manager removes the record, so that
 * further query of the inode attributes no longer require DS updates.
 * Maintaining the "clean" status of inodes improves certain
 * benchmarks, e.g., repeatedly creating empty files, by not requiring
 * the system to get updated attributes from DSs when it is clearly
 * unnecessary.
 *
 * An inode which is either active or unclean has a record in the
 * dirty inode manager.  These inodes are said to be "volatile".  Any
 * volatile entries have a symlink in the filesystem, so that if the
 * system crashed and restarts, it can load use symlinks to know which
 * inodes need attribute updates.
 *
 * A generation number is attached to any entry in the dirty inode
 * manager.  Whenever the inode is set unclean, the generation number
 * is incremented.  The clean operation will succeed only if the
 * generation passed matches the one stored for the inode.

 * The manager also keeps a "version number", which is incremented on
 * every clean operation.  This allows callers to get the version
 * number, do some operation, and check whether an inode is unclean
 * afterwards.  If at the end the inode is clean, it can check whether
 * the version number is unchanged, to know whether the operation
 * retrieves reliable data, defending again cases where the data is
 * changed but marked clean after the operation is performed.
 */
class IDirtyInodeMgr {
 public:
  virtual ~IDirtyInodeMgr() {}

  /**
   * Reset the dirty inode list persisted.  If load persisted inodes
   * is specified, the previously persisted inodes are loaded and
   * become unclean but inactive.  Otherwise, all the inodes are clean
   * and inactive, in both memory and disk.
   *
   * @param load_persisted Whether to load persisted inodes
   */
  virtual void Reset(bool load_persisted) = 0;

  /**
   * @return The entries in the dirty inode manager, with their active
   * status, clean status and generation number
   */
  virtual DirtyInodeMap GetList() = 0;

  /**
   * @return The current version number
   */
  virtual uint64_t version() const = 0;

  /**
   * @param inode The inode
   *
   * @param version_ret Where to return version number.  If null, the
   * returning of version number is skipped
   *
   * @return Whether the inode is either active or unclean
   */
  virtual bool IsVolatile(InodeNum inode, uint64_t* version_ret = 0) const = 0;

  /**
   * Change the status of an inode in the dirty inode manager.
   *
   * @param inode The inode
   *
   * @param active Whether the inode is still active
   *
   * @param clean To be logical-and into the clean flag of the record,
   * to determine whether cleaning is necessary
   *
   * @param gen_ret Where to return the new generation number.  If null,
   * the setting is skipped
   *
   * @return Whether the inode is still clean
   */
  virtual bool SetVolatile(InodeNum inode, bool active, bool clean,
                           uint64_t* gen_ret) = 0;

  /**
   * Notify that mtime / size attributes of an inode is set.  This
   * invalidates previous update calls by incrementing the internal
   * generation number.  No effect if the inode is not volatile.
   *
   * @param inode The inode
   */
  virtual void NotifyAttrSet(InodeNum inode) = 0;

  /**
   * Start cleaning a dirty inode.  It checks whether the inode is
   * currently unclean but inactive, and that it is not already being
   * cleaned, advising whether the caller should proceed with
   * cleaning.
   *
   * @param inode The inode to start cleaning
   *
   * @return Whether the cleaning should proceed
   */
  virtual bool StartCleaning(InodeNum inode) = 0;

  /**
   * Clear cleaning flags for all inodes.  This is useful after a
   * failover, where previously started cleaning cannot be expected to
   * complete.
   */
  virtual void ClearCleaning() = 0;

  /**
   * Clean an inode if it is currently unclean but inactive, and its
   * generation matches the passed one.  It also unconditionally
   * unsets the cleaning flag, so that it is not considered to be
   * being cleaned.
   *
   * @param inode The inode to clean
   *
   * @param gen The inode generation for checking.  Will be filled if
   * false is returned.  The value filled is the current generation
   * number if the failure is due to an unmatched generation number,
   * and 0 otherwise
   *
   * @param lock Lock that will be filled if the inode is now clean,
   * which prevents further operations on the dirty inode manager.
   * This allows time for the store updates to be done while
   * preventing other threads from getting outdated attributes.  If 0,
   * use an internal lock that will be released on return of the
   * function
   *
   * @return Whether the inode is now clean
   */
  virtual bool Clean(InodeNum inode, uint64_t* gen,
                     boost::unique_lock<MUTEX_TYPE>* lock = 0) = 0;
};

/**
 * Function called when attribute updates complete
 */
typedef boost::function<void (FSTime mtime, uint64_t size)> AttrUpdateHandler;

/**
 * Update inode attributes from DS.  This sends messages to DSs if the
 * inode is volatile in the dirty inode manager, and run update
 * handler registered as replies are received.  The update of the
 * store and the dirty manager is not part of the updater.  Instead,
 * workers register a handler here when an actively unclean inode
 * becomes inactive.  In the handler, the worker should use the dirty
 * inode manager to determine whether the update is valid, and update
 * the manager / store or trigger additional update accordingly.
 */
class IAttrUpdater {
 public:
  virtual ~IAttrUpdater() {}

  /**
   * Start an attribute update, and call a handler when once the
   * attribute update is completed.
   *
   * @param inode The inode to update
   *
   * @param mtime The mtime currently recorded in the MS
   *
   * @param size The file size currently recorded in the MS
   *
   * @param complete_handler The handler to call
   *
   * @return Whether attribute update is needed.  If false,
   * complete_handler will not be called
   */
  virtual bool AsyncUpdateAttr(InodeNum inode, FSTime mtime, uint64_t size,
                               AttrUpdateHandler complete_handler) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
