#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IInodeFimDeferMgr for tracking deferred Fims per inode.
 */
#include <stdint.h>

#include <vector>

#include <boost/intrusive_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"

namespace cpfs {

class IFim;
class IFimSocket;

namespace server {

/** Entry of Fim and the FimSocket to be deferred */
class DeferredFimEntry {
 public:
  DeferredFimEntry() {}

  /**
   * @param fim The Fim of the entry
   *
   * @param socket The socket of the entry
   */
  DeferredFimEntry(const FIM_PTR<IFim>& fim,
                   const boost::shared_ptr<IFimSocket>& socket)
      : fim_(fim), socket_(socket) {}

  /**
   * @return The Fim of the entry
   */
  const FIM_PTR<IFim>& fim() {
    return fim_;
  }

  /**
   * @return The socket of the entry
   */
  const boost::shared_ptr<IFimSocket>& socket() {
    return socket_;
  }

 private:
  FIM_PTR<IFim> fim_; /**< The Fim */
  boost::shared_ptr<IFimSocket> socket_; /**< The socket */
};

/**
 * Manage deferred Fims.  The class is not thread safe, and is
 * supposed to be private to a single worker thread.
 */
class IFimDeferMgr {
 public:
  virtual ~IFimDeferMgr() {}

  /**
   * Add a Fim entry to the manager.
   *
   * @param fim The Fim deferred
   *
   * @param socket The socket originating the Fim
   */
  virtual void AddFimEntry(const FIM_PTR<IFim>& fim,
                           const boost::shared_ptr<IFimSocket>& socket) = 0;

  /**
   * @return Whether the manager has any Fim entry
   */
  virtual bool HasFimEntry() const = 0;

  /**
   * @return The next Fim entry int the manager
   */
  virtual DeferredFimEntry GetNextFimEntry() = 0;
};

/**
 * Interface for tracking locked inodes.  Fim entries are added and
 * queued to this manager when the inode is being locked.  The class
 * is not thread safe, and is supposed to be private to a single
 * worker thread.
 */
class IInodeFimDeferMgr {
 public:
  virtual ~IInodeFimDeferMgr() {}

  /**
   * Check if an inode is locked
   *
   * @param inode The inode to check
   */
  virtual bool IsLocked(InodeNum inode) const = 0;

  /**
   * Set whether an inode is locked
   *
   * @param inode The inode to lock or unlock
   *
   * @param isLocked To lock or unlock
   */
  virtual void SetLocked(InodeNum inode, bool isLocked) = 0;

  /**
   * @return Inodes which are locked
   */
  virtual std::vector<InodeNum> LockedInodes() const = 0;

  /**
   * Add an Fim entry to be deferred
   *
   * @param inode The inode for fim defer
   *
   * @param fim The fim
   *
   * @param socket The FimSocket for response
   */
  virtual void AddFimEntry(InodeNum inode, const FIM_PTR<IFim>& fim,
                           const boost::shared_ptr<IFimSocket>& socket) = 0;

  /**
   * Check if the inode has Fim entries deferred
   */
  virtual bool HasFimEntry(InodeNum inode) const = 0;

  /**
   * Get and remove a deferred Fim entry from this manager
   */
  virtual DeferredFimEntry GetNextFimEntry(InodeNum inode) = 0;
};

/**
 * Interface for tracking locked segments.  Fim entries are added and
 * queued to this manager when the segment is being locked.  This
 * class is not thread safe, and is supposed to be private to a single
 * worker thread.
 *
 * This interface refers to a "segment number".  It is the physical
 * offset of the first byte of the segment within the DS,
 * integer-divided by kSegmentSize.
 */
class ISegmentFimDeferMgr {
 public:
  virtual ~ISegmentFimDeferMgr() {}

  /**
   * Check if a segment is locked.
   *
   * @param inode The inode number
   *
   * @param segment The segment number
   */
  virtual bool IsLocked(InodeNum inode, uint64_t segment) const = 0;

  /**
   * Set whether the inode is locked.
   *
   * @param inode The inode to lock or unlock
   *
   * @param segment The segment number
   *
   * @param isLocked To lock or unlock
   */
  virtual void SetLocked(InodeNum inode, uint64_t segment, bool isLocked) = 0;

  /**
   * Add an Fim entry to be deferred.
   *
   * @param inode The inode for fim defer
   *
   * @param segment The segment number
   *
   * @param fim The fim
   *
   * @param socket The FimSocket for response
   */
  virtual void AddFimEntry(InodeNum inode, uint64_t segment,
                           const FIM_PTR<IFim>& fim,
                           const boost::shared_ptr<IFimSocket>& socket) = 0;

  /**
   * Check if a segment has Fim entries deferred.
   *
   * @param inode The inode for fim defer
   *
   * @param segment The segment number
   */
  virtual bool SegmentHasFimEntry(InodeNum inode, uint64_t segment) const = 0;

  /**
   * Get and remove a deferred Fim entry for a segment.
   *
   * @param inode The inode for fim defer
   *
   * @param segment The segment number
   */
  virtual DeferredFimEntry GetSegmentNextFimEntry(InodeNum inode,
                                                  uint64_t segment) = 0;

  /**
   * Get and remove a deferred Fim entry of any segment.  If a segment
   * is found to have no next entry, it is marked unlocked as well.
   *
   * @param entry_ret Where to return the deferred Fim entry removed
   *
   * @return Whether any Fim entry is removed
   */
  virtual bool ClearNextFimEntry(DeferredFimEntry* entry_ret) = 0;
};

}  // namespace server
}  // namespace cpfs
