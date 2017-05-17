#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define InodeUsage for tracking inode operations in MS.
 */

#include <set>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Set of inodes
 */
typedef boost::unordered_set<InodeNum> InodeSet;

/**
 * Access requested by FC
 */
enum InodeAccess {
  kInodeReadAccess = 0, /**< Read-only access */
  kInodeWriteAccess     /**< Read-write access */
};

/**
 * Map of inode to access requested
 */
typedef boost::unordered_map<InodeNum, InodeAccess> InodeAccessMap;

/**
 * Map of client number to inodes
 */
typedef boost::unordered_map<ClientNum, InodeAccessMap> ClientInodeMap;

/**
 * Interface for tracking FCs using each inode in MS.  Also track
 * inodes which cannot be deleted yet due to usage.
 */
class IInodeUsage {
 public:
  virtual ~IInodeUsage() {}

  /**
   * Check whether an inode is being opened by some client.
   *
   * @param inode The inode
   *
   * @param write_only Whether to check only for writable clients
   */
  virtual bool IsOpened(InodeNum inode, bool write_only = false) const = 0;

  /**
   * Check whether an inode is being not opened for write by clients
   * except one.
   *
   * @param client The client to check for exclusive access
   *
   * @param inode The inode
   *
   * @return true if no other client has write access to the inode
   */
  virtual bool IsSoleWriter(ClientNum client, InodeNum inode) const = 0;

  /**
   * Notify that a client is opening an inode.
   *
   * @param client_num Client number
   *
   * @param inode The inode
   *
   * @param access The access requested in this Open
   */
  virtual void SetFCOpened(ClientNum client_num, InodeNum inode,
                           InodeAccess access = kInodeReadAccess) = 0;

  /**
   * Notify that a client has closed an inode.  After this call the
   * client will not write to the inode, but may still read if
   * keep_read is true.
   *
   * @param client_num Client number
   *
   * @param inode The inode
   *
   * @param keep_read Whether client may still read the inode
   *
   * @return Whether the inode changed from writable by some FC to
   * read-only / not used during this operation
   */
  virtual bool SetFCClosed(ClientNum client_num, InodeNum inode,
                           bool keep_read = false) = 0;

  /**
   * Get all inodes opened by a client.
   *
   * @param client_num Client number
   */
  virtual InodeSet GetFCOpened(ClientNum client_num) const = 0;

  /**
   * Swap the list of opened inodes and their access requested for a
   * client with the one specified.
   *
   * @param client_num Client number
   *
   * @param access_map New list of inodes opened by the client and
   * whether write access is allowed for them.  It will also receive
   * the old list
   */
  virtual void SwapClientOpened(ClientNum client_num,
                                InodeAccessMap* access_map) = 0;

  /**
   * @return A map of client to their lists of opened inodes and
   * access requested
   */
  virtual ClientInodeMap client_opened() const = 0;

  // PendingUnlink API

  /**
   * Record that an inode is pending removal.
   *
   * @param inode The inode
   */
  virtual void AddPendingUnlink(InodeNum inode) = 0;

  /**
   * Record that some inodes are pending removal.
   *
   * @param inodes The inodes
   */
  virtual void AddPendingUnlinks(const InodeSet& inodes) = 0;

  /**
   * Remove the record that an inode is pending removal.
   *
   * @param inode The inode
   */
  virtual void RemovePendingUnlink(InodeNum inode) = 0;

  /**
   * Check whether the inode is pending removal.
   *
   * @param inode The inode
   */
  virtual bool IsPendingUnlink(InodeNum inode) const = 0;

  /**
   * Clear the set of inodes pending removal.
   */
  virtual void ClearPendingUnlink() = 0;

  /**
   * @return The list of inodes pending removal
   */
  virtual InodeSet pending_unlink() const = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
