#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the interfaces for data structures handling degrade mode
 * operations.
 */

#include <cstddef>

#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"

namespace cpfs {

class IFim;
class IFimSocket;

namespace server {
namespace ds {

/**
 * A handle for a recovered data segment in failed DS.  The handle is
 * not expected to be accessed concurrently, but can be obtained
 * recursively.
 */
class IDegradedCacheHandle : public boost::noncopyable {
 public:
  virtual ~IDegradedCacheHandle() {}

  /**
   * Check whether the handle is initialized.
   *
   * @return Whether the handle is initialized
   */
  virtual bool initialized() = 0;

  /**
   * Initialize the handle, indicating that the data stored is valid.
   * This effect can also be obtained by calling Allocate(), so this
   * method is useful mostly when the handle data is found to be
   * empty.
   */
  virtual void Initialize() = 0;

  /**
   * Allocate buffer for the data, if it has not been done before.
   */
  virtual void Allocate() = 0;

  /**
   * Get a pointer to the data stored for the handle.  If Allocate()
   * is never called, return the null pointer.
   */
  virtual char* data() = 0;

  /**
   * Read bytes from the cache.
   *
   * @param off The offset from the segment start to read from
   *
   * @param buf The buffer to read to
   *
   * @param buf_size The number of bytes to read
   */
  virtual void Read(std::size_t off, char* buf, std::size_t buf_size) = 0;

  /**
   * Write bytes to the cache.
   *
   * @param off The offset from the segment start to writing to
   *
   * @param buf The buffer to write
   *
   * @param buf_size The number of bytes to write
   *
   * @param checksum_change The checksum change to update checksum server
   */
  virtual void Write(std::size_t off, const char* buf,
                     std::size_t buf_size, char* checksum_change) = 0;

  /**
   * Revert Write() calls that has been made immediately before.
   *
   * @param checksum_change The checksum change to revert
   *
   * @param off The offset from the segment start to writing to
   *
   * @param buf_size The number of bytes to write
   */
  virtual void RevertWrite(std::size_t off, const char* checksum_change,
                           std::size_t buf_size) = 0;
};

/**
 * A cache of segments for the lost DS.
 */
class IDegradedCache : public boost::noncopyable {
 public:
  virtual ~IDegradedCache() {}

  /**
   * @return Whether the manager is now active
   */
  virtual bool IsActive() = 0;

  /**
   * Set the manager active or inactive.  The manager is created
   * inactive.  It must be activated before the cache can work.
   *
   * @param active Whether the manager is to be set active
   */
  virtual void SetActive(bool active) = 0;

  /**
   * Get handle for cache of an inode at a particular DSG offset.  If
   * a new handle is created, it will be uninitialized.  If the
   * manager is not active, always return the empty pointer.  The
   * returned handle must be kept alive when the data inside is
   * accessed.  Once the handle is removed (which should be done as
   * soon as possible to avoid cache out-of-memory conditions), the
   * cache is made available for cache eviction.
   *
   * @param inode The inode number of the inode
   *
   * @param dsg_off The DSG file offset
   *
   * @return The handle
   */
  virtual boost::shared_ptr<IDegradedCacheHandle> GetHandle(
      InodeNum inode, std::size_t dsg_off) = 0;

  /**
   * Free all data kept for an inode.
   *
   * @param inode The inode number of the inode
   */
  virtual void FreeInode(InodeNum inode) = 0;

  /**
   * Truncate cache for an inode.  Note that the cache data will not
   * be truncated by this call, they have to be truncated in some
   * other means (usually Write() to the cache handle).
   *
   * @param inode The inode number of the inode
   *
   * @param dsg_off The DSG offset at the new end of the inode
   */
  virtual void Truncate(InodeNum inode, std::size_t dsg_off) = 0;
};

/**
 * Handle to arrange events to be handled when the DS is degraded.
 * The handle is not expected to be accessed concurrently.
 */
class IDataRecoveryHandle : public boost::noncopyable {
 public:
  virtual ~IDataRecoveryHandle() {}

  /**
   * Set the handle as started.
   *
   * @return Whether the handle has been set as started before
   */
  virtual bool SetStarted() = 0;

  /**
   * Add data sent from a peer for recovery.
   *
   * @param data The data added.  If NULL, notify empty data Fim
   * received
   *
   * @param peer The peer sending the data
   *
   * @return Whether all peers have sent data
   */
  virtual bool AddData(char* data,
                       const boost::shared_ptr<IFimSocket>& peer) = 0;

  /**
   * Recover data using the current checksum and data sent from peer.
   * Also unset the list of DS having sent checksum requests, so that
   * they can send checksum again.
   *
   * @param checksum The current checksum
   *
   * @return Whether any data is detected
   */
  virtual bool RecoverData(char* checksum) = 0;

  /**
   * Queue a Fim.  The Fim can be obtained by UnqueueFim().
   *
   * @param fim The Fim to queue
   *
   * @param peer The peer sending the Fim
   */
  virtual void QueueFim(const FIM_PTR<IFim>& fim,
                        const boost::shared_ptr<IFimSocket>& peer) = 0;

  /**
   * @return Whether recovery data has been sent by a peer
   */
  virtual bool DataSent(const boost::shared_ptr<IFimSocket>& peer) = 0;

  /**
   * Unqueue a Fim queued by QueueFim().
   *
   * @param peer_ret Where to return the peer sending the Fim unqueued
   *
   * @return The Fim unqueued, or empty shared_ptr if no more Fim can
   * be unqueued
   */
  virtual FIM_PTR<IFim> UnqueueFim(
      boost::shared_ptr<IFimSocket>* peer_ret) = 0;
};

/**
 * Manager of IDataRecoveryHandle's.
 */
class IDataRecoveryMgr : public boost::noncopyable {
 public:
  virtual ~IDataRecoveryMgr() {}

  /**
   * Get handle for recovery for a segment of an inode.
   *
   * @param inode The inode number of the inode
   *
   * @param dsg_off The DSG offset of the segment
   *
   * @param create Whether to create the handle if it is not there
   */
  virtual boost::shared_ptr<IDataRecoveryHandle> GetHandle(
      InodeNum inode, std::size_t dsg_off, bool create) = 0;

  /**
   * Indicate the completion of recovery work which a handle is
   * created for.  The handle will be dropped from the manager.
   *
   * @param inode The inode number of the inode
   *
   * @param dsg_off The DSG offset of the segment
   */
  virtual void DropHandle(InodeNum inode, std::size_t dsg_off) = 0;
};

}  // namespace ds
}  // namespace server
}  // namespace cpfs
