#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the IStore interface.
 */

#include <stdint.h>

#include <cstddef>
#include <string>
#include <vector>

#include "common.hpp"

namespace cpfs {

struct FSTime;
class IDirIterator;
class IPosixFS;

namespace server {

class IDurableRange;
class IInodeRemovalTracker;

namespace ds {

/**
 * Empty block of memory for fast memcmp
 */
extern const char kEmptyBuf[kSegmentSize];

/**
 * File data iterator.  This is an interface designed to be able to
 * take advantage of the Linux FIEMAP ioctl to handle files with a
 * hole in it.  The file is broken into blocks of kSegmentSize, and
 * data for the whole block is missing, implementation may decide not
 * to return the block at all (not implemented yet).  The
 * implementation is not thread safe, only one thread should be
 * accessing the iterator at a time.
 */
class IFileDataIterator {
 public:
  virtual ~IFileDataIterator() {}

  /**
   * Get the file information represented in the file.
   *
   * @param mod_time_ret Where to return the last mod time
   *
   * @param file_size_ret Where to return the file size
   */
  virtual void GetInfo(FSTime* mod_time_ret, uint64_t* file_size_ret) = 0;

  /**
   * Read next file data into a buffer.  The data read is aligned to
   * be kSegmentSize bytes.  If end of file is reached, less than
   * kSegmentSize bytes may be read.  The returned value indicate the
   * number of bytes actually read.
   *
   * @param off_ret Where to return the file offset of the data read
   *
   * @param buf The buffer to hold the data read, should be of length
   * kSegmentSize
   *
   * @return The number of bytes read.  If it is less than
   * kSegmentSize, it means EOF is reached
   */
  virtual std::size_t GetNext(std::size_t* off_ret, char* buf) = 0;
};

/**
 * File data writer.  This is an interface designed to cache opened FD
 * and skip file seeking whenever possible.  The implementation is not
 * thread safe, only one thread should be accessing the same writer at
 * the same time.
 */
class IFileDataWriter {
 public:
  virtual ~IFileDataWriter() {}

  /**
   * Write content.
   *
   * @param buf The buffer containing data to write
   *
   * @param count The number of bytes in the buffer to write, should
   * be between 1 and kSegmentSize
   *
   * @param offset The file offset to start writing
   */
  virtual void Write(char* buf, std::size_t count, std::size_t offset) = 0;

  /**
   * Get the FD number.  Currently used only by unit test.
   */
  virtual int fd() = 0;
};

 /**
 * Allow iteration to the content of an inode by checksum group.
 */
class IChecksumGroupIterator {
 public:
  virtual ~IChecksumGroupIterator() {}

  /**
   * Get the file information for the inode.
   *
   * @param mod_time_ret Where to return the last mod time
   *
   * @param file_size_ret Where to return the file size
   */
  virtual void GetInfo(FSTime* mod_time_ret, uint64_t* file_size_ret) = 0;

  /**
   * Read next checksum group data into a buffer.  The data read is
   * aligned to be kSegmentSize bytes.
   *
   * @param off_ret Where to return the checksum group offset of the
   * data read
   *
   * @param buf The buffer to hold the data read, should be of length
   * kSegmentSize
   *
   * @return The number of bytes read.  If it is 0, it means EOF is
   * reached
   */
  virtual std::size_t GetNext(std::size_t* off_ret, char* buf) = 0;
};

 /**
 * Checksum group writer.  Write content to data and checksum files,
 * depending on the offset being used.  The implementation is not
 * thread safe, only one thread should be accessing the same writer at
 * the same time.  Also, the offsets used are assumed to be
 * kSegmentSize aligned.
 */
class IChecksumGroupWriter {
 public:
  virtual ~IChecksumGroupWriter() {}

  /**
   * Write content.
   *
   * @param buf The buffer containing data to write
   *
   * @param count The number of bytes in the buffer to write, should
   * be between 1 and kSegmentSize
   *
   * @param offset The checksum group offset to start writing
   */
  virtual void Write(char* buf, std::size_t count, std::size_t offset) = 0;
};

/**
 * Constants for flags argument of SetFileXattr
 */
enum SetFileXattrFlags {
  kSkipMtime = 1, /**< Skip mtime update */
  kSkipFileSize = 2 /**< Skip file size update */
};

/**
 * Interface for DS store to perform various operations
 */
class IStore {
 public:
  virtual ~IStore() {}

  /**
   * Set PosixFS to use.  Should be called before calling any other
   * methods in the class.
   *
   * @param posix_fs The PosixFS to use
   */
  virtual void SetPosixFS(IPosixFS* posix_fs) = 0;

  /**
   * Set tracker for recording deleted inodes.
   *
   * @param tracker Inode removal tracker
   */
  virtual void SetInodeRemovalTracker(IInodeRemovalTracker* tracker) = 0;

  /**
   * Set durable range for recording modified inode ranges.
   *
   * @param durable_range Durable range
   */
  virtual void SetDurableRange(IDurableRange* durable_range) = 0;

  /**
   * @return Whether the role of the data directory is already set
   */
  virtual bool is_role_set() = 0;

  /**
   * @return The group ID used
   */
  virtual GroupId ds_group() = 0;

  /**
   * @return The group role used
   */
  virtual GroupRole ds_role() = 0;

  /**
   * Get the UUID for this store
   */
  virtual std::string GetUUID() const = 0;

  /**
   * Set the role of the directory.  This can be used only when
   * GetRole() return false, or it returns the same group and role
   * information.
   *
   * @param group_id The group id.
   *
   * @param group_role The group role.
   */
  virtual void SetRole(GroupId group_id, GroupRole group_role) = 0;

  /**
   * Get a checksum group iterator for an inode.  This function does
   * not check whether the underlying files exist.  If not, the
   * resulting data iterator returns false on the first GetNext()
   * call.
   *
   * @param inode The inode
   *
   * @return The file data iterator
   */
  virtual IChecksumGroupIterator* GetChecksumGroupIterator(InodeNum inode) = 0;

  /**
   * Get a file data writer for an inode.
   *
   * @param inode The inode
   *
   * @return The file data writer
   */
  virtual IChecksumGroupWriter* GetChecksumGroupWriter(InodeNum inode) = 0;

  /**
   * List entries in the data directory.
   *
   * @return The directory iterator
   */
  virtual IDirIterator* List() = 0;

  /**
   * List inode paths in the store matching some ranges.
   *
   * @param ranges The ranges to match
   *
   * @return An iterator to list the paths, relative to data store
   * root directory
   */
  virtual IDirIterator* InodeList(const std::vector<InodeNum>& ranges) = 0;

  /**
   * Completely wipe up all data in the data directory.
   */
  virtual void RemoveAll() = 0;

  /**
   * Write data.
   *
   * @param inode The inode to write
   *
   * @param optime The operation time
   *
   * @param last_off End of file offset updated
   *
   * @param off The offset to write the data to.  The offset is from
   * the perspective of the DS.  The client is responsible for
   * converting filesystem file offset to DS file offset
   *
   * @param data The data to write
   *
   * @param size The size of the data
   *
   * @param checksum_change The location to write the checksum change
   * to.  It should have a space of size bytes
   *
   * @return Number of bytes written, or -errno
   */
  virtual int Write(InodeNum inode, const FSTime& optime,
                    uint64_t last_off, std::size_t off,
                    const void* data, std::size_t size,
                    void* checksum_change) = 0;

  /**
   * Read data to buffer.
   *
   * @param inode The inode to read
   *
   * @param cs Whether to read checksum file
   *
   * @param off The offset to read the data from.  The offset is from
   * the perspective of the DS.  The caller is responsible for
   * converting filesystem file offset to DS file offset
   *
   * @param data The buffer to read data to
   *
   * @param size The size of the data
   *
   * @return Number of bytes read, or -errno
   */
  virtual int Read(InodeNum inode, bool cs, std::size_t off, void* data,
                   std::size_t size) = 0;

  /**
   * Apply checksum delta to checksum file.
   *
   * @param inode The inode to write
   *
   * @param optime The operation time
   *
   * @param last_off End of file offset updated
   *
   * @param off The offset to update delta to.  The offset is from the
   * perspective of the DS.  The caller is responsible for converting
   * filesystem file offset to DS file offset
   *
   * @param data The data to write
   *
   * @param size The size of the data
   *
   * @param cs Whether the change is to be applied on the checksum
   * file.  If not, it is applied to the data file
   *
   * @return Number of bytes written, or -errno
   */
  virtual int ApplyDelta(InodeNum inode, const FSTime& optime,
                         uint64_t last_off, std::size_t off,
                         const void* data, std::size_t size,
                         bool cs = true) = 0;

  /**
   * Truncate data of a file in DS.  Also allow the leading bytes to
   * be retrieved, which is needed for checksum updates.  If the file
   * did not exist before, return success and zero out the req_data
   * buffer.
   *
   * @param inode The inode number
   *
   * @param optime The operation time
   *
   * @param data_off The DS data file size after truncation
   *
   * @param checksum_off The checksum file size after truncation
   *
   * @param size The number of leading bytes of truncated data to
   * retrieve
   *
   * @param req_data Return data from offset off to offset
   * data_req_end
   *
   * @return 0, or -errno
   */
  virtual int TruncateData(InodeNum inode, const FSTime& optime,
                           std::size_t data_off, std::size_t checksum_off,
                           std::size_t size, char* req_data) = 0;

  /**
   * Update the mtime record in DS.  Return error if the file didn't
   * exist before, or the attribute setting fails in any other way.
   * On successful update, current size recorded is returned to the
   * caller, which is needed because the Setattr protocol requires all
   * attribute update to be replied with current attributes.
   *
   * @param inode The inode number
   *
   * @param mtime The operation time
   *
   * @param size_ret Where to return current file size
   *
   * @return 0, or -errno
   */
  virtual int UpdateMtime(InodeNum inode, const FSTime& mtime,
                          uint64_t* size_ret) = 0;

  /**
   * Free a data file in DS.
   *
   * @param inode The inode number
   *
   * @param skip_tracker Whether to skip the inode removal tracker
   *
   * @return 0, or -errno
   */
  virtual int FreeData(InodeNum inode, bool skip_tracker) = 0;

  /**
   * Get mtime and file size for an inode.
   *
   * @param inode The inode to get attributes for
   *
   * @param mtime_ret Where to return the mtime
   *
   * @param size_ret Where to return the file size
   *
   * @return 0, or -errno
   */
  virtual int GetAttr(InodeNum inode,
                      FSTime* mtime_ret, uint64_t* size_ret) = 0;

  /**
   * Set mtime and file size for an inode.
   *
   * @param inode The inode to set attributes for
   *
   * @param mtime The mtime
   *
   * @param size The file size
   *
   * @param flags Whether to skip mtime or file size update
   *
   * @return 0, or -errno
   */
  virtual int SetAttr(InodeNum inode, const FSTime& mtime, uint64_t size,
                      SetFileXattrFlags flags = SetFileXattrFlags(0)) = 0;

  /**
   * Update mtime and file size for an inode.
   *
   * This is similar to Write() with size 0, but it is specially
   * crafted to cater for the needs of resync, so it works slightly
   * differently than Write() (and SetAttr()): if the file did not
   * exist before, the file is not created, and the durable range is
   * not updated.  The durable range update happens after the
   * operation completes successfully, which is okay because if the
   * resync operation fails, resync will be restarted anyway, so we
   * don't care whether the durable range is successfully updated.
   * Also, such errors are silently ignored.
   *
   * @param inode The inode to set attributes for
   *
   * @param optime The mtime
   *
   * @param last_off The file size
   */
  virtual void UpdateAttr(InodeNum inode, const FSTime& optime,
                          uint64_t last_off) = 0;

  /**
   * Get file system statistics of this store
   *
   * @param total_space Free bytes in the store
   *
   * @param free_space Total number of bytes available in the sore
   *
   * @return 0, or -errno
   */
  virtual int Stat(uint64_t* total_space, uint64_t* free_space) = 0;
};

}  // namespace ds
}  // namespace server
}  // namespace cpfs
