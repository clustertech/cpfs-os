/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IStore interface.
 */

#include "server/ds/store_impl.hpp"

#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include <linux/fs.h>
#include <linux/types.h>  // IWYU pragma: keep
// Only after linux/types.h
#include <linux/fiemap.h>  // NOLINT(build/include_alpha)

#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/format.hpp>
#include <boost/scope_exit.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "dir_iterator_impl.hpp"
#include "ds_iface.hpp"
#include "finfo.hpp"
#include "logger.hpp"
#include "posix_fs.hpp"
#include "store_util.hpp"
#include "util.hpp"
#include "server/ds/store.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * Get extended attributes kept for representing various meta data of
 * a user file.
 *
 * @param path The path of the file representing the user file
 *
 * @param mod_time_ret Where to return the mtime
 *
 * @param file_size_ret Where to return the file size
 *
 * @return 0 on success, -errno on error
 */
int GetFileXattr(const char* path, FSTime* mod_time_ret,
                 uint64_t* file_size_ret) {
  char buf[128];
  int len = GET_XATTR(path, "user.mt", buf);
  if (len < 0)
    return len;
  if (!ParseXattrTime(buf, mod_time_ret))
    return -EIO;
  len = GET_XATTR(path, "user.fs", buf);
  if (len < 0 || !ParseFileSize(buf, len, file_size_ret))
    return -EIO;
  return 0;
}

/**
 * Get full path for a particular type of per-inode file stored in DS.
 *
 * @param base_path The base path for files of the inode
 *
 * @param cs Whether the full path requested is for checksum
 */
std::string DSFullPath(std::string base_path, bool cs) {
  return base_path + (cs ? ".c" : ".d");
}

/**
 * Whether file extent API is used for iterating files.  This is unset
 * the first time when the FS_IOC_FIEMAP ioctl returns EOPNOTSUPP.
 */
bool use_fie = true;

/**
 * Size of buffer for performing FIEMAP operation.
 */
const unsigned kFieBufSize = 32768U;

/**
 * Implement the IFileDataIterator interface.
 */
class FileDataIterator : public IFileDataIterator {
 public:
  /**
   * @param filename The name of the file to get data from
   */
  explicit FileDataIterator(const std::string& filename)
      : filename_(filename), fd_(-1), off_(0),
        curr_fie_entry_(0), eof_(false), use_fie_(use_fie) {
    map_.fm_mapped_extents = 0;
    InitIterator();
  }

  ~FileDataIterator() {
    if (fd_ >= 0)
      close(fd_);  // Ignore error
    fd_ = -1;
  }

  void GetInfo(FSTime* mod_time_ret, uint64_t* file_size_ret) {
    mod_time_ret->sec = 0;
    mod_time_ret->ns = 0;
    *file_size_ret = 0;
    GetFileXattr(filename_.c_str(), mod_time_ret, file_size_ret);
  }

  std::size_t GetNext(std::size_t* off_ret, char* buf) {
    while (!eof_) {
      DoSeek();
      if (eof_)
        break;
      *off_ret = off_;
      std::size_t ret = read(fd_, buf, kSegmentSize);
      off_ += kSegmentSize;
      if (ret == std::size_t(-1))
        throw std::runtime_error(
            (boost::format("Cannot read file %s") % filename_).str());
      if (ret < kSegmentSize)
        eof_ = true;
      if (std::memcmp(buf, kEmptyBuf, ret))
        return ret;
    }
    return 0;
  }

 private:
  std::string filename_; /**< Name of file */
  int fd_; /**< File descriptor of file to read */
  std::size_t off_; /**< Next offset to read */
  std::size_t curr_fie_entry_; /**< Current entry in map_ being checked */
  bool eof_; /**< Whether EOF is reached */
  bool use_fie_; /**< Whether FIE is still to be used */
  union {
    struct fiemap map_; /**< FIE map structure */
    char buf_[kFieBufSize]; /**< Buffer for FIE map data */
  };

  bool InitIterator() {
    fd_ = open(filename_.c_str(), O_RDONLY);
    eof_ = fd_ < 0;
    return !eof_;
  }

  void DoSeek() {
    if (!use_fie_)
      return;
    if (curr_fie_entry_ >= map_.fm_mapped_extents) {
      DoFiemap();
      if (!use_fie_)
        return;
      if (map_.fm_mapped_extents == 0) {  // No more extent, EOF
        eof_ = true;
        return;
      }
    }
    struct fiemap_extent& extent = map_.fm_extents[curr_fie_entry_];
    std::size_t new_off = extent.fe_logical / kSegmentSize * kSegmentSize;
    if (new_off > off_) {
      off_ = new_off;
      lseek(fd_, new_off, SEEK_SET);
    }
    if (off_ + kSegmentSize >= extent.fe_logical + extent.fe_length)
      ++curr_fie_entry_;
    // Post-condition: either curr_fie_entry_ has exceeded the end of
    // extents returned by the ioctl, or it can be used to check for
    // the next offset.
  }

  void DoFiemap() {
    curr_fie_entry_ = 0;
    std::memset(&buf_, '\0', sizeof(buf_));
    map_.fm_start = off_;
    map_.fm_length = ~0;
    map_.fm_flags = FIEMAP_FLAG_SYNC;
    map_.fm_extent_count = (kFieBufSize - sizeof(map_))
        / sizeof(struct fiemap_extent);
    int ret = ioctl(fd_, FS_IOC_FIEMAP, &map_);
    if (ret < 0) {
      use_fie_ = false;
      use_fie = use_fie && errno != EOPNOTSUPP;
    }
    // Post-condition: either there is no entry, or the first entry is
    // usable for checking off_
  }
};

/**
 * Implement the IChecksumGroupIterator interface.
 */
class ChecksumGroupIterator : public IChecksumGroupIterator {
 public:
  /**
   * @param path The inode path to the files to iterate, without
   * the ".c" or ".d" suffix
   *
   * @param inode The inode number
   *
   * @param role The DS role
   **/
  ChecksumGroupIterator(const std::string& path, InodeNum inode, GroupRole role)
      : inode_(inode), role_(role) {
    // We want to combine the data and checksum stored on the DS, in
    // the same ordering as the DSG offsets.  Because of the possibility
    // of file holes, we do not know whether to return the data or
    // checksum block next.  So we buffer both, and whenever we are asked
    // to return the next block, we return the one with the smaller DSG
    // offset and refill it, maintaining the invariant that both the data
    // and checksum buffers are filled.
    for (int i = 0; i < 2; ++i) {
      iters_[i].reset(new FileDataIterator(DSFullPath(path, bool(i))));
      sizes_[i] = kSegmentSize;
      Refill(i);
    }
  }

  void GetInfo(FSTime* mod_time_ret, uint64_t* file_size_ret) {
    return iters_[0]->GetInfo(mod_time_ret, file_size_ret);
  }

  std::size_t GetNext(std::size_t* off_ret, char* buf) {
    int chosen = cg_[0] < cg_[1] ? 0 : 1;
    if (cg_[chosen] == std::numeric_limits<std::size_t>::max())
      return 0;
    int ret = sizes_[chosen];
    std::memcpy(buf, bufs_[chosen], ret);
    *off_ret = cg_[chosen];
    Refill(chosen);
    return ret;
  }

 private:
  InodeNum inode_; /**< The inode number */
  GroupRole role_; /**< The group role */
  boost::scoped_ptr<FileDataIterator> iters_[2]; /**< Underlying iterators */
  std::size_t cg_[2]; /**< Next checksum group numbers to return */
  std::size_t sizes_[2]; /**< Next sizes to return */
  char bufs_[2][kSegmentSize]; /**< Next buffers to return */

  void Refill(int i) {
    if (sizes_[i] != kSegmentSize) {
      cg_[i] = std::numeric_limits<std::size_t>::max();
      sizes_[i] = 0;
      return;
    }
    std::size_t off;
    sizes_[i] = iters_[i]->GetNext(&off, bufs_[i]);
    if (sizes_[i] == 0)
      cg_[i] = std::numeric_limits<std::size_t>::max();
    else if (i)
      cg_[i] = CgStart(ChecksumToDsgOffset(inode_, role_, off));
    else
      cg_[i] = CgStart(DataToDsgOffset(inode_, role_, off));
  }
};

/**
 * Implement the IFileDataWriter interface.
 */
class FileDataWriter : public IFileDataWriter {
 public:
  /**
   * Constructor.  Unlink and open the file immediately.
   */
  explicit FileDataWriter(const std::string& filename)
      : filename_(filename), off_(0) {
    fd_ = open(filename.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd_ < 0)
      ThrowSysError("Cannot open %s for write: %s");
  }

  ~FileDataWriter() {
    if (close(fd_) < 0) {
      char buf[256] = "";
      strerror_r(errno, buf, 255);
      LOG(error, Store, "Error closing file after writing ", filename_,
          ": ", buf);
    }
  }

  void Write(char* buf, std::size_t count, std::size_t offset) {
    if (offset != off_)
      lseek(fd_, offset, SEEK_SET);
    int ret = write(fd_, buf, count);
    if (ret < 0)
      ThrowSysError("Failed writing to %s: %s");
    off_ = offset + count;
  }

  virtual int fd() {
    return fd_;
  }

 private:
  int fd_;
  std::string filename_;
  std::size_t off_;

  /**
   * Throw an exception base on a system error.
   *
   * @param msg The error message
   */
  void ThrowSysError(const char* msg) {
    char buf[256] = "";
    strerror_r(errno, buf, 255);
    throw std::runtime_error((boost::format(msg) % filename_ % buf).str());
  }
};

/**
 * Implement the IChecksumGroupWriter interface.
 */
class ChecksumGroupWriter : public IChecksumGroupWriter {
 public:
  /**
   * @param path The inode path to the files to iterate, without
   * the ".c" or ".d" suffix
   *
   * @param inode The inode number
   *
   * @param role The DS role
   **/
  ChecksumGroupWriter(const std::string& path, InodeNum inode, GroupRole role)
      : path_(path), inode_(inode), role_(role) {
    iters_[0].reset(new FileDataWriter(DSFullPath(path, false)));
  }

  void Write(char* buf, std::size_t count, std::size_t offset) {
    std::size_t dsg_off = offset;
    std::size_t off = RoleDsgToDataOffset(inode_, role_, &dsg_off);
    if (dsg_off < offset + kChecksumGroupSize) {
      // Data dsg_off is within this checksum group, so I'm responsible for data
      iters_[0]->Write(buf, count, off);
      return;
    }
    // I'm not responsible for data, so I'm responsible for checksum
    if (!iters_[1])
      iters_[1].reset(new FileDataWriter(DSFullPath(path_, true)));
    iters_[1]->Write(buf, count, DsgToChecksumOffset(offset));
  }

 private:
  std::string path_;
  InodeNum inode_; /**< The inode number */
  GroupRole role_; /**< The group role */
  boost::scoped_ptr<FileDataWriter> iters_[2]; /**< Underlying writers */
};

/**
 * Implement the IStore interface.
 */
class Store : public IStore {
 public:
  /**
   * @param data_path The directory path.
   */
  explicit Store(std::string data_path);
  std::string GetUUID() const;
  void SetPosixFS(IPosixFS* posix_fs) { posix_fs_ = posix_fs; }
  void SetInodeRemovalTracker(IInodeRemovalTracker* inode_removal_tracker) {
    inode_removal_tracker_ = inode_removal_tracker;
  }
  void SetDurableRange(IDurableRange* durable_range) {
    durable_range_ = durable_range;
  }
  bool is_role_set() { return role_set_; }
  GroupId ds_group() { return group_id_; }
  GroupRole ds_role() { return group_role_; }
  void SetRole(GroupId group_id, GroupRole group_role);
  IChecksumGroupIterator* GetChecksumGroupIterator(InodeNum inode);
  IChecksumGroupWriter* GetChecksumGroupWriter(InodeNum inode);
  IDirIterator* List();
  IDirIterator* InodeList(const std::vector<InodeNum>& ranges);
  void RemoveAll();
  int Write(InodeNum inode, const FSTime& optime, uint64_t last_off,
            std::size_t off, const void* data, std::size_t size,
            void* checksum_change);
  int Read(InodeNum inode, bool cs, std::size_t off,
           void* data, std::size_t size);
  int ApplyDelta(InodeNum inode, const FSTime& optime, uint64_t last_off,
                 std::size_t off, const void* data, std::size_t size, bool cs);
  int TruncateData(InodeNum inode, const FSTime& optime,
                   std::size_t data_off, std::size_t checksum_off,
                   std::size_t size, char* req_data);
  int UpdateMtime(InodeNum inode, const FSTime& mtime, uint64_t* size_ret);
  int FreeData(InodeNum inode, bool skip_tracker);
  int GetAttr(InodeNum inode, FSTime* mtime_ret, uint64_t* size_ret);
  int SetAttr(InodeNum inode, const FSTime& mtime, uint64_t size,
              SetFileXattrFlags flags);
  void UpdateAttr(InodeNum inode, const FSTime& optime, uint64_t last_off);
  int Stat(uint64_t* total_space, uint64_t* free_space);

 private:
  std::string data_path_;
  IPosixFS* posix_fs_;
  IInodeRemovalTracker* inode_removal_tracker_;
  IDurableRange* durable_range_;
  bool role_set_;
  GroupId group_id_;
  GroupRole group_role_;
  std::string uuid_;

  /**
   * Read the role kept in the directory.
   */
  void ReadRole();

  /**
   * Get the root directory
   */
  std::string GetRoot(const std::string& parent) {
    return data_path_ + "/" + parent;
  }

  /**
   * @return The inode path for an inode
   */
  std::string GetInodePath(InodeNum inode) {
    std::string inode_s = GetInodeStr(inode);
    return GetRoot(inode_s.substr(0, 3)) + "/" + inode_s;
  }

  /**
   * Read a fd, fill unread bytes as null.
   *
   * @param fd The fd to read
   *
   * @param data Where to put the data read
   *
   * @param size The number of bytes to read
   *
   * @param off The offset of the file to read
   */
  int ReadAndFill(int fd, void* data, std::size_t size, std::size_t off) {
    std::size_t ret = posix_fs_->Pread(fd, data, size, off);
    if (ret == std::size_t(-1))
      return -1;
    if (ret < size)
      std::memset(reinterpret_cast<char*>(data) + ret, '\0', size - ret);
    return ret;
  }

  /**
   * Update file extended attributes.
   *
   * @param data_path The path to the inode date file
   *
   * @param optime The time of operation
   *
   * @param last_off The last file offset
   *
   * @param force Whether to create the file if the file has not
   * existed before
   *
   * @return 0, or -errno
   */
  int UpdateFileXattr(const char* data_path,
                      const FSTime& optime, uint64_t last_off,
                      bool force = false) {
    FSTime f_optime = {0, 0};
    uint64_t f_size = 0;
    int ret = GetFileXattr(data_path, &f_optime, &f_size);
    if (ret == -ENOENT && force) {
      int fd = open(data_path, O_CREAT | O_RDWR, 0666);
      if (fd < 0)
        return -errno;
      close(fd);
      ret = 0;
    }
    if (f_optime < optime)
      f_optime = optime;
    if (f_size < last_off)
      f_size = last_off;
    return SetFileXattr(data_path, f_optime, f_size);
  }

  /**
   * Set the extended attributes stored in data file in DS.
   *
   * @param path The path to the data file
   *
   * @param mod_time The last mod date
   *
   * @param file_size The file size
   *
   * @param flags Whether either the mod time or file size update should
   * be skipped
   *
   * @return 0 if successful, -errno if failed.
   */
  int SetFileXattr(const char* path, const FSTime& mod_time,
                   uint64_t file_size,
                   SetFileXattrFlags flags = SetFileXattrFlags(0)) {
    if (!(flags & kSkipMtime)) {
      char buf[128];
      int len = GetFileTimeStr(buf, mod_time);
      if (posix_fs_->Lsetxattr(path, "user.mt", buf, len, 0) == -1)
        return -errno;
    }
    if (!(flags & kSkipFileSize)) {
      std::string str = GetInodeStr(file_size);
      if (posix_fs_->Lsetxattr(path, "user.fs", str.data(), str.size(), 0)
          == -1)
        return -errno;
    }
    return 0;
  }

  /**
   * Open and perhaps create a file for read/write.
   *
   * @param path The path to open or create the file
   *
   * @return File descriptor, or -errno
   */
  int OpenOrCreate(const std::string& path) {
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
      if (errno == ENOENT)
        fd = open(path.c_str(), O_CREAT | O_RDWR, 0666);
      if (fd < 0)
        return -errno;
    }
    return fd;
  }

  /**
   * Open and perhaps create a file for read/write.  If created, set
   * attributes if the filename looks like a data file.
   *
   * @param path The path to create the file
   *
   * @param created_ret Where to return a flag about whether creation
   * occurred
   *
   * @param optime The operation time
   *
   * @param last_off The last offset
   *
   * @return File descriptor, or -errno
   */
  int OpenOrCreate(const std::string& path, bool* created_ret,
                   const FSTime& optime, uint64_t last_off) {
    *created_ret = false;
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
      if (errno != ENOENT)
        return -errno;
      fd = open(path.c_str(), O_CREAT | O_RDWR, 0666);
      if (fd < 0)
        return -errno;
      *created_ret = true;
      int ret = UpdateFileXattr(path.c_str(), optime, last_off);
      if (ret != 0) {
        unlink(path.c_str());
        return ret;
      }
    }
    return fd;
  }

  ssize_t PwriteAll(int fd, const void* buf, size_t count, off_t offset) {
    ssize_t ret = posix_fs_->Pwrite(fd, buf, count, offset);
    if (size_t(ret) < count) {
      ret = -1;
      errno = ENOSPC;
    }
    return ret;
  }

  /**
   * Create empty directory
   *
   * @param dir The directory path to create
   */
  void CreateEmptyDirectory(const std::string& dir) {
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST)
      throw std::runtime_error(
          (boost::format("Cannot create directory %s") % dir).str());
  }

  /**
   * Check if the store has been initialized
   */
  bool IsInitialized() {
    if (!ValidateDirectory(data_path_))
      return false;
    // For simplicity, base dirs are not all checked for existence
    if (!ValidateDirectory(data_path_ + "/000"))
      return false;
    return true;
  }

  /**
   * Validate that the directory is ready to use
   */
  bool ValidateDirectory(const std::string& path) {
    struct stat buf;
    if (stat(path.c_str(), &buf) == -1)
      return false;
    if (!S_ISDIR(buf.st_mode))
      return false;
    if (access(path.c_str(), R_OK | W_OK | X_OK) != 0)
      throw std::runtime_error("Insufficient permission to directory: " + path);
    return true;
  }
};

Store::Store(std::string data_path)
    : data_path_(data_path), inode_removal_tracker_(0), durable_range_(0) {
  if (!IsInitialized()) {
    CreateEmptyDirectory(data_path_);
    for (unsigned d_idx = 0; d_idx < kNumBaseDir; ++d_idx) {
      char buf[4];
      std::snprintf(buf, sizeof(buf), "%03x", d_idx);
      CreateEmptyDirectory(GetRoot(buf));
    }
  }
  uuid_ = LoadOrCreateUUID(data_path_);
  LOG(notice, Server, "Data Server UUID: ", uuid_.c_str());
  ReadRole();
}

std::string Store::GetUUID() const {
  return uuid_;
}

void Store::ReadRole() {
  group_id_ = 0;
  group_role_ = 0;
  char buf[128];
  int len = lgetxattr(data_path_.c_str(), "user.role", buf, sizeof(buf));
  if (len <= 0) {
    role_set_ = false;
    return;
  }
  buf[len] = 0;
  char* eptr;
  errno = 0;
  GroupId group_id = strtoul(buf, &eptr, 10);
  if (errno != 0 || *eptr != '-')
    throw std::runtime_error("Invalid group ID set in data directory");
  GroupRole group_role = strtoull(eptr + 1, &eptr, 10);
  if (errno != 0 || *eptr != '\0')
    throw std::runtime_error("Invalid group role set in data directory");
  role_set_ = true;
  group_id_ = group_id;
  group_role_ = group_role;
}

void Store::SetRole(GroupId group_id, GroupRole group_role) {
  if (role_set_) {
    if (group_id != group_id_ || group_role != group_role_)
      throw std::runtime_error("Attempting to change data directory role");
    else
      return;
  }
  char buf[128];
  int ret = std::snprintf(buf, sizeof(buf), "%" PRIu32 "-%" PRIu32,
                          group_id, group_role);
  lsetxattr(data_path_.c_str(), "user.role", buf, ret, 0);
  role_set_ = true;
  group_id_ = group_id;
  group_role_ = group_role;
}

IChecksumGroupIterator* Store::GetChecksumGroupIterator(InodeNum inode) {
  return MakeChecksumGroupIterator(GetInodePath(inode), inode, group_role_);
}

IChecksumGroupWriter* Store::GetChecksumGroupWriter(InodeNum inode) {
  return MakeChecksumGroupWriter(GetInodePath(inode), inode, group_role_);
}

IDirIterator* Store::List() {
  return MakeLeafViewDirIterator(data_path_);
}

IDirIterator* Store::InodeList(const std::vector<InodeNum>& ranges) {
  return MakeInodeRangeDirIterator(data_path_, ranges, ".d");
}

void Store::RemoveAll() {
  boost::scoped_ptr<IDirIterator> iter(List());
  std::string prefixed_inode;
  bool is_dir;
  while (iter->GetNext(&prefixed_inode, &is_dir)) {
    if (is_dir)
      continue;
    std::string to_remove = data_path_ + "/" + prefixed_inode;
    if (unlink(to_remove.c_str()) != 0)
      throw std::runtime_error(
          (boost::format("Cannot remove file %s: %s")
           % prefixed_inode % errno).str());
  }
}

int Store::Write(InodeNum inode, const FSTime& optime, uint64_t last_off,
                 std::size_t off, const void* data, std::size_t size,
                 void* checksum_change) {
  if (size > kSegmentSize)
    return -EINVAL;
  if (durable_range_)
    durable_range_->Add(inode);
  std::string data_path = DSFullPath(GetInodePath(inode), false);
  bool created;
  int fd = OpenOrCreate(data_path, &created, optime, last_off);
  if (fd < 0)
    return fd;
  BOOST_SCOPE_EXIT(&fd) {
    close(fd);
  } BOOST_SCOPE_EXIT_END;
  boost::scoped_array<char> buf(new char[size]);
  int num_bytes = ReadAndFill(fd, checksum_change, size, off);
  if (num_bytes == -1)
    return -errno;
  int ret = PwriteAll(fd, data, size, off);
  if (ret == -1) {
    ret = -errno;
    if (errno == ENOSPC)  // Attempt reverting
      posix_fs_->Pwrite(fd, checksum_change, size, off);
    return ret;
  }
  XorBytes(reinterpret_cast<char*>(checksum_change),
           reinterpret_cast<const char*>(data), num_bytes);
  std::memcpy(reinterpret_cast<char*>(checksum_change) + num_bytes,
              reinterpret_cast<const char*>(data) + num_bytes,
              size - num_bytes);
  if (!created)
    UpdateFileXattr(data_path.c_str(), optime, last_off);
  return ret;
}

int Store::Read(InodeNum inode, bool cs, std::size_t off, void* data,
                std::size_t size) {
  int fd = open(DSFullPath(GetInodePath(inode), cs).c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno != ENOENT)
      return -errno;
    std::memset(data, '\0', size);
    return 0;
  }
  BOOST_SCOPE_EXIT(&fd) {
    close(fd);
  } BOOST_SCOPE_EXIT_END;
  int ret = ReadAndFill(fd, data, size, off);
  return ret >= 0 ? ret : -errno;
}

int Store::ApplyDelta(InodeNum inode, const FSTime& optime, uint64_t last_off,
                      std::size_t off, const void* data, std::size_t size,
                      bool cs) {
  std::string cs_path = DSFullPath(GetInodePath(inode), cs);
  int fd = OpenOrCreate(cs_path.c_str());
  if (fd < 0)
    return -errno;
  BOOST_SCOPE_EXIT(&fd) {
    close(fd);
  } BOOST_SCOPE_EXIT_END;
  if (durable_range_)
    durable_range_->Add(inode);
  boost::scoped_array<char> buf(new char[size]);
  int num_bytes = ReadAndFill(fd, buf.get(), size, off);
  if (num_bytes == -1)
    return -errno;
  XorBytes(buf.get(), reinterpret_cast<const char*>(data), num_bytes);
  std::memcpy(buf.get() + num_bytes,
              reinterpret_cast<const char*>(data) + num_bytes,
              size - num_bytes);
  int ret = PwriteAll(fd, buf.get(), size, off);
  if (ret == -1) {
    ret = -errno;
    if (errno == ENOSPC) {  // Attempt reverting
      XorBytes(buf.get(), reinterpret_cast<const char*>(data), size);
      posix_fs_->Pwrite(fd, buf.get(), size, off);
    }
    return ret;
  }
  int res = UpdateFileXattr(DSFullPath(GetInodePath(inode), false).c_str(),
                            optime, last_off, true);
  if (res != 0) {
    LOG(warning, Store,
        "Unexpected failure when updating file time of ",
        PHex(inode), " upon checksum update, errno = ", PVal(errno));
  }
  return ret;
}

int Store::TruncateData(InodeNum inode, const FSTime& optime,
                        std::size_t data_off, std::size_t checksum_off,
                        std::size_t size, char* req_data) {
  std::string inode_path = GetInodePath(inode);
  std::string data_path = DSFullPath(inode_path, false);
  int fd = open(data_path.c_str(), O_RDWR, 0666);
  if (fd < 0) {
    if (errno != ENOENT)
      return -errno;
    std::memset(req_data, '\0', size);
  } else {
    BOOST_SCOPE_EXIT(&fd) {
      close(fd);
    } BOOST_SCOPE_EXIT_END;
    if (size > 0 && ReadAndFill(fd, req_data, size, data_off) < 0)
      return -errno;
    if (durable_range_)
      durable_range_->Add(inode);
    if (ftruncate(fd, data_off) < 0)
      return -errno;
    SetFileXattr(data_path.c_str(), optime, 0);
  }
  std::string cs_path = DSFullPath(inode_path, true);
  if (truncate(cs_path.c_str(), checksum_off) < 0)
    if (errno != ENOENT)
      LOG(warning, Store,
          "Unexpected failure when truncating checksum for inode ",
          PHex(inode), ": errno = ", PVal(errno));
  return 0;
}

int Store::UpdateMtime(InodeNum inode, const FSTime& mtime,
                       uint64_t* size_ret) {
  std::string path = DSFullPath(GetInodePath(inode), false);
  const char* cpath = path.c_str();
  if (durable_range_)
    durable_range_->Add(inode);
  FSTime dummy;
  if (SetFileXattr(cpath, mtime, 0, kSkipFileSize) == 0)
    GetFileXattr(cpath, &dummy, size_ret);
  return 0;
}

int Store::FreeData(InodeNum inode, bool skip_tracker) {
  if (!skip_tracker)
    inode_removal_tracker_->RecordRemoved(inode);
  for (int i = 1; i >= 0; --i)  // Free data last to prevent leftover checksum
    if (unlink(DSFullPath(GetInodePath(inode), bool(i)).c_str()) != 0
        && errno != ENOENT)
      return -errno;
  return 0;
}

int Store::GetAttr(InodeNum inode, FSTime* mtime_ret, uint64_t* size_ret) {
  return GetFileXattr(DSFullPath(GetInodePath(inode), false).c_str(),
                      mtime_ret, size_ret);
}

int Store::SetAttr(InodeNum inode, const FSTime& mtime, uint64_t size,
                   SetFileXattrFlags flags) {
  if (durable_range_)
    durable_range_->Add(inode);
  return SetFileXattr(DSFullPath(GetInodePath(inode), false).c_str(),
                      mtime, size, flags);
}

void Store::UpdateAttr(
    InodeNum inode, const FSTime& optime, uint64_t last_off) {
  std::string data_path = DSFullPath(GetInodePath(inode), false);
  if (UpdateFileXattr(data_path.c_str(), optime, last_off) == 0)
    if (durable_range_)
      durable_range_->Add(inode);
}

int Store::Stat(uint64_t* total_space, uint64_t* free_space) {
  struct statvfs stvfsbuf;
  int ret = statvfs(data_path_.c_str(), &stvfsbuf);
  if (ret != 0)
    return ret;
  *total_space = stvfsbuf.f_frsize * stvfsbuf.f_blocks;
  *free_space = stvfsbuf.f_frsize * stvfsbuf.f_bavail;
  return 0;
}

}  // namespace

IFileDataIterator* MakeFileDataIterator(const std::string& path) {
  return new FileDataIterator(path);
}

IChecksumGroupIterator* MakeChecksumGroupIterator(
    const std::string& path, InodeNum inode, GroupRole role) {
  return new ChecksumGroupIterator(path, inode, role);
}

IFileDataWriter* MakeFileDataWriter(const std::string& path) {
  return new FileDataWriter(path);
}

IChecksumGroupWriter* MakeChecksumGroupWriter(
    const std::string& path, InodeNum inode, GroupRole role) {
  return new ChecksumGroupWriter(path, inode, role);
}

IStore* MakeStore(std::string data_path) {
  return new Store(data_path);
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
