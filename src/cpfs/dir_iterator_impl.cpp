/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IDirIterator interface for iterating entries
 * in a directory.
 */

#include "dir_iterator_impl.hpp"

#include <dirent.h>
#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/format.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "store_util.hpp"

namespace cpfs {

namespace {

/**
 * Check whether a directory entry should be returned, and set the
 * results if so.
 *
 * @param name The name of the entry
 *
 * @param path The path of the entry
 *
 * @param ctime The ctime bound.  If ctime != 0, and if ctime of the
 * stat result is less than this the entry is not returned
 *
 * @param is_dir_ret Where to return whether the entry is a directory
 *
 * @param stbuf_ret Where to return the stat buffer for interested
 * callers.  If NULL, use a local stat buffer
 *
 * @return Whether the entry should be returned
 */
bool CheckPath(const std::string& name, const std::string path, uint64_t ctime,
               bool* is_dir_ret, struct stat* stbuf_ret) {
  struct stat my_stbuf;
  if (!stbuf_ret)
    stbuf_ret = &my_stbuf;
  std::string file_path = path + "/" + name;
  if (lstat(file_path.c_str(), stbuf_ret) < 0) {
    if (errno == ENOENT)  // File lost before stat
      return false;  // Can't cover: race condition
    throw std::runtime_error(
        (boost::format("Cannot stat file %s: %s")
         % file_path % strerror(errno)).str());
  }
  // Filter by ctime
  if (ctime && uint64_t(stbuf_ret->st_ctime) < ctime)
    return false;
  *is_dir_ret = S_ISDIR(stbuf_ret->st_mode);
  return true;
}

/**
 * Implement the IDirIterator interface
 */
class DirIterator : public IDirIterator {
 public:
  /**
   * @param path Full path to the directory
   */
  explicit DirIterator(const std::string& path)
      : path_(path), ctime_(0), entry_(0) {
    if ((dir_ = opendir(path_.c_str()))) {
      name_max_ = pathconf(path_.c_str(), _PC_NAME_MAX);
      name_max_ = name_max_ == -1 ? NAME_MAX : name_max_;
      // +8 instead of +1 to avoid Valgrind problems due to odd allocation size
      entrya_.reset(new char[offsetof(dirent, d_name) + name_max_ + 8]);
      entry_ = reinterpret_cast<dirent*>(&entrya_[0]);
    }
  }

  ~DirIterator() {
    if (dir_)
      closedir(dir_);
  }

  bool GetNext(std::string* name, bool* is_dir_ret, struct stat* stbuf_ret) {
    if (!dir_)
      return false;
    dirent* result;
    for (;;) {
      if (readdir_r(dir_, entry_, &result) != 0 || result == NULL)
        return false;
      if (std::strcmp(entry_->d_name, ".") == 0 ||
          std::strcmp(entry_->d_name, "..") == 0)
        continue;
      *name = std::string(entry_->d_name);
      // Skip stat if possible
      if (!ctime_ && entry_->d_type != DT_UNKNOWN && !stbuf_ret) {
        *is_dir_ret = entry_->d_type == DT_DIR;
        return true;
      }
      if (CheckPath(*name, path_, ctime_, is_dir_ret, stbuf_ret))
        return true;
    }
    return false;
  }

  void SetFilterCTime(uint64_t ctim) {
    ctime_ = ctim;
  }

  bool missing() const {
    return dir_ == NULL;
  }

 protected:
  std::string path_;  /**< Full path to directory */
  uint64_t ctime_;  /**< The ctime lower bound */

 private:
  DIR* dir_;
  dirent* entry_;
  int name_max_;
  boost::scoped_array<char> entrya_;
};

/**
 * Implement the LeafViewDirIterator
 */
class LeafViewDirIterator : public DirIterator {
 public:
  /**
   * @param path Full path to the directory
   */
  explicit LeafViewDirIterator(const std::string& path)
      : DirIterator(path) {}

  bool GetNext(std::string* name, bool* is_dir_ret, struct stat* stbuf_ret) {
    while (true) {
      if (dir_iter_ && dir_iter_->GetNext(name, is_dir_ret, stbuf_ret)) {
        *name = b_name_ + "/" + *name;
        break;
      }
      // Traverse next directory or file
      if (!DirIterator::GetNext(&b_name_, is_dir_ret, stbuf_ret))
        return false;
      if (*is_dir_ret && b_name_.length() == 3) {
        dir_iter_.reset(new DirIterator(path_ + "/" + b_name_));
        dir_iter_->SetFilterCTime(ctime_);
        continue;
      } else {
        *name = b_name_;
        break;
      }
    }
    return true;
  }

 private:
  std::string b_name_;
  boost::scoped_ptr<IDirIterator> dir_iter_;
};

/**
 * Implement the RangedDirIterator
 */
class InodeRangeDirIterator : public IDirIterator {
 public:
  /**
   * @param path Full path to the directory
   *
   * @param ranges The Inode ranges requested
   *
   * @param append A string to append to the end of the inode number
   * as filename
   */
  InodeRangeDirIterator(const std::string& path,
                        const std::vector<InodeNum>& ranges,
                        const std::string& append)
      : path_(path), ranges_(ranges), append_(append),
        ctime_(0), last_inode_(0) {
    std::reverse(ranges_.begin(), ranges_.end());
    struct stat stbuf;
    missing_ = lstat(path.c_str(), &stbuf) < 0;
  }

  bool GetNext(std::string* name, bool* is_dir_ret, struct stat* stbuf_ret) {
    InodeNum range_mask = (1ULL << kDurableRangeOrder) - 1;
    for (;;) {
      if (last_inode_ != 0 && (last_inode_ & range_mask) != range_mask) {
        ++last_inode_;
      } else if (ranges_.empty()) {
        return false;
      } else {
        last_inode_ = ranges_.back();
        ranges_.pop_back();
        if (last_inode_ == 0)
          ++last_inode_;
      }
      std::string inode_s = GetInodeStr(last_inode_);
      *name = inode_s.substr(0, 3) + "/" + inode_s + append_;
      if (CheckPath(*name, path_, ctime_, is_dir_ret, stbuf_ret))
        return true;
    }
  }

  void SetFilterCTime(uint64_t ctim) {
    ctime_ = ctim;
  }

  bool missing() const {
    return missing_;
  }

 private:
  std::string path_;  /**< The path */
  std::vector<InodeNum> ranges_;  /**< The ranges to check */
  std::string append_;  /**< What to append to InodeNum to form filename */
  uint64_t ctime_;  /**< The ctime lower bound */
  bool missing_;  /**< Whether the directory is found */
  InodeNum last_inode_;  /**< Next inode to check */
};

}  // namespace

IDirIterator* MakeDirIterator(const std::string& path) {
  return new DirIterator(path);
}

IDirIterator* MakeLeafViewDirIterator(const std::string& path) {
  return new LeafViewDirIterator(path);
}

IDirIterator* MakeInodeRangeDirIterator(const std::string& path,
                                        const std::vector<InodeNum>& ranges,
                                        const std::string& append) {
  return new InodeRangeDirIterator(path, ranges, append);
}

}  // namespace cpfs
