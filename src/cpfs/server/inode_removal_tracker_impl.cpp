/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IInodeRemovalTracker interface.
 */

#include "server/inode_removal_tracker_impl.hpp"

#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

#include "common.hpp"
#include "mutex_util.hpp"
#include "timed_list.hpp"
#include "server/inode_removal_tracker.hpp"

namespace cpfs {
namespace server {
namespace {

/**
 * Implement IInodeRemovalTracker.
 */
class InodeRemovalTracker : public IInodeRemovalTracker {
 public:
  /**
   * @param data_path Path to data directory
   */
  explicit InodeRemovalTracker(const std::string& data_path)
      : data_path_(data_path), removed_rec_path_(data_path + "/r/removed"),
        data_mutex_(MUTEX_INIT), removed_rec_file_(0) {}

  ~InodeRemovalTracker() {
    if (removed_rec_file_)
      fclose(removed_rec_file_);
  }

  void RecordRemoved(InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (removed_rec_file_)
      WriteRecFile_(&inode, 1);
    else
      removed_rec_mem_.Add(inode);
  }

  void SetPersistRemoved(bool enabled) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (enabled) {
      if (mkdir((data_path_ + "/r").c_str(), 0755) != 0 && errno != EEXIST)
        throw std::runtime_error("Cannot create resync record directory");
      removed_rec_file_ = fopen(removed_rec_path_.c_str(), "ab");
      if (!removed_rec_file_)
        throw std::runtime_error("Cannot write resync record file");
      TimedList<InodeNum>::Elems inodes = removed_rec_mem_.FetchAndClear();
      WriteRecFile_(inodes.data(), inodes.size());
    } else {
      if (removed_rec_file_) {
        fclose(removed_rec_file_);
        removed_rec_file_ = 0;
      }
      unlink(removed_rec_path_.c_str());
    }
  }

  std::vector<InodeNum> GetRemovedInodes() {
    std::vector<InodeNum> removed_inodes;
    {  // Prevent coverage false positive
      MUTEX_LOCK_GUARD(data_mutex_);
      FILE* fp = fopen(removed_rec_path_.c_str(), "rb");
      if (!fp)
        return removed_inodes;
      std::fseek(fp, 0, SEEK_END);
      int64_t tell_ret = std::max(int64_t(std::ftell(fp)), int64_t(0));
      InodeNum num_inodes = tell_ret / sizeof(InodeNum);
      removed_inodes.resize(num_inodes);
      std::fseek(fp, 0, SEEK_SET);
      removed_inodes.resize(std::fread(removed_inodes.data(), sizeof(InodeNum),
                                       num_inodes, fp));
      fclose(fp);
    }
    return removed_inodes;
  }

  void ExpireRemoved(int age) {
    MUTEX_LOCK_GUARD(data_mutex_);
    removed_rec_mem_.Expire(age);
  }

 private:
  const std::string data_path_;
  const std::string removed_rec_path_;
  mutable MUTEX_TYPE data_mutex_;  /**< Protect fields below */
  FILE* removed_rec_file_;  /**< File keeping removed inodes on disk */
  TimedList<InodeNum> removed_rec_mem_;  /**< Keeping removed in memory */

  // Write removed inode to disk
  void WriteRecFile_(InodeNum* inodes, int count) {
    std::fwrite(inodes, count, sizeof(inodes[0]), removed_rec_file_);
    std::fflush(removed_rec_file_);
  }
};

}  // namespace

IInodeRemovalTracker* MakeInodeRemovalTracker(const std::string& data_path) {
  return new InodeRemovalTracker(data_path);
}

}  // namespace server
}  // namespace cpfs
