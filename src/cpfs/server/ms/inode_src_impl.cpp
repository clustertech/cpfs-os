/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IInodeSrc interface.
 */

#include "server/ms/inode_src_impl.hpp"

#include <unistd.h>

#include <sys/stat.h>

#include <cerrno>
#include <cstdio>
#include <ctime>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/atomic.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/random.hpp>
#include <boost/thread/tss.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "store_util.hpp"
#include "server/ms/inode_src.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Number of unused inodes to check before concluding a block is free.
 * Useful values start from 0.  Larger value means calls to
 * NotifyUsed() with non-consecutive inode numbers (usually caused by
 * concurrent calls) will less likely cause an unneeded persist call,
 * at the cost that initial loading will need to check more inodes for
 * confirmation.
 */
const int kUnusedFuzzy = 20;

/**
 * Maximum number of inodes not to persist in each entry.  Larger
 * value means calls to NotifyUsed() will cause a persist call less
 * frequently, at the cost that initial loading may need to check more
 * inodes before finding the free slot.
 */
const int kLazyFactor = 2000;

/**
 * Implement IInodeSrc.  This implementation stores symlinks as "c" in
 * the data path to persist the last inode used.  To reduce the number
 * of filesystem writes required for persisting the information, such
 * writes are lazy: When first accessed, we load the symlink to
 * determine the last persisted value, and look for the first block of
 * kUnusedFuzzy unused inodes from there.  Once found it is persisted
 * and cached.  This saves us from persisting future updated for
 * kLazyFactor updates, as long as (nearly) consecutive inodes are
 * used, and there is no removal of inodes.  When an inode is about to
 * be removed, the NotifyRemoved should be called, so that the entry
 * is persisted immediately if needed.
 */
class BaseInodeSrc {
 public:
  /**
   * @param data_path Where data is kept in the MS store.
   *
   * @param last The initial inode number used
   */
  explicit BaseInodeSrc(std::string data_path, InodeNum last)
      : data_path_(data_path), next_alloc_(0),
        data_mutex_(MUTEX_INIT),
        last_used_(last) {}

  /**
   * Initialize the source by reading the persisted file.  Must be
   * called before any other method is called.
   */
  void Init();

  /**
   * Setup the object for Allocate() call.  After the call, all
   * NotifyUsed() calls have arguments coming from Allocate() calls of
   * this object.
   */
  void SetupAllocation();

  /**
   * Allocate an inode number.  This should only be called after a
   * SetupAllocation() call.
   */
  InodeNum Allocate() {
    return next_alloc_.fetch_add(1, boost::memory_order_seq_cst);
  }

  /**
   * Notify that an inode is used.
   */
  void NotifyUsed(InodeNum inode);

  /**
   * Notify that the object representing an inode is removed.
   */
  void NotifyRemoved(InodeNum inode);

  /**
   * Get the last inode used.
   */
  InodeNum GetLastUsed();

  /**
   * Set the last inode used.
   */
  void SetLastUsed(InodeNum inode);

 private:
  std::string data_path_; /**< Data path keeping the MS store */
  boost::atomic<InodeNum> next_alloc_; /**< The next inode number */
  MUTEX_TYPE data_mutex_; /**< Protect the following */
  InodeNum last_used_; /**< Last used inodes */
  InodeNum last_persisted_; /**< Last inode_used_ count persisted */

  std::string GetRoot() {
    return std::string(data_path_);
  }

  /**
   * Get the filename used to persist unused counts.
   *
   * @return The filename.
   */
  std::string LastUsedPath_() {
    return GetRoot() + "/c";
  }

  /**
   * Persist the unused inode number.
   */
  void Persist_() {
    std::string path = LastUsedPath_();
    std::string new_name = path + "-new";
    std::string target = GetInodeStr(last_used_);
    unlink(new_name.c_str());  // Ignore error
    if (symlink(target.c_str(), new_name.c_str()) < 0
        || rename(new_name.c_str(), path.c_str()) < 0)
      throw std::runtime_error("Failed persisting inode usage: " + target);
    last_persisted_ = last_used_;
  }
};

void BaseInodeSrc::Init() {
  std::string path = LastUsedPath_();
  InodeNum inode_ret;
  int ret = SymlinkToInode(path, &inode_ret);
  if (ret == -ENOENT) {
    Persist_();
  } else if (ret) {
    throw std::runtime_error("Cannot read last inode link: " + path);
  } else {
    last_persisted_ = last_used_ = inode_ret;
    LOG(debug, Store, "Last inode number used: ", PHex(inode_ret));
  }
}

void BaseInodeSrc::SetupAllocation() {
  MUTEX_LOCK_GUARD(data_mutex_);
  struct stat buf;  // Dummy, but access() call doesn't work for symlinks
  for (InodeNum to_check = last_used_ + 1;
       to_check <= last_used_ + kUnusedFuzzy;
       ++to_check)
    if (lstat((GetRoot() + "/" + GetInodeStr(to_check)).c_str(), &buf) == 0)
      last_used_ = to_check;
  Persist_();
  next_alloc_.store(last_used_ + 1, boost::memory_order_seq_cst);
}

void BaseInodeSrc::NotifyUsed(InodeNum inode) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (inode <= last_used_)
    return;
  last_used_ = inode;
  if (inode >= last_persisted_ + kLazyFactor ||
      inode >= last_used_ + kUnusedFuzzy)
    Persist_();
}

void BaseInodeSrc::NotifyRemoved(InodeNum inode) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (inode > last_persisted_ && last_persisted_ != last_used_)
    Persist_();
}

InodeNum BaseInodeSrc::GetLastUsed() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return last_used_;
}

void BaseInodeSrc::SetLastUsed(InodeNum inode) {
  MUTEX_LOCK_GUARD(data_mutex_);
  last_used_ = inode;
  Persist_();
}

/**
 * Implement the InodeSrc usable for MS directory layout
 */
class InodeSrc : public IInodeSrc {
 public:
  /**
   * Construct the InodeSrc
   */
  explicit InodeSrc(std::string data_path) {
    for (unsigned d_idx = 0; d_idx < kNumBaseDir; ++d_idx) {
      char buf[4];
      std::snprintf(buf, sizeof(buf), "%03x", d_idx);
      InodeNum last = d_idx > 0 ? (InodeNum(d_idx) << 52) - 1 : 1;
      srcs_.push_back(
          new BaseInodeSrc(data_path + "/" + std::string(buf), last));
    }
  }

  void Init();
  void SetupAllocation();
  InodeNum Allocate(InodeNum inode, bool parent_hex);
  void NotifyUsed(InodeNum inode);
  void NotifyRemoved(InodeNum inode);
  std::vector<InodeNum> GetLastUsed();
  void SetLastUsed(const std::vector<InodeNum>& inodes);

 private:
  boost::ptr_vector<BaseInodeSrc> srcs_;

  /**
   * Per thread used random generator
   */
  struct RandomSlotGen {
    boost::random::mt19937 mt_; /**< Relatively fast random generator */
    boost::random::uniform_int_distribution<> dist_; /**< Even inode space */
    RandomSlotGen()
        : mt_(time(NULL)),  // Use time(0) since high entropy is not needed
          dist_(0, kNumBaseDir - 1) {}
    /**
     * Generate a random number
     */
    unsigned Generate() {
      return dist_(mt_);
    }
  };
  boost::thread_specific_ptr<RandomSlotGen> rand_;

  /**
   * Get the InodeSrc responsible for the given inode
   *
   * @param inode The inode number
   */
  inline BaseInodeSrc& get_(InodeNum inode) {
    return srcs_[inode >> 52];
  }
};

void InodeSrc::Init() {
  for (unsigned i = 0; i < kNumBaseDir; ++i)
    srcs_[i].Init();
}

void InodeSrc::SetupAllocation() {
  for (unsigned i = 0; i < kNumBaseDir; ++i)
    srcs_[i].SetupAllocation();
}

InodeNum InodeSrc::Allocate(InodeNum inode, bool parent_hex) {
  if (parent_hex) {
    return get_(inode).Allocate();
  } else {
    RandomSlotGen* rand_ptr = rand_.get();
    if (!rand_ptr) {
      rand_.reset(new RandomSlotGen());
      rand_ptr = rand_.get();
    }
    return srcs_[rand_ptr->Generate()].Allocate();
  }
}

void InodeSrc::NotifyUsed(InodeNum inode) {
  return get_(inode).NotifyUsed(inode);
}

void InodeSrc::NotifyRemoved(InodeNum inode) {
  return get_(inode).NotifyRemoved(inode);
}

std::vector<InodeNum> InodeSrc::GetLastUsed() {
  std::vector<InodeNum> ret;
  for (unsigned i = 0; i < kNumBaseDir; ++i)
    ret.push_back(srcs_[i].GetLastUsed());
  return ret;
}

void InodeSrc::SetLastUsed(const std::vector<InodeNum>& inodes) {
  for (unsigned i = 0; i < inodes.size(); ++i)
    srcs_[i].SetLastUsed(inodes[i]);
}

}  // namespace

IInodeSrc* MakeInodeSrc(std::string data_path) {
  return new InodeSrc(data_path);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
