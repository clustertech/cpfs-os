/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of InodeUsageSet for tracking inode operations.
 */

#include "client/inode_usage_impl.hpp"

#include <stdexcept>

#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "client/inode_usage.hpp"

namespace cpfs {
namespace client {
namespace {

/**
 * Represent the inode usage information for one inode.
 */
struct InodeUsage {
 public:
  InodeUsage() : count(0), w_count(0), ls_count(0), clean(true) {}

  boost::condition no_ls_cond; /**< ls_count goes to 0 */
  int count; /**< The use count of the inode */
  int w_count; /**< The write count of the inode */
  int ls_count; /**< Number of locking setattr */
  bool clean; /**< Whether any release advised the file is modified */
};

/**
 * Allow deferring unlocking of the inode usage mutex.  If a lock is
 * held, the mutex is unlocked when this object destructs.
 */
class KeepLockGuard : public IInodeUsageGuard {
 public:
  /**
   * @param lock The lock to transfer to the guard
   */
  explicit KeepLockGuard(boost::unique_lock<MUTEX_TYPE>* lock) {
    lock_.swap(*lock);
  }
 private:
  boost::unique_lock<MUTEX_TYPE> lock_;
};

class BaseInodeUsageSet;

/**
 * Decrement the locked setattr count of an inode on release.
 */
class StopLSGuard : public IInodeUsageGuard {
 public:
  /**
   * @param usage_set The usage set
   *
   * @param inode The inode
   */
  StopLSGuard(BaseInodeUsageSet* usage_set, InodeNum inode)
      : usage_set_(usage_set), inode_(inode) {}
  ~StopLSGuard();
 private:
  BaseInodeUsageSet* usage_set_;
  InodeNum inode_;
};

/**
 * Base version of InodeUsageSet.
 */
class BaseInodeUsageSet : public IInodeUsageSet {
 public:
  BaseInodeUsageSet() : data_mutex_(MUTEX_INIT) {}

  void UpdateOpened(InodeNum inode, bool is_write,
                    boost::scoped_ptr<IInodeUsageGuard>* guard) {
    LOG(debug, FS, "Inode usage: Opened ", PHex(inode));
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    InodeUsage& elt = recs_[inode];
    ++elt.count;
    if (is_write)
      ++elt.w_count;
    if (guard)
      guard->reset(new KeepLockGuard(&lock));
  }

  bool StartLockedSetattr(
      InodeNum inode, boost::scoped_ptr<IInodeUsageGuard>* guard) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    InodeUsage& elt = recs_[inode];
    if (!elt.clean)
      return false;
    LOG(debug, FS, "Inode usage: Started locked setattr ", PHex(inode));
    ++elt.ls_count;
    if (guard)
      guard->reset(new StopLSGuard(this, inode));
    return true;
  }

  void StopLockedSetattr(InodeNum inode) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    InodeUsage& elt = recs_[inode];
    if (--elt.ls_count == 0) {
      elt.no_ls_cond.notify_all();
      if (elt.count == 0)
        recs_.erase(inode);
    }
  }

  void SetDirty(InodeNum inode) {
    LOG(debug, FS, "Inode usage: Dirty ", PHex(inode));
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    InodeUsage& elt = recs_[inode];
    for (;;) {
      if (!elt.clean)
        return;
      if (!elt.ls_count)
        break;
      elt.no_ls_cond.wait(lock);
    }
    elt.clean = false;
  }

  int UpdateClosed(InodeNum inode, bool is_write, bool* clean_ret,
                   boost::scoped_ptr<IInodeUsageGuard>* guard) {
    LOG(debug, FS, "Inode usage: Closed ", PHex(inode));
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    int ret = kClientAccessUnchanged;
    InodeUsage& elt = recs_[inode];
    if (is_write) {
      *clean_ret = elt.clean;
      if (--elt.w_count == 0) {
        ret = 1;
        elt.clean = true;
      } else if (elt.w_count < 0) {
        throw std::runtime_error("Error in InodeUsage maintenance (w)");
      }
    } else {
      *clean_ret = true;
    }
    if (--elt.count == 0) {
      ret = 0;
      if (elt.ls_count == 0)
        recs_.erase(inode);
    } else if (elt.count < 0) {
      throw std::runtime_error("Error in InodeUsage maintenance");
    }
    if (guard)
      guard->reset(new KeepLockGuard(&lock));
    return ret;
  }

 private:
  MUTEX_TYPE data_mutex_; /**< Protect fields below */
  typedef boost::unordered_map<InodeNum, InodeUsage> InodeUsageRecs;
  InodeUsageRecs recs_; /**< The actual records */
};

StopLSGuard::~StopLSGuard() {
  usage_set_->StopLockedSetattr(inode_);
}

/**
 * Actual implementation of inode usage set.
 */
class InodeUsageSet : public IInodeUsageSet {
 public:
  /**
   * @param num_classes The number of inode classes
   */
  explicit InodeUsageSet(unsigned num_classes)
      : num_classes_(num_classes),
        usage_sets_(new BaseInodeUsageSet[num_classes]) {}

  void UpdateOpened(InodeNum inode, bool is_write,
                    boost::scoped_ptr<IInodeUsageGuard>* guard) {
    GetBase(inode).UpdateOpened(inode, is_write, guard);
  }

  bool StartLockedSetattr(
      InodeNum inode, boost::scoped_ptr<IInodeUsageGuard>* guard) {
    return GetBase(inode).StartLockedSetattr(inode, guard);
  }

  void StopLockedSetattr(InodeNum inode) {
    GetBase(inode).StopLockedSetattr(inode);
  }

  void SetDirty(InodeNum inode) {
    GetBase(inode).SetDirty(inode);
  }

  int UpdateClosed(InodeNum inode, bool is_write,
                   bool* clean_ret,
                   boost::scoped_ptr<IInodeUsageGuard>* guard) {
    return GetBase(inode).UpdateClosed(inode, is_write, clean_ret, guard);
  }

 private:
  unsigned num_classes_;
  boost::scoped_array<BaseInodeUsageSet> usage_sets_;

  BaseInodeUsageSet& GetBase(InodeNum inode) {
    return usage_sets_[inode % num_classes_];
  }
};

}  // namespace

IInodeUsageSet* MakeInodeUsageSet(unsigned num_classes) {
  return new InodeUsageSet(num_classes);
}

}  // namespace client
}  // namespace cpfs
