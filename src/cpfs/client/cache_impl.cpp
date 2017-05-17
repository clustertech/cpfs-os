/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of interface for client-side cache management.
 */

#include "client/cache_impl.hpp"

#include <sys/time.h>

#include <ctime>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/thread.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "util.hpp"
#include "client/cache.hpp"

struct fuse_chan;

namespace cpfs {
namespace client {

namespace {

/**
 * Actual implementation of the cache invalidation record.  It
 * contains a unordered set of invalidated inodes, as well as the next
 * invalidation record.  Invalidations are only marked in the latest
 * invalidation record, so to implement InodeInvalidated(), one goes
 * through the CacheInvRecord one by one to see whether any of
 * them contains the specified inode.
 *
 * Note that all method are called without first locking the mutex.
 * The client code must thus create a lock before calling functions
 * here.
 */
class CacheInvRecord : public ICacheInvRecord {
 public:
  /**
   */
  CacheInvRecord() : all_invalid_(false) {}

  /**
   * Set the mutex to use to prevent race condition.
   *
   * @param mutex The mutex to use
   */
  void SetMutex(MUTEX_TYPE* mutex) {
    mutex_ = mutex;
  }

  /**
   * Set the next invalidation record.
   *
   * @param next The next invalidation record.
   */
  void SetNextRecord(const boost::shared_ptr<CacheInvRecord>& next) {
    next_ = next;
  }

  MUTEX_TYPE* GetMutex() {
    return mutex_;
  }

  bool InodeInvalidated(InodeNum inode, bool page_only) const {
    if (all_invalid_)
      return true;
    InvalidatedState::const_iterator it = invalidated_state_.find(inode);
    if (it != invalidated_state_.end())
      if (!page_only || it->second)
        return true;
    if (!next_)
      return false;
    return next_->InodeInvalidated(inode, page_only);
  }

  /**
   * Add an inode to be treated as invalidated.
   *
   * @param inode The inode
   *
   * @param clear_pages Whether pages are requested to be cleared
   */
  void AddInvalidatedInode(InodeNum inode, bool clear_pages) {
    invalidated_state_[inode] = invalidated_state_[inode] || clear_pages;
  }

  /**
   * Invalidate all called.
   */
  void AllInvalid() {
    all_invalid_ = true;
  }

  /**
   * @return Whether the record has been used, i.e., any inode is
   * added to it.  If this is false, the record may be reused.
   */
  bool Used() {
    return !invalidated_state_.empty();
  }

 private:
  MUTEX_TYPE* mutex_; /**< Mutex to use */
  boost::shared_ptr<CacheInvRecord> next_; /**< Next record */
  bool all_invalid_; /**< Whether InvalidateAll() has been called */
  /** Type for invalidated_state_ */
  typedef boost::unordered_map<InodeNum, bool> InvalidatedState;
  /**
   * Invalidated inodes state: true => page invalidated, false => only
   * inode invalidated
   */
  InvalidatedState invalidated_state_;
};

/**
 * Represent an entry that keeps information about the cache entry.
 */
struct CMEntry {
  InodeNum inode; /**< The inode of the entry */
  std::string name; /**< The filename of the entry, of empty for inode attrs */
  struct timespec time_added; /**< Time when the entries is added */
};

/** Inode number member */
typedef boost::multi_index::member<CMEntry, InodeNum,
                                   &CMEntry::inode> InodeMember;

/** Tag for inode number */
struct ByInode {};

/** File name member */
typedef boost::multi_index::member<CMEntry, std::string,
                                   &CMEntry::name> NameMember;

/** Tag for insertion-order index */
struct ByInsertOrder {};

/**
 * The internal data structure holding entries in the cache manager.
 */
typedef boost::multi_index::multi_index_container<
  CMEntry,
  boost::multi_index::indexed_by<
    boost::multi_index::hashed_unique<
      boost::multi_index::composite_key<CMEntry, InodeMember, NameMember>
    >,
    boost::multi_index::ordered_non_unique<boost::multi_index::tag<ByInode>,
                                           InodeMember>,
    boost::multi_index::sequenced<boost::multi_index::tag<ByInsertOrder> >
  >
> CMEntries;

/**
 * Inode index of CMEntries.
 */
typedef CMEntries::index<ByInode>::type CMEntryByInode;

/**
 * Insertion order index of CCEntries.
 */
typedef CMEntries::index<ByInsertOrder>::type CMEntryByInsertOrder;

/**
 * Actual implementation of the client cache manager.
 *
 * @tparam TFuseMethodPolicy The method policy class to use.
 */
class CacheMgr : public ICacheMgr {
 public:
  /**
   * @param chan The FUSE channel to use
   *
   * @param inv_policy The cache invalidation policy object to use
   */
  explicit CacheMgr(fuse_chan* chan, ICacheInvPolicy* inv_policy)
      : chan_(chan), data_mutex_(MUTEX_INIT), cache_exiting_(false),
        inv_policy_(inv_policy), to_inval_all_(false) {}

  void SetFuseChannel(fuse_chan* chan) {
    chan_ = chan;
  }

  void Init() {
    cache_thread_.reset(
        new boost::thread(boost::bind(&CacheMgr::Work, this)));
  }

  void Shutdown() {
    UNIQUE_PTR<boost::thread> cache_thread;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      if (!cache_thread_.get())
        return;
      cache_thread = MOVE(cache_thread_);
      cache_exiting_ = true;
      cache_wake_cond_.notify_all();
    }
    cache_thread->join();
    cache_thread.reset();
  }

  boost::shared_ptr<ICacheInvRecord> StartLookup() {
    MUTEX_LOCK_GUARD(data_mutex_);
    ReleaseIRMaybe_();
    if (!curr_ir_ || curr_ir_->Used()) {
      boost::shared_ptr<CacheInvRecord> ret
          = boost::make_shared<CacheInvRecord>();
      ret->SetMutex(&data_mutex_);
      if (curr_ir_)
        curr_ir_->SetNextRecord(ret);
      curr_ir_ = ret;
    }
    return curr_ir_;
  }

  void AddEntry(InodeNum inode, const std::string& name, bool skip_lock) {
    if (skip_lock) {
      AddEntry_(inode, name);
    } else {
      MUTEX_LOCK_GUARD(data_mutex_);
      AddEntry_(inode, name);
    }
  }

  void RegisterInode(InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    AddEntry_(inode, "");
  }

  void InvalidateInode(InodeNum inode, bool clear_pages) {
    MUTEX_LOCK_GUARD(data_mutex_);
    ReleaseIRMaybe_();
    if (clear_page_.find(inode) != clear_page_.end()) {
      clear_page_[inode] = clear_page_[inode] || clear_pages;
      return;
    }
    inode_inval_.push_back(inode);
    clear_page_[inode] = clear_pages;
    if (curr_ir_)
      curr_ir_->AddInvalidatedInode(inode, clear_pages);
    cache_wake_cond_.notify_all();
  }

  void InvalidateAll() {
    MUTEX_LOCK_GUARD(data_mutex_);
    ReleaseIRMaybe_();
    to_inval_all_ = true;
    if (curr_ir_)
      curr_ir_->AllInvalid();
    cache_wake_cond_.notify_all();
  }

  void CleanMgr(int min_age) {
    MUTEX_LOCK_GUARD(data_mutex_);
    LOG(informational, Server, "Cleaning up client cache");
    CMEntryByInsertOrder& ins_index = cm_entries_.get<ByInsertOrder>();
    CompareTime ct;
    struct timespec max_added;
    clock_gettime(CLOCK_MONOTONIC, &max_added);
    max_added.tv_sec -= min_age;
    for (CMEntryByInsertOrder::iterator it = ins_index.begin();
         it != ins_index.end(); ) {
      if (!ct(it->time_added, max_added))
        break;
      ins_index.erase(it++);
    }
  }

 private:
  // Constructor args
  fuse_chan* chan_; /**< FUSE channel to use */

  // Threading
  UNIQUE_PTR<boost::thread> cache_thread_;
  boost::condition_variable cache_wake_cond_; /**< Wake up main loop */
  MUTEX_TYPE data_mutex_; /**< Protect everything below */
  bool cache_exiting_; /**< Thread exit requested */

  // Inode invalidation
  /** Cache invalidation policy */
  boost::scoped_ptr<ICacheInvPolicy> inv_policy_;
  boost::shared_ptr<CacheInvRecord> curr_ir_; /**< latest record */
  std::vector<InodeNum> inode_inval_; /**< Pages awaiting invalidation */
  bool to_inval_all_; /**< InvalidateAll() requested */
  boost::unordered_map<InodeNum, bool> clear_page_; /**< inode => pages */
  CMEntries cm_entries_; /**< inode and directory entries */

  /**
   * The action to be done by the thread.  Currently, it just performs
   * invalidations.
   */
  void Work() {
    for (;;) {
      std::vector<std::pair<InodeNum, int> > inodes_to_inval;
      std::vector<std::pair<InodeNum, std::string> > entries_to_inval;
      if (!GetInvInodesEntries(&inodes_to_inval, &entries_to_inval))
        return;
      for (std::vector<std::pair<InodeNum, int> >::iterator it =
               inodes_to_inval.begin();
           it != inodes_to_inval.end();
           ++it) {
        LOG(debug, Cache, "Invalidating ", PHex(it->first));
        inv_policy_->NotifyInvalInode(chan_, it->first, it->second, 0);
      }
      for (std::vector<std::pair<InodeNum, std::string> >::iterator it =
               entries_to_inval.begin();
           it != entries_to_inval.end();
           ++it) {
        LOG(debug, Cache, "Invalidating entry ",
            PHex(it->first), " ", it->second);
        inv_policy_->NotifyInvalEntry(chan_, it->first,
                                      it->second.data(), it->second.size());
      }
    }
  }

  /**
   * Get and clear entries to be invalidated.
   *
   * @param inodes_to_inval Where to put inodes to be invalidated
   *
   * @param entries_to_inval Where to put entries to be invalidated
   *
   * @return Whether to continue the cache
   */
  bool GetInvInodesEntries(
      std::vector<std::pair<InodeNum, int> >* inodes_to_inval,
      std::vector<std::pair<InodeNum, std::string> >* entries_to_inval) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    if (cache_exiting_)
      return false;
    if (to_inval_all_) {
      to_inval_all_ = false;
      GetInvAllInodesEntries_(inodes_to_inval, entries_to_inval);
      return true;
    }
    if (inode_inval_.size() == 0)
      MUTEX_WAIT(lock, cache_wake_cond_);
    for (unsigned int i = 0; i < inode_inval_.size(); ++i) {
      InodeNum inode = inode_inval_[i];
      bool inval_pages = clear_page_[inode];
      inodes_to_inval->push_back(
          std::make_pair(inode, inval_pages ? 0 : -1));
      if (inval_pages) {
        CMEntryByInode& inode_index = cm_entries_.get<ByInode>();
        std::pair<CMEntryByInode::iterator, CMEntryByInode::iterator>
            range = inode_index.equal_range(inode);
        CMEntryByInode::iterator start = range.first, end = range.second;
        while (start != end) {
          if (start->name.size() != 0)  // Inode-only entry
            entries_to_inval->push_back(std::make_pair(inode, start->name));
          inode_index.erase(start++);
        }
      }
    }
    inode_inval_.clear();
    clear_page_.clear();
    return true;
  }

  void GetInvAllInodesEntries_(
      std::vector<std::pair<InodeNum, int> >* inodes_to_inval,
      std::vector<std::pair<InodeNum, std::string> >* entries_to_inval) {
    inode_inval_.clear();
    clear_page_.clear();
    InodeNum last_inode = 0;
    CMEntryByInode& inode_index = cm_entries_.get<ByInode>();
    for (CMEntryByInode::iterator curr = inode_index.begin();
         curr != inode_index.end();
         ++curr) {
      if (last_inode != curr->inode) {
        last_inode = curr->inode;
        inodes_to_inval->push_back(std::make_pair(last_inode, 0));
      }
      if (curr->name.size() == 0)
        continue;
      entries_to_inval->push_back(std::make_pair(curr->inode, curr->name));
    }
    cm_entries_.clear();
  }

  /**
   * Add entry.
   */
  void AddEntry_(InodeNum inode, const std::string& name) {
    CMEntries::iterator it = cm_entries_.find(boost::make_tuple(inode, name));
    CMEntry entry;
    if (it != cm_entries_.end()) {
      entry = *it;
      cm_entries_.erase(it);
    } else {
      // New entry
      entry.inode = inode;
      entry.name = name;
    }
    clock_gettime(CLOCK_MONOTONIC, &entry.time_added);
    cm_entries_.insert(entry);
  }

  /**
   * Release the invalidation record if possible.
   */
  void ReleaseIRMaybe_() {
    if (curr_ir_.use_count() == 1)
      curr_ir_.reset();
  }
};

}  // namespace

ICacheMgr* MakeCacheMgr(fuse_chan* chan, ICacheInvPolicy* inv_policy) {
  return new CacheMgr(chan, inv_policy);
}

}  // namespace client
}  // namespace cpfs
