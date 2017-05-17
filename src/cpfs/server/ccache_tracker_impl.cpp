/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the ICCacheTracker interface.
 */

#include "server/ccache_tracker_impl.hpp"

#include <sys/time.h>

#include <cstddef>
#include <ctime>
#include <utility>
#include <vector>

#include <boost/function.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/tuple/tuple.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "util.hpp"
#include "server/ccache_tracker.hpp"

namespace cpfs {
namespace server {
namespace {

/**
 * Client cache tracker entry.
 */
struct CCEntry {
  InodeNum inode;         /**< The inode number */
  ClientNum client;       /**< The client number */
  struct timespec last_access;  /**< Last access time */
};

/** Tag for inode-based index */
struct ByInode {};

/** Inode member key type */
typedef boost::multi_index::member<CCEntry, InodeNum,
                                   &CCEntry::inode> InodeMember;

/** Tag for client-based index */
struct ByClient {};

/** Client member key type */
typedef boost::multi_index::member<CCEntry, ClientNum,
                                   &CCEntry::client> ClientMember;

/** Tag for insertion-order index */
struct ByInsertOrder {};

/**
 * Type for the cache monitor.
 */
typedef boost::multi_index::multi_index_container<
  CCEntry,
  boost::multi_index::indexed_by<
    boost::multi_index::hashed_unique<
      boost::multi_index::composite_key<CCEntry, InodeMember, ClientMember>
    >,
    boost::multi_index::ordered_non_unique<boost::multi_index::tag<ByInode>,
                                           InodeMember>,
    boost::multi_index::ordered_non_unique<boost::multi_index::tag<ByClient>,
                                           ClientMember>,
    boost::multi_index::sequenced<boost::multi_index::tag<ByInsertOrder> >
  >
> CCEntries;

/**
 * Inode index of CCEntries.
 */
typedef CCEntries::index<ByInode>::type CCEntryByInode;

/**
 * Client index of CCEntries.
 */
typedef CCEntries::index<ByClient>::type CCEntryByClient;

/**
 * Insertion order index of CCEntries.
 */
typedef CCEntries::index<ByInsertOrder>::type CCEntryByInsertOrder;

/**
 * Implementation of ICCacheTracker
 */
class CCacheTracker : public ICCacheTracker {
 public:
  CCacheTracker() : data_mutex_(MUTEX_INIT) {}
  std::size_t Size() const;
  void SetCache(InodeNum inode_num, ClientNum client_num);
  void RemoveClient(ClientNum client_num);
  void InvGetClients(InodeNum inode_num, ClientNum except,
                     CCacheExpiryFunc expiry_func);
  void ExpireCache(int min_age, CCacheExpiryFunc expiry_func);
 private:
  mutable MUTEX_TYPE data_mutex_;
  CCEntries entries_;
};

std::size_t CCacheTracker::Size() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return entries_.size();
}

void CCacheTracker::SetCache(InodeNum inode_num, ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  CCEntries::iterator it = entries_.find(
      boost::make_tuple(inode_num, client_num));
  if (it != entries_.end()) {
    CCEntry entry = *it;
    entries_.erase(it);
    clock_gettime(CLOCK_MONOTONIC, &entry.last_access);
    entries_.insert(entry);
    return;
  }
  // New entry
  CCEntry entry;
  entry.inode = inode_num;
  entry.client = client_num;
  clock_gettime(CLOCK_MONOTONIC, &entry.last_access);
  entries_.insert(entry);
}

void CCacheTracker::RemoveClient(ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  CCEntryByClient& client_index = entries_.get<ByClient>();
  std::pair<CCEntryByClient::iterator, CCEntryByClient::iterator>
      pair = client_index.equal_range(client_num);
  CCEntryByClient::iterator start = pair.first, end = pair.second;
  while (start != end)
    client_index.erase(start++);
}

/**
 * Run the expiry_func for some entries
 */
class CCacheExpiryFuncRunner {
 public:
  /**
   * @param expiry_func The expiry_func to run.  If an empty function,
   * Add() and Run() are no-ops.
   */
  explicit CCacheExpiryFuncRunner(CCacheExpiryFunc expiry_func)
      : expiry_func_(expiry_func) {}

  /**
   * Add an entry to have the expiry_func run.
   *
   * @param entry The entry to add
   */
  void Add(const CCEntry& entry) {
    if (expiry_func_)
      inv_list_.push_back(std::make_pair(entry.inode, entry.client));
  }

  /**
   * Run expiry_func for all entries added.
   */
  void Run() {
    for (std::size_t i = 0; i < inv_list_.size(); ++i)
      expiry_func_(inv_list_[i].first, inv_list_[i].second);
    inv_list_.clear();
  }

 private:
  CCacheExpiryFunc expiry_func_;
  std::vector<std::pair<InodeNum, ClientNum> > inv_list_;
};

void CCacheTracker::InvGetClients(InodeNum inode_num, ClientNum except,
                                  CCacheExpiryFunc expiry_func) {
  CCacheExpiryFuncRunner expiry_func_runner(expiry_func);
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    CCEntryByInode& inode_index = entries_.get<ByInode>();
    std::pair<CCEntryByInode::iterator, CCEntryByInode::iterator>
        pair = inode_index.equal_range(inode_num);
    CCEntryByInode::iterator start = pair.first, end = pair.second;
    while (start != end) {
      CCEntryByInode::iterator it = start;
      ++start;
      if (it->client == except)
        continue;
      expiry_func_runner.Add(*it);
      inode_index.erase(it);
    }
  }
  expiry_func_runner.Run();
}

void CCacheTracker::ExpireCache(int min_age, CCacheExpiryFunc expiry_func) {
  LOG(debug, Cache, "Expiring cache tracker entries");
  int cnt = 0;
  CCacheExpiryFuncRunner expiry_func_runner(expiry_func);
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    CCEntryByInsertOrder& ins_index = entries_.get<ByInsertOrder>();
    CompareTime ct;
    struct timespec max_last_use;
    clock_gettime(CLOCK_MONOTONIC, &max_last_use);
    max_last_use.tv_sec -= min_age;
    for (CCEntryByInsertOrder::iterator it = ins_index.begin();
         it != ins_index.end(); ++cnt) {
      if (!ct(it->last_access, max_last_use))
        break;
      expiry_func_runner.Add(*it);
      ins_index.erase(it++);
    }
  }
  expiry_func_runner.Run();
  LOG(debug, Cache, "Expired ", PINT(cnt), " cache tracker entries");
}

}  // namespace

ICCacheTracker* MakeICacheTracker() {
  return new CCacheTracker();
}

}  // namespace server
}  // namespace cpfs
