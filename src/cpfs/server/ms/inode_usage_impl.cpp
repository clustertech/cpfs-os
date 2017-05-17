/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of InodeUsage for tracking inode operations in MS
 * and MS.
 */

#include "server/ms/inode_usage_impl.hpp"

#include <utility>

#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "server/ms/inode_usage.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Represent an entry that keeps information about the usage of an
 * inode by a client.
 */
struct IUEntry {
  /**
   * @param inode The inode
   *
   * @param client The client
   *
   * @param access The access requested
   */
  IUEntry(InodeNum inode = 0, ClientNum client = 0,
          InodeAccess access = kInodeReadAccess)
      : inode(inode), client(client), access(access) {}
  InodeNum inode; /**< The inode */
  ClientNum client; /**< The client */
  InodeAccess access; /**< Access requested */
};

/** Inode number member */
typedef boost::multi_index::member<IUEntry, InodeNum,
                                   &IUEntry::inode> InodeMember;

/** Client number member */
typedef boost::multi_index::member<IUEntry, ClientNum,
                                   &IUEntry::client> ClientMember;

/** Access member */
typedef boost::multi_index::member<IUEntry, InodeAccess,
                                   &IUEntry::access> AccessMember;



/** Tag for inode number */
struct ByInode {};

/** Tag for inode number + writer */
struct ByInodeAccess {};

/** Tag for inode number */
struct ByClient {};

/**
 * The internal data structure holding entries in the inode usage.
 */
typedef boost::multi_index::multi_index_container<
  IUEntry,
  boost::multi_index::indexed_by<
    boost::multi_index::hashed_unique<
      boost::multi_index::composite_key<IUEntry, InodeMember, ClientMember>
    >,
    boost::multi_index::ordered_non_unique<boost::multi_index::tag<ByInode>,
                                           InodeMember>,
    boost::multi_index::ordered_non_unique<
      boost::multi_index::tag<ByInodeAccess>,
      boost::multi_index::composite_key<IUEntry, InodeMember, AccessMember>
    >,
    boost::multi_index::ordered_non_unique<boost::multi_index::tag<ByClient>,
                                           ClientMember>
  >
> IUEntries;

/**
 * Inode index of IUEntries.
 */
typedef IUEntries::index<ByInode>::type IUEntryByInode;

/**
 * Inode writer index of IUEntries.
 */
typedef IUEntries::index<ByInodeAccess>::type IUEntryByInodeAccess;

/**
 * Client index of IUEntries.
 */
typedef IUEntries::index<ByClient>::type IUEntryByClient;

/**
 * Actual implementation of MS inode usage.
 */
class InodeUsage : public IInodeUsage {
 public:
  InodeUsage() : data_mutex_(MUTEX_INIT) {}

  bool IsOpened(InodeNum inode, bool write_only) const {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (write_only) {
      return IsOpenedForWrite_(inode);
    } else {
      const IUEntryByInode& index = iu_entries_.get<ByInode>();
      return index.find(inode) != index.end();
    }
  }

  bool IsSoleWriter(ClientNum client, InodeNum inode) const {
    MUTEX_LOCK_GUARD(data_mutex_);
    const IUEntryByInodeAccess& index = iu_entries_.get<ByInodeAccess>();
    std::pair<IUEntryByInodeAccess::iterator, IUEntryByInodeAccess::iterator>
        range = index.equal_range(boost::make_tuple(inode, kInodeWriteAccess));
    for (IUEntryByInodeAccess::iterator it = range.first;
         it != range.second;
         ++it) {
      if (it->client != client)
        return false;
    }
    return true;
  }

  void SetFCOpened(ClientNum client_num, InodeNum inode, InodeAccess access) {
    MUTEX_LOCK_GUARD(data_mutex_);
    IUEntry entry(inode, client_num, access);
    IUEntries::iterator it = iu_entries_.end();
    // replace() path needed only if access == kInodeWriteAccess,
    // otherwise we just rely on the behavior of multi-index set of
    // ignoring insert's when the element is existing
    if (access == kInodeWriteAccess)
      it = iu_entries_.find(boost::make_tuple(inode, client_num));
    if (it == iu_entries_.end()) {
      iu_entries_.insert(entry);
    } else {
      iu_entries_.replace(it, entry);
    }
  }

  bool SetFCClosed(ClientNum client_num, InodeNum inode, bool keep_read) {
    MUTEX_LOCK_GUARD(data_mutex_);
    IUEntries::iterator it = iu_entries_.find(
        boost::make_tuple(inode, client_num));
    if (it == iu_entries_.end())
      return false;
    bool is_write = it->access == kInodeWriteAccess;
    if (keep_read) {
      IUEntry entry(inode, client_num, kInodeReadAccess);
      iu_entries_.replace(it, entry);
    } else {
      iu_entries_.erase(it);
    }
    if (!is_write)
      return false;
    return !IsOpenedForWrite_(inode);
  }

  InodeSet GetFCOpened(ClientNum client_num) const {
    InodeSet ret;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      const IUEntryByClient& client_index = iu_entries_.get<ByClient>();
      std::pair<IUEntryByClient::const_iterator,
                IUEntryByClient::const_iterator> range =
          client_index.equal_range(client_num);
      for (IUEntryByClient::const_iterator it = range.first;
           it != range.second;
           ++it)
        ret.insert(it->inode);
    }
    return ret;
  }

  void SwapClientOpened(ClientNum client_num, InodeAccessMap* access_map) {
    MUTEX_LOCK_GUARD(data_mutex_);
    InodeAccessMap ret;
    IUEntryByClient& client_index = iu_entries_.get<ByClient>();
    std::pair<IUEntryByClient::iterator, IUEntryByClient::iterator> range =
        client_index.equal_range(client_num);
    for (IUEntryByClient::iterator it = range.first; it != range.second;) {
      ret[it->inode] = it->access;
      client_index.erase(it++);
    }
    for (InodeAccessMap::iterator it = access_map->begin();
         it != access_map->end();
         ++it)
      iu_entries_.insert(IUEntry(it->first, client_num, it->second));
    *access_map = ret;
  }

  ClientInodeMap client_opened() const {
    ClientInodeMap ret;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      for (IUEntries::iterator it = iu_entries_.begin();
           it != iu_entries_.end();
           ++it)
        ret[it->client][it->inode] = it->access;
    }
    return ret;
  }

  void AddPendingUnlink(InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    LOG(debug, FS, "Pending unlink ", PHex(inode));
    ds_defer_unlink_.insert(inode);
  }

  void AddPendingUnlinks(const InodeSet& inodes) {
    MUTEX_LOCK_GUARD(data_mutex_);
    LOG(debug, FS, "Pending unlink set, size ", PVal(inodes.size()));
    ds_defer_unlink_.insert(inodes.begin(), inodes.end());
  }

  void RemovePendingUnlink(InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    LOG(debug, FS, "Clear pending unlink ", PHex(inode));
    ds_defer_unlink_.erase(inode);
  }

  bool IsPendingUnlink(InodeNum inode) const {
    MUTEX_LOCK_GUARD(data_mutex_);
    return ds_defer_unlink_.find(inode) != ds_defer_unlink_.end();
  }

  void ClearPendingUnlink() {
    MUTEX_LOCK_GUARD(data_mutex_);
    ds_defer_unlink_.clear();
  }

  InodeSet pending_unlink() const {
    MUTEX_LOCK_GUARD(data_mutex_);
    return ds_defer_unlink_;
  }

 private:
  mutable MUTEX_TYPE data_mutex_; /**< Protect fields below */
  IUEntries iu_entries_; /**< Inode / client user entries */
  InodeSet ds_defer_unlink_; /**< Inode not yet freed because it is opened */

  /**
   * Check whether an inode is opened for write by some client.
   *
   * @param inode The inode
   *
   * @return Whether it is opened for write
   */
  bool IsOpenedForWrite_(InodeNum inode) const {
    const IUEntryByInodeAccess& index = iu_entries_.get<ByInodeAccess>();
    return index.find(boost::make_tuple(inode, kInodeWriteAccess)) !=
        index.end();
  }
};

}  // namespace

IInodeUsage* MakeInodeUsage() {
  return new InodeUsage;
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
