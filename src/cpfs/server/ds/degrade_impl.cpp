/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IDegradedCache interface and IDataRecoveryMgr
 * interface.
 */

#include "server/ds/degrade_impl.hpp"

#include <cstddef>
#include <cstring>
#include <list>
#include <map>
#include <stdexcept>
#include <utility>

#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/weak_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "mutex_util.hpp"
#include "util.hpp"
#include "server/ds/degrade.hpp"

namespace cpfs {

class IFimSocket;

namespace server {
namespace ds {
namespace {

class DegradedCache;

/**
 * Implement the IDegradedCacheHandle interface.  See API doc of
 * DegradedCache for implementation note.
 */
class DegradedCacheHandle : public IDegradedCacheHandle {
 public:
  /**
   * @param mgr The manager creating this handle
   */
  explicit DegradedCacheHandle(DegradedCache* mgr) :
      mgr_(mgr), initialized_(false), segment_(0) {}

  ~DegradedCacheHandle();

  bool initialized() {
    return initialized_;
  }

  void Initialize() {
    initialized_ = true;
  }

  void Allocate();

  char* data() {
    return segment_;
  }

  void Read(std::size_t off, char* buf, std::size_t buf_size) {
    if (!segment_) {
      for (unsigned i = 0; i < buf_size; ++i)
        buf[i] = 0;
    } else {
      for (unsigned i = 0; i < buf_size; ++i)
        buf[i] = segment_[i + off];
    }
  }

  void Write(std::size_t off, const char* buf, std::size_t buf_size,
             char* checksum_change) {
    if (!segment_) {
      Allocate();
      std::memset(segment_, '\0', kSegmentSize);
    }
    std::memcpy(checksum_change, segment_ + off, buf_size);
    for (unsigned i = 0; i < buf_size; ++i)
      segment_[i + off] = buf[i];
    XorBytes(checksum_change, buf, buf_size);
  }

  void RevertWrite(std::size_t off, const char* checksum_change,
                   std::size_t buf_size) {
    XorBytes(segment_ + off, checksum_change, buf_size);
  }

  /**
   * The DegradedCache uses this to keep a handle so that repeated
   * calls to GetHandle() will return the same handle.
   */
  boost::weak_ptr<IDegradedCacheHandle> handle_in_use;

 private:
  DegradedCache* mgr_;
  bool initialized_;
  char* segment_;
};

/**
 * Type for the map storing created handles.
 */
typedef std::map<std::pair<InodeNum, std::size_t>,
                 boost::shared_ptr<DegradedCacheHandle> > HandleMap;

/**
 * A record of expirable data.
 */
struct EDEntry {
  InodeNum inode; /**< inode number of the entry */
  std::size_t dsg_off; /**< DSG offset of the entry */
};

/** Inode number member */
typedef boost::multi_index::member<EDEntry, InodeNum,
                                   &EDEntry::inode> InodeMember;

/** File name member */
typedef boost::multi_index::member<EDEntry, std::size_t,
                                   &EDEntry::dsg_off> OffsetMember;

/** Tag for insertion-order index */
struct ByInsertOrder {};

/**
 * The internal data structure holding entries in the cache manager.
 */
typedef boost::multi_index::multi_index_container<
  EDEntry,
  boost::multi_index::indexed_by<
    boost::multi_index::hashed_unique<
      boost::multi_index::composite_key<EDEntry, InodeMember, OffsetMember>
    >,
    boost::multi_index::sequenced<boost::multi_index::tag<ByInsertOrder> >
  >
> EDEntries;

/**
 * Insertion order index of CCEntries.
 */
typedef EDEntries::index<ByInsertOrder>::type EDEntryByInsertOrder;

/**
 * Implement the IDegradedCache interface.
 *
 * For the cache implementation, the base data structures is a map to
 * store existing handles, together with a large memory pool allocated
 * on activation and deleted on deactivation for storing the actual
 * data, and a free segment list.  An auxillary data structure keeps
 * an expirable entry list indexed by inode-offset but also listable
 * in LRU order.
 *
 * When GetHandle() is called and the handle already exists, the
 * handle is removed from the expirable entry list, and a shared_ptr
 * of the handle is returned.  The shared_ptr has a custom deletor to
 * add it back to the expirable entry list on destruction.  This
 * ensures that handles currently in use will never be expired.
 *
 * When segment memory is needed (on
 * IDegradedCacheHandle::Allocate()), the free list is tried.  If no
 * memory is left, it expires existing handles.  The handle list is
 * updated to remove the evicted handle, and on destructor of the
 * handle the segment memory is released so that segment memory
 * becomes available again for allocation.  Out-of-memory condition is
 * currently not handled as it is not expected: most handle uses
 * should be simply to copy some data into or out of the segment
 * memory and possibly calculating the checksum, and should be
 * released immediately afterwards.
 *
 * No reference counting is done within this class.  It is expected
 * that such shared_ptr of handles will be released before another
 * shared_ptr is obtained using GetHandle(), or Truncate() is
 * called on the same inode, etc.  This is the case because the same
 * inode is never handled by two different threads.
 */
class DegradedCache : public IDegradedCache {
 public:
  /**
   * @param num_cached_segments Number of segments to cache
   */
  explicit DegradedCache(unsigned num_cached_segments)
      : data_mutex_(MUTEX_INIT), num_cached_segments_(num_cached_segments) {}

  bool IsActive() {
    MUTEX_LOCK_GUARD(data_mutex_);
    return bool(mempool_);
  }

  void SetActive(bool active) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    if (active == bool(mempool_))
      return;
    if (active) {
      mempool_.reset(new char[num_cached_segments_ * kSegmentSize]);
      for (unsigned i = 0; i < num_cached_segments_; ++i)
        free_list_.push_back(mempool_.get() + i * kSegmentSize);
    } else {
      while (!handles_.empty()) {
        EDEntryByInsertOrder& ins_index = expirable_data_.get<ByInsertOrder>();
        if (ins_index.empty()) {
          MUTEX_WAIT(lock, handle_put_);
          continue;
        }
        EDEntry entry = ins_index.front();
        ins_index.pop_front();
        handles_.erase(std::make_pair(entry.inode, entry.dsg_off));
      }
      mempool_.reset();
      free_list_.clear();
    }
  }

  boost::shared_ptr<IDegradedCacheHandle> GetHandle(
      InodeNum inode, std::size_t dsg_off) {
    boost::shared_ptr<IDegradedCacheHandle> ret;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      if (!mempool_)
        return ret;
      std::pair<HandleMap::iterator, bool> ins_ret =
          handles_.insert(
              std::make_pair(std::make_pair(inode, dsg_off),
                             boost::shared_ptr<DegradedCacheHandle>()));
      boost::shared_ptr<DegradedCacheHandle>& handle = ins_ret.first->second;
      if (ins_ret.second) {  // handle is new
        handle.reset(new DegradedCacheHandle(this));
      } else {  // handle is old
        EDEntries::iterator it =
            expirable_data_.find(boost::make_tuple(inode, dsg_off));
        if (it != expirable_data_.end())  // defensive, should always be true
          expirable_data_.erase(it);
      }
      ret = handle->handle_in_use.lock();
      if (!ret) {
        ret = boost::shared_ptr<IDegradedCacheHandle>(
            handle.get(),
            boost::bind(&DegradedCache::MakeExpirable, this, inode, dsg_off));
        handle->handle_in_use = ret;
      }
    }
    return ret;
  }

  /**
   * Make a handle expirable.
   *
   * @param inode The inode number of the handle
   *
   * @param dsg_off The DSG offset of the handle
   */
  void MakeExpirable(InodeNum inode, std::size_t dsg_off) {
    MUTEX_LOCK_GUARD(data_mutex_);
    EDEntry entry;
    entry.inode = inode;
    entry.dsg_off = dsg_off;
    expirable_data_.insert(entry);
    handle_put_.notify_all();
  }

  void FreeInode(InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    FreeEntriesFrom_(inode, handles_.lower_bound(std::make_pair(inode, 0)));
  }

  void Truncate(InodeNum inode, std::size_t dsg_off) {
    MUTEX_LOCK_GUARD(data_mutex_);
    std::size_t last_segment_off = dsg_off / kSegmentSize * kSegmentSize;
    if (last_segment_off < dsg_off)
      last_segment_off += kSegmentSize;
    FreeEntriesFrom_(inode, handles_.lower_bound(
        std::make_pair(inode, last_segment_off)));
  }

  /**
   * Deallocate a segment.
   *
   * @param segment The segment to deallocate
   */
  void DeallocateSegment(char* segment) {
    // Only called by DegradedCacheHandle triggered because the
    // HandleMap entries are erased, data_mutex_ is already locked
    free_list_.push_back(segment);
  }

  /**
   * Allocate a segment.
   *
   * @return The allocated segment
   */
  char* AllocateSegment() {
    MUTEX_LOCK_GUARD(data_mutex_);
    while (free_list_.empty()) {
      EDEntryByInsertOrder& ins_index = expirable_data_.get<ByInsertOrder>();
      if (ins_index.empty())
        throw std::runtime_error("No segment can be allocated");
      EDEntry entry = ins_index.front();
      ins_index.pop_front();
      handles_.erase(std::make_pair(entry.inode, entry.dsg_off));
    }
    char* ret = free_list_.front();
    free_list_.pop_front();
    return ret;
  }

 private:
  MUTEX_TYPE data_mutex_; /**< Protect data structure below */
  boost::condition_variable handle_put_;  /**< Some handle has been put back */
  unsigned num_cached_segments_; /**< Number of segments to cache */
  boost::scoped_array<char> mempool_; /**< Segment memory pool */
  std::list<char*> free_list_; /**< Segment slots free for allocation */
  HandleMap handles_; /** Handles that have already been created */
  EDEntries expirable_data_; /**< Expirable data entries */

  void FreeEntriesFrom_(InodeNum inode, HandleMap::iterator start) {
    for (; start != handles_.end() && start->first.first == inode;
         handles_.erase(start++)) {
      EDEntries::iterator it =
          expirable_data_.find(boost::make_tuple(inode, start->first.second));
      if (it != expirable_data_.end())
        expirable_data_.erase(it);
    }
  }
};

DegradedCacheHandle::~DegradedCacheHandle() {
  if (segment_)
    mgr_->DeallocateSegment(segment_);
}

void DegradedCacheHandle::Allocate() {
  initialized_ = true;
  if (!segment_)
    segment_ = mgr_->AllocateSegment();
}

/**
 * Implement the IDataRecoveryHandle interface.
 */
class DataRecoveryHandle : public IDataRecoveryHandle {
  /**
   * Type used for storing Fim queue.
   */
  typedef std::list<std::pair<FIM_PTR<IFim>,
                              boost::shared_ptr<IFimSocket> > > QueueType;

 public:
  DataRecoveryHandle() : started_(false) {
    std::memset(buf_, '\0', kSegmentSize);
  }

  bool SetStarted() {
    bool ret = started_;
    started_ = true;
    return ret;
  }

  bool AddData(char* data, const boost::shared_ptr<IFimSocket>& peer) {
    if (ds_sent_.find(peer) == ds_sent_.end()) {
      ds_sent_.insert(peer);
      if (data)
        XorBytes(buf_, data, kSegmentSize);
    }
    return ds_sent_.size() >= kNumDSPerGroup - 2;  // One lost, one is self
  }

  bool RecoverData(char* checksum) {
    ds_sent_.clear();
    XorBytes(checksum, buf_, kSegmentSize);
    for (unsigned i = 0; i < kSegmentSize; ++i)
      if (checksum[i])
        return true;
    return false;
  }

  void QueueFim(const FIM_PTR<IFim>& fim,
                const boost::shared_ptr<IFimSocket>& peer) {
    queue_.push_back(std::make_pair(fim, peer));
  }

  bool DataSent(const boost::shared_ptr<IFimSocket>& peer) {
    return ds_sent_.find(peer) != ds_sent_.end();
  }

  FIM_PTR<IFim> UnqueueFim(boost::shared_ptr<IFimSocket>* peer_ret) {
    if (queue_.empty())
      return FIM_PTR<IFim>();
    *peer_ret = queue_.front().second;
    FIM_PTR<IFim> ret = queue_.front().first;
    queue_.pop_front();
    return ret;
  }

 private:
  bool started_;
  char buf_[kSegmentSize];
  boost::unordered_set<boost::shared_ptr<IFimSocket> > ds_sent_;
  QueueType queue_;
};

/**
 * Implement the IDataRecoveryMgr interface.
 */
class DataRecoveryMgr : public IDataRecoveryMgr {
  /**
   * Type for storing active handles
   */
  typedef boost::unordered_map<
    std::pair<InodeNum, std::size_t>,
    boost::shared_ptr<DataRecoveryHandle> > HandleMap;

 public:
  DataRecoveryMgr() : data_mutex_(MUTEX_INIT) {}
  boost::shared_ptr<IDataRecoveryHandle> GetHandle(
      InodeNum inode, std::size_t dsg_off, bool create) {
    MUTEX_LOCK_GUARD(data_mutex_);
    std::pair<InodeNum, std::size_t> key(inode, dsg_off);
    HandleMap::iterator it = handles_.find(key);
    if (it != handles_.end())
      return it->second;
    if (!create)
      return boost::shared_ptr<IDataRecoveryHandle>();
    return handles_[key] = boost::make_shared<DataRecoveryHandle>();
  }

  void DropHandle(InodeNum inode, std::size_t dsg_off) {
    MUTEX_LOCK_GUARD(data_mutex_);
    handles_.erase(std::make_pair(inode, dsg_off));
  }

 private:
  MUTEX_TYPE data_mutex_; /**< Protect data structure below */
  HandleMap handles_; /** Currently active handles */
};

}  // namespace

IDegradedCache* MakeDegradedCache(unsigned num_cached_segments) {
  return new DegradedCache(num_cached_segments);
}

IDataRecoveryMgr* MakeDataRecoveryMgr() {
  return new DataRecoveryMgr;
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
