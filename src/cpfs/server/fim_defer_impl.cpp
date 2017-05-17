/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IFimDeferMgr for trackering deferred Fims, and
 * IInodeFimDeferMgr for tracking deferred Fims per inode.
 */

#include "server/fim_defer_impl.hpp"

#include <stdint.h>

#include <list>
#include <stdexcept>
#include <utility>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "server/fim_defer.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimSocket;

namespace server {
namespace {

/**
 * Implement IFimDeferMgr
 */
class FimDeferMgr : public IFimDeferMgr {
 public:
  FimDeferMgr() {}

  void AddFimEntry(const FIM_PTR<IFim>& fim,
                   const boost::shared_ptr<IFimSocket>& socket) {
    data_.push_back(DeferredFimEntry(fim, socket));
  }

  bool HasFimEntry() const {
    return !data_.empty();
  }

  DeferredFimEntry GetNextFimEntry() {
    if (data_.empty())
      throw std::runtime_error("No FimEntry in FimDeferMgr");
    DeferredFimEntry entry = data_.front();
    data_.pop_front();
    return entry;
  }

 private:
  std::list<DeferredFimEntry> data_;
};

/**
 * Implementation of LockedInodeMgr
 */
class InodeFimDeferMgr : public IInodeFimDeferMgr {
 public:
  bool IsLocked(InodeNum inode) const {
    return defer_mgrs_.find(inode) != defer_mgrs_.end();
  }

  void SetLocked(InodeNum inode, bool isLocked) {
    if (isLocked)
      defer_mgrs_[inode];
    else
      defer_mgrs_.erase(inode);
  }

  std::vector<InodeNum> LockedInodes() const {
    std::vector<InodeNum> ret;
    BOOST_FOREACH(MapType::value_type val_pair, defer_mgrs_) {
      ret.push_back(val_pair.first);
    }
    return ret;
  }

  void AddFimEntry(InodeNum inode, const FIM_PTR<IFim>& fim,
                   const boost::shared_ptr<IFimSocket>& socket) {
    defer_mgrs_[inode].AddFimEntry(fim, socket);
  }

  bool HasFimEntry(InodeNum inode) const {
    MapType::const_iterator it = defer_mgrs_.find(inode);
    if (it == defer_mgrs_.end())
      return false;
    return it->second.HasFimEntry();
  }

  DeferredFimEntry GetNextFimEntry(InodeNum inode) {
    MapType::iterator it = defer_mgrs_.find(inode);
    if (it == defer_mgrs_.end())
      throw std::runtime_error("No FimEntry in InodeFimDeferMgr");
    return it->second.GetNextFimEntry();
  }

 private:
  /** Type of the internal data structure used */
  typedef boost::unordered_map<InodeNum, FimDeferMgr> MapType;
  MapType defer_mgrs_; /**< Nested mgrs */
};

/**
 * Implement the ISegmentFimDeferMgr interface.
 */
class SegmentFimDeferMgr : public ISegmentFimDeferMgr {
 public:
  bool IsLocked(InodeNum inode, uint64_t segment) const {
    return defer_mgrs_.find(std::make_pair(inode, segment)) !=
        defer_mgrs_.end();
  }

  void SetLocked(InodeNum inode, uint64_t segment, bool isLocked) {
    if (isLocked) {
      defer_mgrs_[std::make_pair(inode, segment)];
    } else {
      defer_mgrs_.erase(std::make_pair(inode, segment));
    }
  }

  void AddFimEntry(InodeNum inode, uint64_t segment, const FIM_PTR<IFim>& fim,
                   const boost::shared_ptr<IFimSocket>& socket) {
    defer_mgrs_[std::make_pair(inode, segment)].AddFimEntry(fim, socket);
  }

  bool SegmentHasFimEntry(InodeNum inode, uint64_t segment) const {
    MapType::const_iterator it =
        defer_mgrs_.find(std::make_pair(inode, segment));
    if (it == defer_mgrs_.end())
      return false;
    return it->second.HasFimEntry();
  }

  DeferredFimEntry GetSegmentNextFimEntry(InodeNum inode, uint64_t segment) {
    MapType::iterator it = defer_mgrs_.find(std::make_pair(inode, segment));
    if (it == defer_mgrs_.end())
      throw std::runtime_error("No FimEntry in SegmentFimDeferMgr");
    return it->second.GetNextFimEntry();
  }

  bool ClearNextFimEntry(DeferredFimEntry* entry_ret) {
    for (;;) {
      MapType::iterator it = defer_mgrs_.begin();
      if (it == defer_mgrs_.end())
        return false;
      if (it->second.HasFimEntry()) {
        *entry_ret = it->second.GetNextFimEntry();
        return true;
      }
      defer_mgrs_.erase(it);
    }
    return false;
  }

 private:
  /** Type of the key of the internal map type */
  typedef std::pair<InodeNum, uint64_t> SegKey;
  /** The internal map type */
  typedef boost::unordered_map<SegKey, FimDeferMgr> MapType;
  MapType defer_mgrs_; /**< Nested mgrs */
};

}  // namespace

IFimDeferMgr* MakeFimDeferMgr() {
  return new FimDeferMgr;
}

IInodeFimDeferMgr* MakeInodeFimDeferMgr() {
  return new InodeFimDeferMgr;
}

ISegmentFimDeferMgr* MakeSegmentFimDeferMgr() {
  return new SegmentFimDeferMgr;
}

}  // namespace server
}  // namespace cpfs
