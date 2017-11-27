/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IDirtyInodeMgr interface.
 */

#include "server/ms/dirty_inode_impl.hpp"

#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "dir_iterator_impl.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "store_util.hpp"
#include "tracker_mapper.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/dirty_inode.hpp"
#include "server/ms/store.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implement the IDirtyInodeMgr interface.
 */
class DirtyInodeMgr : public IDirtyInodeMgr {
 public:
  /**
   * @param data_path Where data is stored in the corresponding metadir
   */
  explicit DirtyInodeMgr(std::string data_path);
  void Reset(bool load_persisted);
  DirtyInodeMap GetList();
  uint64_t version() const {
    MUTEX_LOCK_GUARD(data_mutex_);
    return version_;
  }
  bool IsVolatile(InodeNum inode, uint64_t* version_ret) const;
  bool SetVolatile(InodeNum inode, bool active, bool clean, uint64_t* gen_ret);
  void NotifyAttrSet(InodeNum inode);
  bool StartCleaning(InodeNum inode);
  void ClearCleaning();
  bool Clean(InodeNum inode, uint64_t* gen,
             boost::unique_lock<MUTEX_TYPE>* lock);

 private:
  mutable MUTEX_TYPE data_mutex_; /**< Protect fields below */
  std::string persist_path_; /**< Where list is persisted */
  DirtyInodeMap dirty_inode_map_; /**< In-memory copy */
  uint64_t version_; /**< Current version number */

  /**
   * Get the path for an inode.
   *
   * @param name The stringified inode
   *
   * @retur The path to the file for it
   */
  std::string GetPath(const std::string& name) {
    return persist_path_ + "/" + name;
  }

  /**
   * Call a function for each inode kept in the persistent storage.
   *
   * @param func The function to call
   */
  void ForEachPersisted(boost::function<void(const std::string&, InodeNum)>
                        func) {
    boost::scoped_ptr<IDirIterator> dir_iterator(
        MakeDirIterator(persist_path_));
    if (dir_iterator->missing()) {
      mkdir(persist_path_.c_str(), 0700);
    } else {
      std::string name;
      bool is_dir;
      while (dir_iterator->GetNext(&name, &is_dir, 0)) {
        InodeNum inode;
        if (ParseInodeNum(name.c_str(), name.size(), &inode))
          func(name, inode);
      }
    }
  }

  /**
   * Remove a file.
   *
   * @param name The name of the file to remove
   */
  void RemoveFile_(std::string name) {
    unlink(GetPath(name).c_str());
  }

  /**
   * Set an inode as dirty but inactive.
   *
   * @param inode The inode
   */
  void SetVolatileInactive_(InodeNum inode) {
    dirty_inode_map_[inode].clean = false;
  }
};

DirtyInodeMgr::DirtyInodeMgr(std::string data_path)
    : data_mutex_(MUTEX_INIT), persist_path_(data_path + "/d"), version_(0) {}

void DirtyInodeMgr::Reset(bool load_persisted) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ++version_;
  if (load_persisted) {
    ForEachPersisted(boost::bind(&DirtyInodeMgr::SetVolatileInactive_,
                                 this, _2));
  } else {
    dirty_inode_map_.clear();
    ForEachPersisted(boost::bind(&DirtyInodeMgr::RemoveFile_, this, _1));
  }
}

DirtyInodeMap DirtyInodeMgr::GetList() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return dirty_inode_map_;
}

bool DirtyInodeMgr::IsVolatile(InodeNum inode, uint64_t* version_ret) const {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (version_ret)
    *version_ret = version_;
  return dirty_inode_map_.find(inode) != dirty_inode_map_.end();
}

bool DirtyInodeMgr::SetVolatile(InodeNum inode, bool active, bool clean,
                                uint64_t* gen_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (dirty_inode_map_.find(inode) == dirty_inode_map_.end())
    symlink(".", GetPath(GetInodeStr(inode)).c_str());
  DirtyInodeInfo& dirty_info = dirty_inode_map_[inode];
  dirty_info.active = active;
  if (active)
    ++dirty_info.gen;
  if (!clean)
    dirty_info.clean = false;
  if (gen_ret)
    *gen_ret = dirty_info.gen;
  return dirty_info.clean;
}

void DirtyInodeMgr::NotifyAttrSet(InodeNum inode) {
  MUTEX_LOCK_GUARD(data_mutex_);
  DirtyInodeMap::iterator it = dirty_inode_map_.find(inode);
  if (it == dirty_inode_map_.end())
    return;
  ++it->second.gen;
}

bool DirtyInodeMgr::StartCleaning(InodeNum inode) {
  MUTEX_LOCK_GUARD(data_mutex_);
  DirtyInodeMap::iterator it = dirty_inode_map_.find(inode);
  if (it == dirty_inode_map_.end() || it->second.active || it->second.cleaning)
    return false;
  it->second.cleaning = true;
  return true;
}

void DirtyInodeMgr::ClearCleaning() {
  MUTEX_LOCK_GUARD(data_mutex_);
  BOOST_FOREACH(DirtyInodeMap::value_type& item,  // NOLINT(runtime/references)
                dirty_inode_map_)
    item.second.cleaning = false;
}

bool DirtyInodeMgr::Clean(InodeNum inode, uint64_t* gen,
                          boost::unique_lock<MUTEX_TYPE>* lock) {
  MUTEX_LOCK(boost::unique_lock, data_mutex_, my_lock);
  DirtyInodeMap::iterator it = dirty_inode_map_.find(inode);
  if (it != dirty_inode_map_.end()) {
    it->second.cleaning = false;
    // In standby it->second.gen is in general <= *gen, usually by a
    // large margin, because NotifyAttrSet() is only called in the
    // active MS.  But in standby, we want Clean to always succeed, so
    // this is okay.
    if (it->second.active || it->second.gen > *gen) {
      *gen = it->second.active ? 0 : it->second.gen;
      return false;
    }
    dirty_inode_map_.erase(inode);
    RemoveFile_(GetInodeStr(inode));
  }
  ++version_;
  if (lock)
    lock->swap(my_lock);
  return true;
}

/**
 * Hold a record of pending attribute update
 */
struct AttrUpdateRec {
  AttrUpdateRec() : data_mutex(MUTEX_INIT) {}
  mutable MUTEX_TYPE data_mutex; /**< Protect fields below */
  FSTime mtime; /**< mtime so far */
  uint64_t size; /**< size so far */
  int num_req_pending; /**< Number of requests to DS not completed */
  AttrUpdateHandler complete_handler; /**< Handlers for this update */
};

/**
 * Implement the IAttrUpdater interface.
 */
class AttrUpdater : public IAttrUpdater {
 public:
  /**
   * @param server The meta server owning the updater
   */
  explicit AttrUpdater(BaseMetaServer* server) : server_(server) {}
  bool AsyncUpdateAttr(InodeNum inode, FSTime mtime, uint64_t size,
                       AttrUpdateHandler complete_handler);

 private:
  BaseMetaServer* server_; /**< The meta server owning the updater */

  /** To be called by request tracker when DS replies */
  void ReqCompleted(boost::shared_ptr<AttrUpdateRec> update_rec,
                    InodeNum inode, const boost::shared_ptr<IReqEntry>&);
};

bool AttrUpdater::AsyncUpdateAttr(InodeNum inode, FSTime mtime, uint64_t size,
                                  AttrUpdateHandler complete_handler) {
  boost::shared_ptr<AttrUpdateRec> update_rec =
      boost::make_shared<AttrUpdateRec>();
  MUTEX_LOCK_GUARD(update_rec->data_mutex);
  update_rec->mtime = mtime;
  update_rec->size = size;
  update_rec->num_req_pending = 0;
  update_rec->complete_handler = complete_handler;
  std::vector<GroupId> group_ids;
  if (server_->store()->GetFileGroupIds(inode, &group_ids) < 0)
    throw std::runtime_error("Cannot find groups to update inode attrs");
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  for (unsigned i = 0; i < group_ids.size(); ++i) {
    GroupId g = group_ids[i];
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      boost::shared_ptr<IReqTracker> tracker =
          tracker_mapper->GetDSTracker(g, r);
      if (!tracker)
        continue;
      FIM_PTR<AttrUpdateFim> fim = AttrUpdateFim::MakePtr();
      (*fim)->inode = inode;
      boost::shared_ptr<IReqEntry> entry = MakeTransientReqEntry(tracker, fim);
      boost::unique_lock<MUTEX_TYPE> req_lock;
      if (!tracker->AddRequestEntry(entry, &req_lock)) {
        LOG(debug, Store,
            "When updating attrs for inode ", PHex(inode), ", skipping ",
            PVal(tracker->name()), " as it is not connected");
        continue;
      }
      ++update_rec->num_req_pending;
      entry->OnAck(boost::bind(&AttrUpdater::ReqCompleted, this,
                               update_rec, inode, _1));
    }
  }
  return true;
}

void AttrUpdater::ReqCompleted(
    boost::shared_ptr<AttrUpdateRec> update_rec,
    InodeNum inode, const boost::shared_ptr<IReqEntry>& entry) {
  {
    MUTEX_LOCK_GUARD(update_rec->data_mutex);
    --update_rec->num_req_pending;
    FIM_PTR<IFim> reply = entry->reply();
    if (!reply || reply->type() != kAttrUpdateReplyFim) {
      LOG(informational, Store, "Missing reply for attr update of ",
          PHex(inode), " ignoring");
    } else {
      AttrUpdateReplyFim& rreply = static_cast<AttrUpdateReplyFim&>(*reply);
      if (update_rec->mtime < rreply->mtime)
        update_rec->mtime = rreply->mtime;
      update_rec->size = std::max(update_rec->size, rreply->size);
    }
  }  // Release all locks before calling handlers
  if (update_rec->num_req_pending == 0)
    update_rec->complete_handler(update_rec->mtime, update_rec->size);
}

}  // namespace

IDirtyInodeMgr* MakeDirtyInodeMgr(std::string data_path) {
  return new DirtyInodeMgr(data_path);
}

IAttrUpdater* MakeAttrUpdater(BaseMetaServer* meta_server) {
  return new AttrUpdater(meta_server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
