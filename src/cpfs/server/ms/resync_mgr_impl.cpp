/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the cpfs::IResyncMgr interface for
 * resynchronization.
 */

#include "server/ms/resync_mgr_impl.hpp"

#include <stdint.h>

#include <sys/stat.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/scope_exit.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "config_mgr.hpp"
#include "dir_iterator.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_tracker.hpp"
#include "store_util.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/dirty_inode.hpp"
#include "server/ms/inode_src.hpp"
#include "server/ms/inode_usage.hpp"
#include "server/ms/meta_dir_reader.hpp"
#include "server/ms/resync_mgr.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/store.hpp"
#include "server/ms/topology.hpp"
#include "server/server_info.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

const unsigned kMaxInodesInFim = 16384; /**< Max number of inodes in tailbuf */

/**
 * Implement interface for ResyncMgr.
 */
class ResyncMgr : public IResyncMgr {
 public:
  /**
   * @param server The meta server
   *
   * @param max_send Number of resync fim to send before reply is received
   */
  explicit ResyncMgr(BaseMetaServer* server, unsigned max_send)
      : server_(server), max_send_(max_send), data_mutex_(MUTEX_INIT) {}

  void RequestResync() {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (thread_) {
      LOG(error, Server, "Cannot request resync when another is in progress");
      return;
    }
    thread_.reset(
        new boost::thread(boost::bind(&ResyncMgr::DoRequestResync, this)));
  }

  void SendAllResync(bool optimized, const std::vector<InodeNum>& removed) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (thread_) {
      LOG(error, Server, "Cannot start resync when another is in progress");
      return;
    }
    thread_.reset(
        new boost::thread(boost::bind(&ResyncMgr::DoSendAllResync, this,
                                      optimized, removed)));
  }

 private:
  BaseMetaServer* server_;
  const unsigned max_send_;
  std::deque<boost::shared_ptr<IReqEntry> > requests_;
  mutable MUTEX_TYPE data_mutex_; /**< Protect fields below */
  /**< Running thread, to avoid blocking Asio */
  boost::scoped_ptr<boost::thread> thread_;

  void DoRequestResync() {
    BOOST_SCOPE_EXIT(&data_mutex_, &thread_) {
      MUTEX_LOCK_GUARD(data_mutex_);
      thread_.reset();
    } BOOST_SCOPE_EXIT_END;
    LOG(notice, Server, "Requesting MS Resync");
    server_->ha_counter()->PersistCount(0);  // "Don't use me if possible"
    uint64_t last_seen = server_->peer_time_keeper()->GetLastUpdate();
    boost::shared_ptr<IFimSocket> ms_socket =
        server_->tracker_mapper()->GetMSFimSocket();
    FIM_PTR<MSResyncReqFim> req_fim = MSResyncReqFim::MakePtr();
    std::memset(req_fim->body(), 0, req_fim->body_size());
    (*req_fim)->first = 1;
    if (last_seen <= kReplAndIODelay) {
      (*req_fim)->last = 'F';  // Request full resync
    } else {
      server_->store()->RemoveInodeSince(
          server_->durable_range()->Get(),
          last_seen - kReplAndIODelay);
      std::vector<InodeNum> removed =
          server_->inode_removal_tracker()->GetRemovedInodes();
      LOG(notice, Server, "Removing obsolete inodes: ",
          PVal(removed.size()), " inodes removed");
      if (removed.size()) {
        std::size_t curr = 0;
        for (;;) {
          std::size_t cnt = std::min(std::size_t(kMaxInodesInFim),
                                     removed.size() - curr);
          std::size_t buf_size = cnt * sizeof(InodeNum);
          req_fim->tail_buf_resize(buf_size);
          std::memcpy(req_fim->tail_buf(), removed.data() + curr, buf_size);
          curr += cnt;
          if (curr >= removed.size())  // No more removed inodes to send
            break;
          ms_socket->WriteMsg(req_fim);
          req_fim = MSResyncReqFim::MakePtr();
          std::memset(req_fim->body(), 0, req_fim->body_size());
        }
      }
      (*req_fim)->last = 'O';
    }
    ms_socket->WriteMsg(req_fim);
  }

  void DoSendAllResync(bool optimized, const std::vector<InodeNum>& removed) {
    BOOST_SCOPE_EXIT(&data_mutex_, &thread_) {
      MUTEX_LOCK_GUARD(data_mutex_);
      thread_.reset();
    } BOOST_SCOPE_EXIT_END;
    LOG(notice, Server, "MS Resync started");
    boost::shared_ptr<IReqTracker> ms_tracker
        = server_->tracker_mapper()->GetMSTracker();
    try {
      server_->topology_mgr()->SendAllFCInfo();
      server_->topology_mgr()->SendAllDSInfo(ms_tracker->GetFimSocket());
      ResyncRemoval();
      // Clear inode usage in peer for resync new content
      std::vector<InodeNum> i_used = server_->inode_src()->GetLastUsed();
      FIM_PTR<ResyncInodeUsagePrepFim> prep_fim =
          ResyncInodeUsagePrepFim::MakePtr(i_used.size() * sizeof(InodeNum));
      std::memcpy(prep_fim->tail_buf(), i_used.data(),
                  prep_fim->tail_buf_size());
      SendRequest(prep_fim);
      ResyncExtendedAttrs();
      ResyncDirtyInodes();
      ResyncPendingUnlink();
      ResyncOpenedInodes();
      ResyncInodes(optimized, removed);
      // Notify completion
      ms_tracker->AddTransientRequest(MSResyncEndFim::MakePtr())->WaitReply();
      // Drop removal records since peer MS finished processing
      server_->inode_removal_tracker()->SetPersistRemoved(false);
      server_->durable_range()->SetConservative(false);
      // Start time keeper only when resync completed, so that if resync is
      // failed in the middle, the time is still correct upon retry
      server_->peer_time_keeper()->Start();
    } catch (const std::exception& ex) {
      LOG(error, Server, "MS Resync error: ", ex.what());
      boost::shared_ptr<IFimSocket> ms_socket = ms_tracker->GetFimSocket();
      if (ms_socket) {
        LOG(notice, Server, "Shutting down MS connection");
        ms_socket->Shutdown();
      }
    }
    server_->state_mgr()->SwitchState(kStateActive);
    // Unfreeze MS
    server_->topology_mgr()->StartStopWorker();
    LOG(notice, Server, "MS Resync completed");
  }

  void ResyncRemoval() {
    std::vector<InodeNum> removed_inodes
        = server_->inode_removal_tracker()->GetRemovedInodes();
    for (std::vector<InodeNum>::const_iterator it = removed_inodes.begin();
         it != removed_inodes.end(); ++it) {
        FIM_PTR<ResyncRemovalFim> fim = ResyncRemovalFim::MakePtr();
        (*fim)->inode = *it;
        SendRequest(fim);
    }
  }

  void ResyncInodes(bool optimized, const std::vector<InodeNum>& peer_removed) {
    uint64_t from_t = 0;
    if (optimized) {
      uint64_t backward_t = kReplAndIODelay +
          uint64_t(server_->configs().heartbeat_interval()) * 2;
      uint64_t last_update = server_->peer_time_keeper()->GetLastUpdate();
      if (last_update > backward_t && last_update <= uint64_t(std::time(0)))
        from_t = last_update - backward_t;  // Use optimized resync
    }
    // Resync inodes with ctime newer than from_t
    boost::scoped_ptr<IDirIterator> inode_iter;
    if (from_t) {
      inode_iter.reset(server_->store()->InodeList(
          server_->durable_range()->Get()));
      inode_iter->SetFilterCTime(from_t);
    } else {
      inode_iter.reset(server_->store()->List(""));
    }
    std::string prefixed_inode_s;
    bool inode_is_dir;
    boost::unordered_set<InodeNum> resynced;
    while (inode_iter->GetNext(&prefixed_inode_s, &inode_is_dir)) {
      InodeNum inode;
      // Not really prefixed
      if (prefixed_inode_s.size() <= kBaseDirNameLen + 1 ||
          prefixed_inode_s[kBaseDirNameLen] != '/')
        continue;
      std::string inode_name = StripPrefixInodeStr(prefixed_inode_s);
      // Skip maintenance directories, hardlink and control files
      if (inode_name.size() <= 8 ||
          !ParseInodeNum(inode_name.c_str(), inode_name.size(), &inode))
        continue;
      ResyncInode(prefixed_inode_s, inode_is_dir);
      resynced.insert(inode);
    }
    // Resync inodes removed by stand-by
    const std::string data_path = server_->configs().data_dir();
    for (unsigned i = 0; i < peer_removed.size(); ++i) {
      if (resynced.find(peer_removed[i]) != resynced.end())
        continue;
      std::string inode_str = GetPrefixedInodeStr(peer_removed[i]);
      // TODO(Joseph): WIP for #13801
      std::string inode_path = data_path + "/" + inode_str;
      // Inodes removed by stand-by might have been removed in active also
      struct stat stbuf;
      if (lstat(inode_path.c_str(), &stbuf) == 0)
        ResyncInode(inode_str, S_ISDIR(stbuf.st_mode));
    }
  }

  /* Resync an inode and its dentries (if is directory inode) */
  void ResyncInode(const std::string& prefixed_inode_s, bool inode_is_dir) {
    SendRequest(server_->meta_dir_reader()->ToInodeFim(prefixed_inode_s));
    if (inode_is_dir) {  // Recreate dentries
      boost::scoped_ptr<IDirIterator> dentry_iter(
          server_->store()->List(prefixed_inode_s));
      std::string dentry_name;
      bool dentry_is_dir;
      while (dentry_iter->GetNext(&dentry_name, &dentry_is_dir))
        SendRequest(server_->meta_dir_reader()->ToDentryFim(
            prefixed_inode_s, dentry_name, dentry_is_dir ? 'D' : 'F'));
    }
  }

  void ResyncDirtyInodes() {
    // DirtyInode: Send dirty inode list
    // Rely on opened inodes sync to update dirty activity
    DirtyInodeMap dirty_map = server_->dirty_inode_mgr()->GetList();
    for (DirtyInodeMap::iterator it = dirty_map.begin();
         it != dirty_map.end();
         ++it) {
      if (it->second.clean)
        continue;  // Will make cleanly-active during OpenedInodes resync
      FIM_PTR<ResyncDirtyInodeFim> fim = ResyncDirtyInodeFim::MakePtr();
      (*fim)->inode = it->first;
      SendRequest(fim);
    }
  }

  void ResyncPendingUnlink() {
    InodeSet defer_rm_inodes = server_->inode_usage()->pending_unlink();
    if (defer_rm_inodes.size() == 0)
      return;
    std::vector<InodeNum> data;
    data.insert(data.end(), defer_rm_inodes.begin(), defer_rm_inodes.end());
    for (unsigned i = 0; i < data.size(); i += kMaxInodesInFim) {
      unsigned to_copy = std::min(unsigned(data.size() - i), kMaxInodesInFim) *
          sizeof(InodeNum);
      FIM_PTR<ResyncPendingUnlinkFim> fim =
          ResyncPendingUnlinkFim::MakePtr(to_copy);
      (*fim)->first = (i == 0);
      (*fim)->last = (i + kMaxInodesInFim >= data.size());
      std::memcpy(fim->tail_buf(), data.data() + i, to_copy);
      SendRequest(fim);
    }
  }

  void ResyncOpenedInodes() {
    // InodeUsage: Send client opened inodes
    ClientInodeMap client_inodes = server_->inode_usage()->client_opened();
    for (ClientInodeMap::iterator it = client_inodes.begin();
         it != client_inodes.end(); ++it) {
      FIM_PTR<ResyncClientOpenedFim> fim = ResyncClientOpenedFim::MakePtr();
      (*fim)->client_num = it->first;
      (*fim)->num_writable = 0;
      AccessMapToFim(fim, it->second);
      SendRequest(fim);
    }
  }

  /**
   * Resync xattrs. Only DS group failure information for now
   */
  void ResyncExtendedAttrs() {
    std::string attr_name = "swait";
    std::string attr_val = server_->server_info()->Get(attr_name, "");
    if (attr_val.empty())
      return;
    FIM_PTR<ResyncXattrFim> fim =
        ResyncXattrFim::MakePtr(attr_val.length() + 1);
    std::memset((*fim)->name, '\0', sizeof((*fim)->name));
    std::strncpy((*fim)->name, attr_name.c_str(), sizeof((*fim)->name) - 1);
    std::memset(fim->tail_buf(), '\0', fim->tail_buf_size());
    std::strncpy(fim->tail_buf(), attr_val.c_str(), fim->tail_buf_size() - 1);
    SendRequest(fim);
  }

  /* Write an inode access map to the tail buffer of a Fim. */
  void AccessMapToFim(FIM_PTR<ResyncClientOpenedFim> fim,
                      const InodeAccessMap& access_map) {
    std::vector<InodeNum> usage;
    for (InodeAccessMap::const_iterator am_it = access_map.begin();
         am_it != access_map.end();
         ++am_it)
      if (am_it->second) {
        ++(*fim)->num_writable;
        usage.push_back(am_it->first);
      }
    for (InodeAccessMap::const_iterator am_it = access_map.begin();
         am_it != access_map.end();
         ++am_it)
      if (!am_it->second)
        usage.push_back(am_it->first);
    std::size_t buf_size = usage.size() * sizeof(InodeNum);
    fim->tail_buf_resize(buf_size);
    std::memcpy(fim->tail_buf(), usage.data(), buf_size);
  }

  /* Send request to the peer. If reply received is too few (MaxSend /
   * 2), this method will be blocked. */
  void SendRequest(FIM_PTR<IFim> fim) {
    boost::shared_ptr<IReqTracker> ms_tracker
        = server_->tracker_mapper()->GetMSTracker();
    requests_.push_back(ms_tracker->AddTransientRequest(fim));
    while (requests_.size()) {
      if (requests_.front()->reply()) {
        requests_.pop_front();
        continue;
      }
      if (requests_.size() <= (max_send_ / 2))
        break;
      requests_.front()->WaitReply();
    }
  }
};

}  // namespace

IResyncMgr* MakeResyncMgr(BaseMetaServer* server, unsigned max_send) {
  return new ResyncMgr(server, max_send);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
