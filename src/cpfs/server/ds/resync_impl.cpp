/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the interfaces for classes handling DS resync
 * operations.
 */

#include "server/ds/resync_impl.hpp"

#include <stdint.h>

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <ctime>
#include <exception>
#include <iterator>
#include <limits>
#include <list>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <boost/functional/factory.hpp>
#include <boost/functional/forward_adapter.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"
#include "config_mgr.hpp"
#include "dir_iterator.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "mutex_util.hpp"
#include "posix_fs.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "shaped_sender.hpp"
#include "store_util.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/resync.hpp"
#include "server/ds/store.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/worker_util.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * Get inode number from directory iteration results.
 *
 * @param name The name of the directory entry
 *
 * @param is_dir The is_dir flag
 *
 * @return The inode number, or 0 if the entry is not a file inode
 */
InodeNum InodeFromDirIter(std::string name, bool is_dir) {
  if (name.size() < 4)
    return 0;
  name = StripPrefixInodeStr(name);
  if (is_dir || name.substr(name.size() - 2) != ".d")
    return 0;
  name = name.substr(0, name.size() - 2);
  InodeNum ret;
  if (!ParseInodeNum(name.data(), name.size(), &ret))
    return 0;
  return ret;
}

/**
 * Maximum number of Directory Fims waiting to be sent.
 */
const unsigned kMaxDirFims = 256 * 1024;

/**
 * Maximum number of inodes to receive in one ResyncListReplyFim.
 */
const unsigned kMaxListSize = 32768;

/**
 * Maximum number of Resync Fims waiting to be sent.
 */
const unsigned kMaxResyncFims = 32768;

/**
 * Maximum number of records in a DSResyncDirFim
 */
const unsigned kMaxResyncDirRecs = 16384;

/**
 * Implement the IResyncSender interface.
 */
class ResyncSender : public IResyncSender {
 public:
  /**
   * @param server The data server using the resync manager
   *
   * @param target The role number of the DS to send to
   */
  ResyncSender(BaseDataServer* server, GroupRole target)
      : server_(server), target_(target) {}

  void SetShapedSenderMaker(ShapedSenderMaker s_sender_maker) {
    s_sender_maker_ = s_sender_maker;
  }

  void Run() {
    boost::shared_ptr<IReqTracker> target_tracker =
        server_->tracker_mapper()->GetDSTracker(
            server_->store()->ds_group(), target_);
    LOG(notice, Degraded, "Sending inode list for DS Resync");
    SendDirFims(target_tracker);
    LOG(notice, Degraded, "Sending data removal requests");
    SendDataRemoval(target_tracker);
    while (ReadResyncList(target_tracker))
      SendAllResync(target_tracker);
    LOG(notice, Degraded, "Empty resync list received, resync complete");
  }

  void SendDirFims(boost::shared_ptr<IReqTracker> target_tracker) {
    boost::scoped_ptr<IDirIterator> iter;
    time_t max_old_ctime = 0;
    if (server_->opt_resync())
      max_old_ctime = std::max(
          uint64_t(kReplAndIODelay),
          server_->dsg_ready_time_keeper()->GetLastUpdate()) - kReplAndIODelay;
    if (max_old_ctime) {
      iter.reset(server_->store()->InodeList(server_->durable_range()->Get()));
      iter->SetFilterCTime(max_old_ctime);
    } else {
      iter.reset(server_->store()->List());
    }
    boost::scoped_ptr<IShapedSender> shaped_sender(
        s_sender_maker_(target_tracker, kMaxDirFims));
    unsigned num_recs = 0;
    FIM_PTR<DSResyncDirFim> fim;
    for (;;) {
      // Carefully avoid code duplication, so that we don't need to
      // test with a large number of dentries.
      std::string name;
      bool is_dir;
      struct stat buf;
      bool to_continue = iter->GetNext(&name, &is_dir, &buf);
      if (num_recs == kMaxResyncDirRecs || (num_recs && !to_continue)) {
        fim->tail_buf_resize(num_recs * sizeof(InodeNum));
        shaped_sender->SendFim(fim);
        num_recs = 0;
      }
      if (!to_continue)
        break;
      InodeNum inode = InodeFromDirIter(name, is_dir);
      if (!inode)
        continue;
      if (num_recs == 0)
        fim = DSResyncDirFim::MakePtr(sizeof(InodeNum) * kMaxResyncDirRecs);
      reinterpret_cast<InodeNum*>(fim->tail_buf())[num_recs++] = inode;
    }
    shaped_sender->WaitAllReplied();
  }

  void SendDataRemoval(boost::shared_ptr<IReqTracker> target_tracker) {
    boost::scoped_ptr<IShapedSender> shaped_sender(
        s_sender_maker_(target_tracker, kMaxResyncFims));
    uint64_t count = 0;
    std::vector<InodeNum> removed =
        server_->inode_removal_tracker()->GetRemovedInodes();
    unsigned max_inodes = kSegmentSize / sizeof(InodeNum);
    for (unsigned pos = 0; pos < removed.size(); pos += max_inodes) {
      unsigned num_inodes = std::min(max_inodes,
                                     unsigned(removed.size() - pos));
      FIM_PTR<IFim> fim = DSResyncRemovalFim::MakePtr(
          num_inodes * sizeof(InodeNum));
      std::memcpy(fim->tail_buf(), removed.data() + pos,
                  num_inodes * sizeof(InodeNum));
      SendResyncFim(shaped_sender, fim, &count);
    }
    shaped_sender->WaitAllReplied();
  }

  size_t ReadResyncList(boost::shared_ptr<IReqTracker> target_tracker) {
    LOG(informational, Degraded, "Reading resync inode list for DS Resync");
    FIM_PTR<DSResyncListFim> req = DSResyncListFim::MakePtr();
    boost::shared_ptr<IReqEntry> entry =
        MakeTransientReqEntry(target_tracker, req);
    if (!target_tracker->AddRequestEntry(entry))
      throw std::runtime_error("Socket lost during DS Resync");
    FIM_PTR<IFim> reply = entry->WaitReply();
    if (reply->type() != kDSResyncListReplyFim)
      throw std::runtime_error("Improper reply to DS Resync List request");
    DSResyncListReplyFim& rreply = static_cast<DSResyncListReplyFim&>(*reply);
    InodeNum* inodes = reinterpret_cast<InodeNum*>(rreply.tail_buf());
    for (InodeNum i = 0; i < rreply.tail_buf_size() / sizeof(inodes[0]); ++i)
      pending_inodes_.push_back(inodes[i]);
    LOG(informational, Degraded, "Got ", PVal(pending_inodes_.size()),
        " inodes");
    return pending_inodes_.size();
  }

  void SendAllResync(boost::shared_ptr<IReqTracker> target_tracker) {
    LOG(informational, Degraded, "Sending inode data");
    std::reverse(pending_inodes_.begin(), pending_inodes_.end());
    boost::scoped_ptr<IShapedSender> shaped_sender(
        s_sender_maker_(target_tracker, kMaxResyncFims));
    uint64_t count = 0;
    for (; !pending_inodes_.empty(); pending_inodes_.pop_back()) {
      InodeNum inode = pending_inodes_.back();
      boost::scoped_ptr<IChecksumGroupIterator> cg_iter(
          server_->store()->GetChecksumGroupIterator(inode));
      FIM_PTR<DSResyncInfoFim> info_fim = DSResyncInfoFim::MakePtr();
      (*info_fim)->inode = inode;
      cg_iter->GetInfo(&((*info_fim)->mtime), &((*info_fim)->size));
      SendResyncFim(shaped_sender, info_fim, &count);
      while (FIM_PTR<IFim> fim = GetResyncFim(cg_iter.get()))
        SendResyncFim(shaped_sender, fim, &count);
    }
    SendResyncFim(shaped_sender, DSResyncDataEndFim::MakePtr(), &count);
    LOG(informational, Degraded, "Completed a phase of DS Resync");
    shaped_sender->WaitAllReplied();
    pending_inodes_.clear();
  }

 private:
  BaseDataServer* server_; /**< Data server using the sender */
  GroupRole target_; /**< Receiver of the current resync, if any */
  ShapedSenderMaker s_sender_maker_; /**< Make shaped senders */
  std::vector<InodeNum> pending_inodes_; /**< Inodes to resync, reversed */

  static void SendResyncFim(
      const boost::scoped_ptr<IShapedSender>& shaped_sender,
      const FIM_PTR<IFim>& fim,
      uint64_t* count) {
    uint64_t orig_count = (*count)++;
    shaped_sender->SendFim(fim);
    if (orig_count % 10000 == 0)
      LOG(notice, Degraded,
          "Sending data Fim ", PINT(orig_count),  " for DS Resync");
  }

  /**
   * Create one DSResyncFim for a checksum group iterator.
   *
   * @param cg_iter the iterator
   *
   * @return The created Fim, empty if all Fims for the iterator is
   * generated in previous calls
   */
  FIM_PTR<DSResyncFim> GetResyncFim(IChecksumGroupIterator* cg_iter) {
    FIM_PTR<DSResyncFim> req = DSResyncFim::MakePtr(kSegmentSize);
    (*req)->inode = pending_inodes_.back();
    std::size_t num_read = cg_iter->GetNext(&(*req)->cg_off, req->tail_buf());
    if (num_read <= 0)
      return FIM_PTR<DSResyncFim>();
    req->tail_buf_resize(num_read);
    LOG(debug, Degraded, "Sending DS inode ", PHex((*req)->inode),
        " of CG ", PINT((*req)->cg_off), " for resync");
    return req;
  }
};

/**
 * Implement the IResyncMgr interface.
 */
class ResyncMgr : public IResyncMgr {
 public:
  /**
   * @param server The data server using the sender
   */
  explicit ResyncMgr(BaseDataServer* server)
      : server_(server), rsender_maker_(kResyncSenderMaker),
        data_mutex_(MUTEX_INIT) {}

  void Start(GroupRole target) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (thread_)
      return;
    target_ = target;
    IResyncSender* sender = rsender_maker_(server_, target);
    sender->SetShapedSenderMaker(s_sender_maker_);
    thread_.reset(new boost::thread(&ResyncMgr::Run, this, sender));
  }

  bool IsStarted(GroupRole* target_ret) {
    MUTEX_LOCK_GUARD(data_mutex_);
    *target_ret = target_;
    return bool(thread_);
  }

  void SetResyncSenderMaker(ResyncSenderMaker rsender_maker) {
    rsender_maker_ = rsender_maker;
  }

  void SetShapedSenderMaker(ShapedSenderMaker s_sender_maker) {
    s_sender_maker_ = s_sender_maker;
  }

 private:
  BaseDataServer* server_; /**< Data server using the sender */
  ResyncSenderMaker rsender_maker_; /** Create sender to do the real work */
  ShapedSenderMaker s_sender_maker_; /**< Make shaped senders */
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  GroupRole target_; /**< Receiver of the current resync, if any */
  boost::scoped_ptr<boost::thread> thread_; /**< Thread during resync */

  void Run(IResyncSender* sender) {
    try {
      boost::scoped_ptr<IResyncSender>(sender)->Run();
    } catch (const std::exception& e) {
      LOG(error, Degraded, "Exception thrown when sending resync to peer DS ",
          PINT(target_), ": ", e);
    } catch (...) {
      LOG(error, Degraded, "Unknown exception thrown "
          "when sending resync to peer DS", PINT(target_));
    }
    MUTEX_LOCK_GUARD(data_mutex_);
    thread_.reset();
  }
};

/**
 * Per-DS information about resync progress held by resync receiver.
 */
struct ResyncDSInfo {
  /** Unanswered list Fim */
  FIM_PTR<DSResyncListFim> pending_list;
  /** Whether ResyncListFim is seen */
  bool list_received;
  /** Resync Fims waiting to be processed */
  std::list<FIM_PTR<DSResyncFim> > pending_fims;
  /** Whether the ResyncDataEndFim is seen */
  bool end_received;

  ResyncDSInfo() : list_received(false), end_received(false) {}
};

/**
 * Per-file meta-data information.
 */
struct ResyncDSInodeInfo {
  uint64_t size; /**< The largest file size as sent by DSs */
  FSTime mtime; /**< The latest mtime of the file as sent by DSs */
  ResyncDSInodeInfo() {
    size = 0;
    mtime.sec = 0;
    mtime.ns = 0;
  }
};

/**
 * Type to store meta data information collected.
 */
typedef boost::unordered_map<InodeNum, ResyncDSInodeInfo> ResyncDSInodeInfoMap;

/**
 * Actual type used to store all resync DS info.
 */
typedef boost::unordered_map<boost::shared_ptr<IFimSocket>, ResyncDSInfo>
ResyncDSInfoMap;

/**
 * Fim processor to handle Resync Fims
 */
class ResyncFimProcessor
    : public IResyncFimProcessor,
      private MemberFimProcessor<ResyncFimProcessor> {
  friend class MemberFimProcessor<ResyncFimProcessor>;
 public:
  /**
   * @param server The data server using the Fim processor
   */
  explicit ResyncFimProcessor(BaseDataServer* server)
      : server_(server), data_mutex_(MUTEX_INIT),
        enabled_(false), resync_list_ready_(false) {
    AddHandler(&ResyncFimProcessor::HandleDSResyncDir);
    AddHandler(&ResyncFimProcessor::HandleDSResyncRemoval);
    AddHandler(&ResyncFimProcessor::HandleDSResyncList);
    AddHandler(&ResyncFimProcessor::HandleDSResyncInfo);
    AddHandler(&ResyncFimProcessor::HandleDSResync);
    AddHandler(&ResyncFimProcessor::HandleDSResyncDataEnd);
  }

  void AsyncResync(ResyncCompleteHandler completion_handler) {
    LOG(notice, Degraded, "DS Resync started");
    MUTEX_LOCK_GUARD(data_mutex_);
    Reset();
    if (!server_->opt_resync())
      server_->store()->RemoveAll();
    enabled_ = true;
    completion_handler_ = completion_handler;
  }

  /**
   * Check whether the specified fim is accepted by this processor
   */
  bool Accept(const FIM_PTR<IFim>& fim) const {
    return MemberFimProcessor<ResyncFimProcessor>::Accept(fim);
  }

  /**
   * What to do when Fim is received from FimSocket.
   *
   * @param fim The fim received
   *
   * @param socket The socket receiving the fim
   *
   * @return Whether the handling is completed
   */
  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket) {
    MUTEX_LOCK_GUARD(data_mutex_);
    return MemberFimProcessor<ResyncFimProcessor>::Process(fim, socket);
  }

 private:
  BaseDataServer* server_; /**< The data server using the FimProcessor */
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  bool enabled_; /**< Whether the processor is enabled */
  bool resync_list_ready_; /**< Whether resync_list_ is ready */
  /** Handler to call on completion */
  ResyncCompleteHandler completion_handler_;
  /** List of updated inodes for resync */
  boost::unordered_set<InodeNum> updated_inodes_;
  /** Computed resync list */
  std::vector<InodeNum> resync_list_;
  ResyncDSInfoMap ds_infos_; /**< Per-DS information about resync progress */
  uint64_t num_handled_; /**< Number of segments handled. */
  InodeNum last_inode_; /**< Last inode written */
  /** Writer for last_inode_ */
  boost::scoped_ptr<IChecksumGroupWriter> curr_writer_;
  ResyncDSInodeInfoMap inode_info_map_; /**< Per-inode metadata info */

  /**
   * Prepare for a new round of DS resync, forgetting the information
   * about any previous round.
   */
  void Reset() {
    enabled_ = resync_list_ready_ = false;
    completion_handler_ = ResyncCompleteHandler();
    updated_inodes_.clear();
    resync_list_.clear();
    ds_infos_.clear();
    curr_writer_.reset();
    num_handled_ = 0;
    inode_info_map_.clear();
  }

  bool HandleDSResyncDir(const FIM_PTR<DSResyncDirFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer) {
    ReplyOnExit r(fim, peer);
    if (!enabled_) {
      r.SetResult(-EINVAL);
      return true;
    }
    InodeNum* recs = reinterpret_cast<InodeNum*>(fim->tail_buf());
    unsigned num_recs = fim->tail_buf_size() / sizeof(InodeNum);
    for (unsigned i = 0; i < num_recs; ++i)
      updated_inodes_.insert(recs[i]);
    r.SetResult(0);
    return true;
  }

  bool HandleDSResyncRemoval(const FIM_PTR<DSResyncRemovalFim>& fim,
                             const boost::shared_ptr<IFimSocket>& peer) {
    InodeNum num_removed = fim->tail_buf_size() / sizeof(InodeNum);
    const InodeNum* removed = reinterpret_cast<const InodeNum*>(
        fim->tail_buf());
    for (InodeNum i = 0; i < num_removed; ++i)
      server_->store()->FreeData(removed[i], true);
    ReplyOnExit(fim, peer).SetResult(0);
    return true;
  }

  bool HandleDSResyncList(const FIM_PTR<DSResyncListFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer) {
    if (!enabled_) {
      ReplyOnExit(fim, peer).SetResult(-EINVAL);
      return true;
    }
    ResyncDSInfo& info = ds_infos_[peer];
    info.list_received = true;
    info.pending_list = fim;
    if (!CheckDSInfosSize())
      return true;
    for (ResyncDSInfoMap::const_iterator it = ds_infos_.cbegin();
         it != ds_infos_.cend();
         ++it)
      if (!it->second.list_received)
        return true;
    if (!resync_list_ready_)
      ComposeResyncList();
    ReplyResyncList();
    return true;
  }

  /**
   * Compose the Resync list.  This is invoked during optimized
   * resync, on the first time when all expected peers have sent a
   * DSResyncListFim request, indicating that all of them have all
   * their DSResyncDirFim requests sent.  This method then compute the
   * combined resync list and drop the original updated inode lists.
   *
   * This method works by listing the directory of itself, deleting a
   * file if it is new in the recovering DS.
   */
  void ComposeResyncList() {
    LOG(notice, Degraded, "Composing full inode lists");
    server_->inode_removal_tracker()->SetPersistRemoved(true);
    time_t max_old_ctime = std::max(
        uint64_t(kReplAndIODelay),
        server_->dsg_ready_time_keeper()->GetLastUpdate())
        - kReplAndIODelay;
    boost::scoped_ptr<IDirIterator> iter;
    if (max_old_ctime) {
      iter.reset(server_->store()->InodeList(server_->durable_range()->Get()));
      iter->SetFilterCTime(max_old_ctime);
    } else {
      iter.reset(server_->store()->List());
    }
    std::string name;
    bool is_dir;
    struct stat buf;
    while (iter->GetNext(&name, &is_dir, &buf)) {
      InodeNum inode = InodeFromDirIter(name, is_dir);
      if (inode)
        updated_inodes_.insert(inode);
    }
    {
      std::vector<InodeNum> removed =
          server_->inode_removal_tracker()->GetRemovedInodes();
      std::copy(removed.begin(), removed.end(),
                std::inserter(updated_inodes_, updated_inodes_.end()));
    }
    BOOST_FOREACH(InodeNum to_del, updated_inodes_) {
      server_->store()->FreeData(to_del, true);
    }
    std::copy(updated_inodes_.begin(), updated_inodes_.end(),
              std::back_inserter(resync_list_));
    updated_inodes_.clear();
    std::sort(resync_list_.begin(), resync_list_.end());
    std::reverse(resync_list_.begin(), resync_list_.end());
    resync_list_ready_ = true;
    LOG(informational, Degraded, "DS resync list: num to resync = ",
        PINT(resync_list_.size()));
  }

  /**
   * Send reply to DSResyncListFim previously received, using
   * information collected previously by ComposeResyncList.
   */
  void ReplyResyncList() {
    size_t max_cnt = server_->configs().data_sync_num_inodes() - 1;  // 0 -> inf
    max_cnt = std::min<size_t>(max_cnt, kMaxListSize - 1) + 1;
    std::vector<InodeNum> phase_resync_list;
    while (!resync_list_.empty() && phase_resync_list.size() < max_cnt) {
      InodeNum inode = resync_list_.back();
      phase_resync_list.push_back(inode);
      resync_list_.pop_back();
    }
    LOG(informational, Degraded, "Sending resync inode list, size ",
        PVal(phase_resync_list.size()));
    for (ResyncDSInfoMap::iterator it = ds_infos_.begin();
         it != ds_infos_.end();
         ++it) {
      const FIM_PTR<DSResyncListFim>& fim = it->second.pending_list;
      const boost::shared_ptr<IFimSocket>& peer = it->first;
      ReplyOnExit r(fim, peer);
      FIM_PTR<DSResyncListReplyFim> reply =
          DSResyncListReplyFim::MakePtr(
              phase_resync_list.size() * sizeof(InodeNum));
      r.SetNormalReply(reply);
      InodeNum* inodes = reinterpret_cast<InodeNum*>(reply->tail_buf());
      for (size_t i = 0; i < phase_resync_list.size(); ++i)
        inodes[i] = phase_resync_list[i];
      it->second.pending_list = 0;
      it->second.list_received = false;
    }
    if (phase_resync_list.empty())
      Finalize();
  }

  bool HandleDSResyncInfo(const FIM_PTR<DSResyncInfoFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer) {
    ResyncDSInodeInfo& info = inode_info_map_[(*fim)->inode];
    info.size = std::max(info.size, (*fim)->size);
    if (info.mtime < (*fim)->mtime) {
      info.mtime.sec = (*fim)->mtime.sec;
      info.mtime.ns = (*fim)->mtime.ns;
    }
    ReplyOnExit(fim, peer).SetResult(0);
    return true;
  }

  /**
   * Handle DSResyncFim.  The handling and reply may be deferred if
   * the ResyncMgr is waiting for more Fims from other DS to complete
   * the inode segment.
   *
   * @param fim The DSResyncFim
   *
   * @param peer The DS sending the Fim
   */
  bool HandleDSResync(const FIM_PTR<DSResyncFim>& fim,
                      const boost::shared_ptr<IFimSocket>& peer) {
    if (!enabled_) {
      ReplyOnExit(fim, peer).SetResult(-EINVAL);
      return true;
    }
    ds_infos_[peer].pending_fims.push_back(fim);
    Flush();
    return true;
  }

  bool HandleDSResyncDataEnd(const FIM_PTR<DSResyncDataEndFim>& fim,
                             const boost::shared_ptr<IFimSocket>& peer) {
    ReplyOnExit r(fim, peer);
    if (!enabled_) {
      r.SetResult(-EINVAL);
      return true;
    }
    LOG(debug, Degraded, "DS Resync stream end for ", PVal(peer));
    ds_infos_[peer].end_received = true;
    Flush();
    r.SetResult(0);
    return true;
  }

  /**
   * Process all DSResyncFim received so far if possible.
   */
  void Flush() {
    while (FlushOne()) {}
  }

  /**
   * Process one DSResyncFim received if possible.  The completion
   * handler is run if all Fims are handled, and the
   * DSResyncDataEndFim is received from all peers.
   */
  bool FlushOne() {
    if (!CheckDSInfosSize())
      return false;
    InodeNum inode = std::numeric_limits<InodeNum>::max();
    std::size_t cg_off = std::numeric_limits<std::size_t>::max();
    bool has_unfinished = false;
    for (ResyncDSInfoMap::const_iterator it = ds_infos_.cbegin();
         it != ds_infos_.cend();
         ++it) {
      const ResyncDSInfo& ds_info = it->second;
      if (ds_info.pending_fims.empty()) {
        if (ds_info.end_received)
          continue;
        return false;
      }
      has_unfinished = true;
      DSResyncFim* fim = ds_info.pending_fims.front().get();
      if ((*fim)->inode < inode ||
          (inode == (*fim)->inode && (*fim)->cg_off < cg_off)) {
        inode = (*fim)->inode;
        cg_off = (*fim)->cg_off;
      }
    }
    if (!has_unfinished) {
      for (ResyncDSInfoMap::iterator it = ds_infos_.begin();
           it != ds_infos_.end();
           ++it)
        it->second.end_received = false;
      FlushInodeInfo();
      return false;
    }
    if (num_handled_ % 10000 == 0)
      LOG(notice, Degraded, "DS Resync segment ", PVal(num_handled_));
    ++num_handled_;
    WriteData(inode, cg_off);
    return true;
  }

  /**
   * Flush inode info.
   *
   * This actually writes the inode information as extended attributes
   * of the files representing them.  This is done last, so that we
   * don't need to create a file unnecessarily in case the file
   * content is all null bytes (when the file creation is skipped).
   * After completion, sync() is run to ensure that the resync'ed
   * contents reach the disk.
   */
  void FlushInodeInfo() {
    for (ResyncDSInodeInfoMap::const_iterator it = inode_info_map_.cbegin();
         it != inode_info_map_.cend();
         ++it)
      server_->store()->UpdateAttr(
          it->first, it->second.mtime, it->second.size);
    inode_info_map_.clear();
  }

  /**
   * Finalize resync.
   */
  void Finalize() {
    server_->posix_fs()->Sync();
    if (completion_handler_)
      completion_handler_(true);
    Reset();
  }

  /**
   * Write data collected from DSResyncFim for an inode at a
   * particular offset to the file in the store storing it.  Reply to
   * the peer sending the corresponding Fim so that it can send more
   * Fims.
   *
   * @param inode The inode
   *
   * @param cg_off The checksum group offset
   */
  void WriteData(InodeNum inode, std::size_t cg_off) {
    LOG(debug, Degraded, "DS Resync for inode ", PHex(inode),
        " of CG ", PINT(cg_off));
    char data[kSegmentSize];
    std::memset(data, 0, kSegmentSize);
    std::size_t size = 0;
    for (ResyncDSInfoMap::iterator it = ds_infos_.begin();
         it != ds_infos_.end();
         ++it) {
      ResyncDSInfo& ds_info = it->second;
      if (!ds_info.pending_fims.empty()) {
        const FIM_PTR<DSResyncFim>& fim =
            ds_info.pending_fims.front();
        DSResyncFim* dfim = fim.get();
        if (inode != (*dfim)->inode || cg_off != (*dfim)->cg_off)
          continue;
        LOG(debug, Degraded, "Adding DS Resync data from ", PVal(it->first));
        XorBytes(data, dfim->tail_buf(), dfim->tail_buf_size());
        size = std::max(size, std::size_t(dfim->tail_buf_size()));
        ReplyOnExit(fim, it->first).SetResult(0);
        ds_info.pending_fims.pop_front();
      }
    }
    if (std::memcmp(data, kEmptyBuf, size) == 0)
      return;
    if (!curr_writer_ || inode != last_inode_) {
      curr_writer_.reset(server_->store()->GetChecksumGroupWriter(inode));
      last_inode_ = inode;
    }
    curr_writer_->Write(data, size, cg_off);
  }

  /**
   * @return true if all other DSs of the same group have sent their
   * first Fim.
   */
  bool CheckDSInfosSize() {
    if (ds_infos_.size() >= kNumDSPerGroup) {
      LOG(error, Server, "Too many peers for DS resync");
      if (completion_handler_)
        completion_handler_(false);
      Reset();
      return false;
    }
    return ds_infos_.size() == kNumDSPerGroup - 1;
  }
};

}  // namespace

ResyncSenderMaker kResyncSenderMaker = boost::forward_adapter<
  boost::factory<ResyncSender*> >();

IResyncMgr* MakeResyncMgr(BaseDataServer* server) {
  return new ResyncMgr(server);
}

IResyncFimProcessor* MakeResyncFimProcessor(BaseDataServer* server) {
  return new ResyncFimProcessor(server);
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
