/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define implementation of the IWorker interface for the data server.
 */

#include "server/ds/worker_impl.hpp"

#include <stdint.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <exception>
#include <vector>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/signals2.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"
#include "ds_iface.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "mutex_util.hpp"
#include "op_completion.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "tracer.hpp"
#include "tracker_mapper.hpp"
#include "server/ccache_tracker.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/cleaner.hpp"
#include "server/ds/degrade.hpp"
#include "server/ds/store.hpp"
#include "server/fim_defer.hpp"
#include "server/fim_defer_impl.hpp"
#include "server/worker.hpp"
#include "server/worker_base.hpp"
#include "server/worker_util.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * Make an appropriate reply on exit of scope.  To make a successful
 * reply, clients must either call SetNormalReply to set the reply, or
 * make a call SetResult with a non-negative value.  Any call to
 * SetResult with a negative value will cause the reply to be replaced
 * by a ResultCodeReplyFim with err_no being the negation of the last
 * negative value set.  If neither SetNormalReply nor SetResult is
 * called, the a ResultCodeReplyFim with err_no of EIO will be
 * replied.
 */
class DSReplyOnExit : public BaseWorkerReplier {
 public:
  /**
   * @param req The request (for finding the ID to reply to).
   *
   * @param peer The peer to send reply to.
   */
  explicit DSReplyOnExit(const FIM_PTR<IFim>& req,
                         const boost::shared_ptr<IFimSocket>& peer)
      : BaseWorkerReplier(req, peer), replicating_(false) {}

  void ReplyComplete() {
    reply_->set_req_id(req_->req_id());
    if (!replicating_)
      reply_->set_final();
    peer_->WriteMsg(reply_);
  }

  /**
   * Set that the operation is being replicated.  This causes the
   * final bit not to be set in the reply.
   */
  void SetReplicating() {
    replicating_ = true;
  }

  ~DSReplyOnExit() {
    MakeReply();
  }

 private:
  bool replicating_;  // Whether the operation is being replicated
};

/** Find inode number stored in a Fim */
InodeNum FimInodeNum(const FIM_PTR<IFim>& fim) {
  return reinterpret_cast<InodeNum&>(*(fim->body()));
}

/** Type for simple callback */
typedef boost::function<void()> Callback;

/**
 * Type for the deferred callbacks to be called
 */
typedef boost::signals2::signal<void()> DeferCallbackSlots;

class Worker;

/**
 * Manage deferment due to DS resync.
 */
class ResyncDeferment {
 public:
  /**
   * Constructor
   *
   * @param worker The associated worker
   */
  explicit ResyncDeferment(Worker* worker)
      : worker_(worker),
        defer_all_fims_(false), defer_mgr_(MakeFimDeferMgr()),
        defer_released_(new DeferCallbackSlots) {}

  /**
   * Refetch the current state and retry all pending actions.
   *
   * Once the current state is updated, it runs all callbacks and then
   * processes all pending Fims.  The spaces are emptied first, so
   * that these actions can enter callback again.
   */
  void ResetDefer();

  /**
   * Defer a client Fim if necessary.
   *
   * The caller should have acquired the DSG info read lock before
   * calling this.
   *
   * @param fim The Fim to check
   *
   * @param socket The client socket
   *
   * @return Whether the Fim needs to be deferred
   */
  bool ProcessFimDefer(const FIM_PTR<IFim>& fim,
                       const boost::shared_ptr<IFimSocket>& socket) {
    if (!InodeNeedDefer(FimInodeNum(fim)))
      return false;
    defer_mgr_->AddFimEntry(fim, socket);
    return true;
  }

  /**
   * Defer a callback if necessary.
   *
   * The caller should have acquired the DSG info read lock before
   * calling this.
   *
   * @param inode The inode being operated
   *
   * @param callback The callback
   *
   * @return Whether the callback needs to be deferred
   */
  bool ProcessCallbackDefer(InodeNum inode, Callback callback) {
    if (!InodeNeedDefer(inode))
      return false;
    defer_released_->connect(callback);
    return true;
  }

 private:
  /** The associated worker */
  Worker* worker_;
  /**
   * Use the worker-wide (rather than inode-wide) Fim deferring
   * manager, which is triggered when the DSG is found to be
   * recovering when a Fim needs to be processed.  When activated, all
   * FC Fim processing are deferred.  To reset it, send a
   * DeferResetFim to the worker (see below)
   */
  bool defer_all_fims_;
  /**
   * Inodes currently being deferred.  This may contains a set
   * different from the list in the DSG states, because a deferment is
   * discovered only by ProcessFimDefer(), and because a deferment
   * needs to be maintained until it is reset by ResetDefer().
   */
  boost::unordered_set<InodeNum> inodes_deferred_;
  /**
   * The Fim deferring manager to hold Fims
   */
  boost::scoped_ptr<IFimDeferMgr> defer_mgr_;
  /**
   * Callbacks to run upon lifting of deferment
   */
  boost::scoped_ptr<DeferCallbackSlots> defer_released_;

  /**
   * Update the defer_all_fims_ flag.
   *
   * @param state_ret Whether to put the DSG state found, skip if 0
   */
  void UpdateDeferAll(DSGroupState* state_ret);

  /**
   * Check whether access to an inode needs to be deferred.
   *
   * This updates the internal states so that if an inode needs to be
   * deferred, further access to the same inode before a ResetDefer()
   * will also be deferred.
   *
   * The caller should have acquired the DSG info read lock before
   * calling this.
   *
   * @param inode The inode to check
   *
   * @return Whether the inode access should be deferred
   */
  bool InodeNeedDefer(InodeNum inode);
};

/**
 * Implement the IFimProcessor interface for data server.
 */
class Worker : public BaseWorker, private MemberFimProcessor<Worker> {
  // Allow casting from MemberFimProcessor<Worker> to Worker
  // within template MemberFimProcessor
  friend class MemberFimProcessor<Worker>;
  friend class ResyncDeferment;

 public:
  Worker();

  /**
   * Check whether the specified fim is accepted by this worker
   */
  bool Accept(const FIM_PTR<IFim>& fim) const;

  /**
   * Defer the fim if the inode is locked and it originates from a FC,
   * otherwise process it using the underlying member fim processor.
   *
   * @param fim The fim object.
   *
   * @param socket The sender.
   */
  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket);

 private:
  /** How to handle deferments due to resync */
  ResyncDeferment resync_deferment_;
  /**
   * Inode defer manager, used when an inode lock is request by the MS
   */
  boost::scoped_ptr<IInodeFimDeferMgr> inode_defer_mgr_;
  /**
   * Callbacks deferred while inode is being locked
   */
  boost::unordered_map<InodeNum, DeferCallbackSlots> inode_defer_released_;
  /**
   * Whether the worker is in distress mode.  When triggered, all
   * writes will go through the distress defer manager, so that the
   * same segment of the same inode will never be written by two FC
   * operations in such a way that the periods between write and
   * checksum update overlaps
   */
  bool distressed_;
  /**
   * Defer manager to defer segments when distressed
   */
  boost::scoped_ptr<ISegmentFimDeferMgr> distress_defer_mgr_;

  /**
   * Same as Process(), but assume shared lock for DSG state already
   * obtained.
   */
  bool Process_(const FIM_PTR<IFim>& fim,
                const boost::shared_ptr<IFimSocket>& socket);

  bool HandleDSInodeLock(const FIM_PTR<DSInodeLockFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer);
  bool HandleDSGDistressModeChange(
      const FIM_PTR<DSGDistressModeChangeFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleWrite(const FIM_PTR<WriteFim>& fim,
                   const boost::shared_ptr<IFimSocket>& peer);
  bool HandleRevertWrite(const FIM_PTR<RevertWriteFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer);
  bool HandleDistressedWriteCompletion(
      const FIM_PTR<DistressedWriteCompletionFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleRead(const FIM_PTR<ReadFim>& fim,
                  const boost::shared_ptr<IFimSocket>& peer);
  bool HandleDataRecovery(const FIM_PTR<DataRecoveryFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer);
  bool HandleRecoveryData(const FIM_PTR<RecoveryDataFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer);
  bool HandleChecksumUpdate(const FIM_PTR<ChecksumUpdateFim>& fim,
                            const boost::shared_ptr<IFimSocket>& peer);
  bool HandleTruncateData(const FIM_PTR<TruncateDataFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer);
  bool HandleMtimeUpdate(const FIM_PTR<MtimeUpdateFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer);
  bool HandleFreeData(const FIM_PTR<FreeDataFim>& fim,
                      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleAttrUpdateFim(const FIM_PTR<AttrUpdateFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer);

  /**
   * @return Data server using the worker
   */
  BaseDataServer* ds() {
    return static_cast<BaseDataServer*>(server());
  }

  void DSInodeLocked(const FIM_PTR<DSInodeLockFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer);

  GroupRole GetRole() {
    return ds()->store()->ds_role();
  }

  bool IsDSGDegraded(GroupRole* failed_ret, InodeNum inode) {
    uint64_t state_change_id;
    DSGroupState state = ds()->dsg_state(&state_change_id, failed_ret);
    if (state == kDSGDegraded)
      return true;
    return state == kDSGResync && ds()->is_inode_to_resync(inode);
  }

  bool IsDSFailed(GroupRole role, InodeNum inode) {
    GroupRole failed;
    return IsDSGDegraded(&failed, inode) && failed == role;
  }

  /**
   * Check whether the DS role is the checksum role, and if so whether
   * the system is known to be degraded and whether it is necessary to
   * start recovery.
   *
   * @param fim The Fim triggering the operation
   *
   * @param peer The peer sending the Fim
   *
   * @param checksum_role The checksum role for the operation
   *
   * @param inode The inode for the operation
   *
   * @param dsg_off The DSG offset of the operation
   *
   * @param cache_handle_ret Where to put the cache handle in case of
   * degraded operation with data already recovered
   *
   * @return Whether the check already does all work that can be done
   * at the moment (either there is an error and is already replied,
   * or recovery is initiated and is pending)
   */
  bool DegradedModeCheck(const FIM_PTR<IFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer,
                         GroupRole target_role, GroupRole checksum_role,
                         InodeNum inode, std::size_t dsg_off,
                         boost::shared_ptr<IDegradedCacheHandle>*
                         cache_handle_ret) {
    if (GetRole() == checksum_role) {
      GroupRole failed;
      if (!IsDSGDegraded(&failed, inode) || failed != target_role) {
        LOG(informational, Degraded, "Inappropriate degraded msg, ", PVal(fim));
        FIM_PTR<NotDegradedFim> redirect_req =
            NotDegradedFim::MakePtr();
        (*redirect_req)->redirect_req = fim->req_id();
        peer->WriteMsg(redirect_req);
        return true;
      }
      *cache_handle_ret = DoReqRecovery(fim, peer, inode, CgStart(dsg_off));
      if (!*cache_handle_ret)
        return true;  // Actions to be performed later, no reply sent yet
    }
    return false;
  }

  /**
   * Perform degrade mode data recovery.  After the call, either the
   * recovery result is already cached and a handle is returned, or
   * the recovery is started and the Fim is queued for reprocessing
   * once recovery is complete.
   *
   * @param fim The Fim triggering the recovery
   *
   * @param peer The peer sending the Fim
   *
   * @param inode The inode that needs to be recovered
   *
   * @param cg_start The DSG offset of the checksum group for recovery
   *
   * @return The cache handle in case the recovered data is
   * immediately available
   */
  boost::shared_ptr<IDegradedCacheHandle> DoReqRecovery(
      const FIM_PTR<IFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer,
      InodeNum inode, std::size_t cg_start);

  void DegradedTruncateData(const FIM_PTR<TruncateDataFim>& fim,
                            const boost::shared_ptr<IFimSocket>& peer);

  /**
   * Completion callback after write replication completion.  In
   * addition to the normal call to handle disk full conditions,
   *
   * @param entry The request entry completed
   *
   * @param req The checksum update request for the replication
   *
   * @param peer The client socket the original WriteFim is received from
   *
   * @param distress Whether the Write is activated when the worker is
   * distressed
   */
  void WriteReplCallback(const boost::shared_ptr<IReqEntry>& entry,
                         const FIM_PTR<ChecksumUpdateFim>& req,
                         const boost::shared_ptr<IFimSocket>& peer,
                         bool distress);

  /**
   * Send final reply.  Normally, if we detect that we are going to do
   * replication, final reply are sent by the completion checker when
   * the reply to a replication Fim is received.  But at times we
   * determined that we are going to do replication, send the normal
   * reply, but then find that the replication call fails.  This
   * function is then used to clear up the mess.
   *
   * @param req_id The request Id to reply for
   *
   * @param peer The peer to send the reply to
   */
  void SendFinalReply(ReqId req_id, const boost::shared_ptr<IFimSocket>& peer) {
    FIM_PTR<FinalReplyFim> final_reply = FinalReplyFim::MakePtr();
    final_reply->set_req_id(req_id);
    final_reply->set_final();
    (*final_reply)->err_no = 0;
    peer->WriteMsg(final_reply);
  }

  /**
   * Unlock a DS inode lock.
   *
   * @param inode The inode to unlock
   */
  void DSInodeUnlock(InodeNum inode);

  /**
   * Handle a ReadFim, skipping the distress test.
   *
   * @param fim The ReadFim to handle
   *
   * @param peer The peer from where the ReadFim is received
   */
  void DoRead(const FIM_PTR<ReadFim>& fim,
              const boost::shared_ptr<IFimSocket>& peer,
              const boost::shared_ptr<IDegradedCacheHandle>& cache_handle);

  /**
   * Handle a WriteFim, skipping the distress test.
   *
   * @param fim The WriteFim to handle
   *
   * @param peer The peer from where the WriteFim is received
   *
   * @return Whether all processing for the Fim has been completed
   * (i.e., no checksum update is pending)
   */
  bool DoWrite(const FIM_PTR<WriteFim>& fim,
               const boost::shared_ptr<IFimSocket>& peer,
               const boost::shared_ptr<IDegradedCacheHandle>& cache_handle);

  /**
   * Notify FCs that an inode has been written by a client.
   *
   * @param inode The inode
   *
   * @param peer The peer writing it
   */
  void NotifyFCOnWrite(InodeNum inode,
                       const boost::shared_ptr<IFimSocket>& peer) {
    ClientNum client_num = peer->GetReqTracker()->peer_client_num();
    cache_invalidator_.InvalidateClients(inode, true, client_num);
    ds()->cache_tracker()->SetCache(inode, client_num);
  }

  /**
   * Handle completion of a distressed write.  Check to see if any Fim
   * is queued on the same segment, and do the processing repeatedly
   * if found until processing of one Fim cannot complete immediately.
   *
   * @param inode The inode number
   *
   * @param segment The segment
   */
  void DoDistressedWriteComplete(InodeNum inode, uint64_t segment);

  /**
   * Complete the replication operation.
   *
   * @param entry The request entry of the write / truncate operation
   *
   * @param peer The peer to send the reply to
   *
   * @param inode The inode number involved
   */
  void CompleteReplOp(const boost::shared_ptr<IReqEntry>& entry,
                      const boost::shared_ptr<IFimSocket>& peer,
                      InodeNum inode) {
    SendFinalReply(entry->req_id(), peer);
    ds()->op_completion_checker_set()->CompleteOp(inode, entry.get());
  }
};

void ResyncDeferment::ResetDefer() {
  defer_all_fims_ = false;
  inodes_deferred_.clear();
  UpdateDeferAll(0);
  if (defer_all_fims_)
    return;
  // Pick up deferred works to do
  boost::scoped_ptr<DeferCallbackSlots> my_slots(new DeferCallbackSlots);
  my_slots.swap(defer_released_);
  std::vector<DeferredFimEntry> deferred_entries;
  while (defer_mgr_->HasFimEntry())
    deferred_entries.push_back(defer_mgr_->GetNextFimEntry());
  // Run deferred works
  (*my_slots)();
  my_slots->disconnect_all_slots();
  BOOST_FOREACH(DeferredFimEntry& entry,  // NOLINT(runtime/references)
                deferred_entries) {
    if (!ProcessFimDefer(entry.fim(), entry.socket()))
      if (!worker_->Process_(entry.fim(), entry.socket()))
        LOG(warning, Fim, "Unprocessed Fim ", PVal(entry.fim()),
            " on DS worker unlock");
  }
}

void ResyncDeferment::UpdateDeferAll(DSGroupState* state_ret) {
  uint64_t sc_id;
  GroupRole failed_role;
  DSGroupState state;
  if (!state_ret)
    state_ret = &state;
  *state_ret = worker_->ds()->dsg_state(&sc_id, &failed_role);
  if (*state_ret == kDSGRecovering)
    defer_all_fims_ = true;
}

bool ResyncDeferment::InodeNeedDefer(InodeNum inode) {
  if (defer_all_fims_)
    return true;
  DSGroupState state;
  UpdateDeferAll(&state);
  if (defer_all_fims_)
    return true;
  if (inodes_deferred_.find(inode) != inodes_deferred_.end())
    return true;
  if (state == kDSGResync && worker_->ds()->is_inode_resyncing(inode)) {
    inodes_deferred_.insert(inode);
    return true;
  }
  return false;
}

Worker::Worker() : resync_deferment_(this),
                   inode_defer_mgr_(MakeInodeFimDeferMgr()),
                   distressed_(false),
                   distress_defer_mgr_(MakeSegmentFimDeferMgr()) {
  AddHandler(&Worker::HandleDSInodeLock);
  AddHandler(&Worker::HandleDSGDistressModeChange);
  AddHandler(&Worker::HandleWrite);
  AddHandler(&Worker::HandleRevertWrite);
  AddHandler(&Worker::HandleDistressedWriteCompletion);
  AddHandler(&Worker::HandleRead);
  AddHandler(&Worker::HandleDataRecovery);
  AddHandler(&Worker::HandleRecoveryData);
  AddHandler(&Worker::HandleChecksumUpdate);
  AddHandler(&Worker::HandleTruncateData);
  AddHandler(&Worker::HandleMtimeUpdate);
  AddHandler(&Worker::HandleFreeData);
  AddHandler(&Worker::HandleAttrUpdateFim);
}

bool Worker::Accept(const FIM_PTR<IFim>& fim) const {
  return MemberFimProcessor<Worker>::Accept(fim) ||
      fim->type() == kDeferResetFim;
}

bool Worker::Process(const FIM_PTR<IFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
  boost::shared_lock<boost::shared_mutex> lock;
  ds()->ReadLockDSGState(&lock);
  MUTEX_LOCK_SCOPE_LOG();
  if (fim->type() == kDeferResetFim) {
    resync_deferment_.ResetDefer();
    return true;
  }
  if (socket && socket->GetReqTracker()->peer_client_num() != kNotClient &&
      resync_deferment_.ProcessFimDefer(fim, socket))
    return true;
  return Process_(fim, socket);
}

bool Worker::Process_(const FIM_PTR<IFim>& fim,
                      const boost::shared_ptr<IFimSocket>& socket) {
  if (fim->type() >= kMinReqReplyFimType) {
    InodeNum inode = FimInodeNum(fim);
    if (inode_defer_mgr_->IsLocked(inode) && socket &&
        socket->GetReqTracker()->peer_client_num() != kNotClient) {
      LOG(debug, FS, "Deferring processing for ", PVal(fim));
      inode_defer_mgr_->AddFimEntry(inode, fim, socket);
      return true;
    }
  }
  return MemberFimProcessor<Worker>::Process(fim, socket);
}

bool Worker::HandleDSInodeLock(const FIM_PTR<DSInodeLockFim>& fim,
                               const boost::shared_ptr<IFimSocket>& peer) {
  InodeNum inode = (*fim)->inode;
  ds()->tracer()->Log(__func__, inode, kNotClient);
  if ((*fim)->lock == 1) {
    inode_defer_mgr_->SetLocked(inode, true);
    boost::shared_ptr<IOpCompletionChecker> checker =
        ds()->op_completion_checker_set()->Get(inode);
    checker->OnCompleteAll(boost::bind(&Worker::DSInodeLocked,
                                       this, fim, peer));
    // Don't reply
  } else if ((*fim)->lock == 0) {
    DSReplyOnExit replier(fim, peer);
    DSInodeUnlock(inode);
    replier.SetResult(0);
  } else if ((*fim)->lock == 2) {
    std::vector<InodeNum> locked = inode_defer_mgr_->LockedInodes();
    BOOST_FOREACH(InodeNum l_inode, locked) {
      DSInodeUnlock(l_inode);
    }
    // Don't reply
  }
  return true;
}

void Worker::DSInodeUnlock(InodeNum inode) {
  ds()->tracer()->Log(__func__, inode, kNotClient);
  if (resync_deferment_.ProcessCallbackDefer(
          inode, boost::bind(&Worker::DSInodeUnlock, this, inode)))
    return;
  std::vector<DeferredFimEntry> entries;
  while (inode_defer_mgr_->HasFimEntry(inode))
    entries.push_back(inode_defer_mgr_->GetNextFimEntry(inode));
  inode_defer_mgr_->SetLocked(inode, false);
  if (inode_defer_released_.find(inode) != inode_defer_released_.end()) {
    inode_defer_released_[inode]();
    inode_defer_released_.erase(inode);
  }
  BOOST_FOREACH(DeferredFimEntry entry, entries) {
    if (!MemberFimProcessor<Worker>::Process(entry.fim(), entry.socket()))
      LOG(warning, Fim, "Unprocessed Fim ", PVal(entry.fim()),
          " on DS unlock");
  }
}

void Worker::DSInodeLocked(const FIM_PTR<DSInodeLockFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
  DSReplyOnExit replier(fim, peer);
  replier.SetResult(0);
}

bool Worker::HandleDSGDistressModeChange(
    const FIM_PTR<DSGDistressModeChangeFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  (void) peer;  // Always null
  ds()->tracer()->Log(__func__, 0, kNotClient);
  distressed_ = (*fim)->distress;
  if (!distressed_) {
    DeferredFimEntry entry;
    while (distress_defer_mgr_->ClearNextFimEntry(&entry))
      Process_(entry.fim(), entry.socket());
  }
  return true;
}

bool Worker::HandleWrite(const FIM_PTR<WriteFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer) {
  InodeNum inode = (*fim)->inode;
  uint64_t dsg_off = (*fim)->dsg_off;
  ds()->tracer()->Log(
      __func__, inode, peer->GetReqTracker()->peer_client_num());
  boost::shared_ptr<IDegradedCacheHandle> cache_handle;
  if (DegradedModeCheck(fim, peer, (*fim)->target_role, (*fim)->checksum_role,
                        inode, dsg_off, &cache_handle))
    return true;
  if (!distressed_ || cache_handle) {
    DoWrite(fim, peer, cache_handle);
    return true;
  }
  // distressed_, and the WriteFim is has me as the target_role
  uint64_t segment = dsg_off / kSegmentSize;
  if (distress_defer_mgr_->IsLocked(inode, segment)) {
    distress_defer_mgr_->AddFimEntry(inode, segment, fim, peer);
    return true;
  }
  if (!DoWrite(fim, peer, cache_handle))
    distress_defer_mgr_->SetLocked(inode, segment, true);
  return true;
}

bool Worker::DoWrite(const FIM_PTR<WriteFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer,
                     const boost::shared_ptr<IDegradedCacheHandle>&
                     cache_handle) {
  InodeNum inode = (*fim)->inode;
  uint64_t dsg_off = (*fim)->dsg_off;
  // This lock needs to be put before the replier, to guarantee that
  // the reply is sent before the reply to the replication is handled.
  // This prevent a possible case where final reply is sent before the
  // initial reply.
  boost::unique_lock<MUTEX_TYPE> repl_lock;
  boost::scoped_ptr<DSReplyOnExit> replier(new DSReplyOnExit(fim, peer));
  FIM_PTR<ChecksumUpdateFim> update_fim =
      ChecksumUpdateFim::MakePtr(fim->tail_buf_size());
  update_fim->set_req_id(fim->req_id());
  if (cache_handle) {  // Degraded write
    cache_handle->Write(dsg_off % kSegmentSize, fim->tail_buf(),
                        fim->tail_buf_size(), update_fim->tail_buf());
    int result = ds()->store()->ApplyDelta(
        inode, (*fim)->optime, (*fim)->last_off, DsgToChecksumOffset(dsg_off),
        update_fim->tail_buf(), update_fim->tail_buf_size());
    if (result >= 0)
      NotifyFCOnWrite(inode, peer);
    else
      cache_handle->RevertWrite(dsg_off % kSegmentSize, update_fim->tail_buf(),
                                update_fim->tail_buf_size());
    replier->SetResult(result);
    return true;
  }
  FSTime& optime = (*fim)->optime;
  int result = ds()->store()->
      Write(inode, optime, (*fim)->last_off, DsgToDataOffset(dsg_off),
            fim->tail_buf(), fim->tail_buf_size(), update_fim->tail_buf());
  replier->SetResult(result);
  if (result < 0)
    return true;
  // Invalidate cache for others, set cache for writing client
  NotifyFCOnWrite(inode, peer);
  // Send checksum change request
  if (IsDSFailed((*fim)->checksum_role, inode))
    return true;
  (*update_fim)->inode = inode;
  (*update_fim)->optime = (*fim)->optime;
  (*update_fim)->dsg_off = dsg_off;
  (*update_fim)->last_off = (*fim)->last_off;
  boost::shared_ptr<IOpCompletionChecker> checker =
      ds()->op_completion_checker_set()->Get(inode);
  boost::shared_ptr<IReqTracker> tracker = ds()->tracker_mapper()->
      GetDSTracker(ds()->store()->ds_group(), (*fim)->checksum_role);
  replier->SetReplicating();
  replier.reset();  // Reply first, replicate later to avoid race condition
  boost::shared_ptr<IReqEntry> entry = MakeReplReqEntry(tracker, update_fim);
  if (tracker->AddRequestEntry(entry, &repl_lock)) {
    entry->OnAck(boost::bind(&Worker::WriteReplCallback, this, _1,
                             update_fim, peer, distressed_));
    checker->RegisterOp(entry.get());
    return false;
  } else {
    SendFinalReply(fim->req_id(), peer);
    return true;
  }
}

/**
 * Memo for workers to revert writes.
 */
class RevertWriteMemo : public WorkerMemo {
 public:
  RevertWriteMemo() : WorkerMemo(kRevertWriteFim) {}
  /**
   * The write request entry awaiting reversion of write before
   * declared completed
   */
  boost::shared_ptr<IReqEntry> req_entry;
  /**
   * The checksum update request made in response to the reply of the
   * request entry
   */
  FIM_PTR<ChecksumUpdateFim> checksum_update_req;
  /**
   * The client socket the original WriteFim is received from
   */
  boost::shared_ptr<IFimSocket> peer;
  /**
   * Whether the write is done in distress mode
   */
  bool distress;
};

// Called by the communication thread
void Worker::WriteReplCallback(const boost::shared_ptr<IReqEntry>& entry,
                               const FIM_PTR<ChecksumUpdateFim>& req,
                               const boost::shared_ptr<IFimSocket>& peer,
                               bool distress) {
  FIM_PTR<IFim> reply = entry->reply();
  bool defer = false;
  if (reply && reply->type() == kResultCodeReplyFim) {
    ResultCodeReplyFim& rreply = static_cast<ResultCodeReplyFim&>(*reply);
    if (rreply->err_no == ENOSPC) {
      FIM_PTR<RevertWriteFim> revert_fim = RevertWriteFim::MakePtr();
      (*revert_fim)->memo_id = GetMemoId();
      boost::shared_ptr<RevertWriteMemo> memo =
          boost::make_shared<RevertWriteMemo>();
      memo->req_entry = entry;
      memo->checksum_update_req = req;
      memo->peer = peer;
      memo->distress = distress;
      PutMemo((*revert_fim)->memo_id, memo);
      Enqueue(revert_fim, boost::shared_ptr<IFimSocket>());
      defer = true;
    }
  }
  if (!defer) {
    CompleteReplOp(entry, peer, (*req)->inode);
    if (distress) {
      FIM_PTR<DistressedWriteCompletionFim> completion_fim =
          DistressedWriteCompletionFim::MakePtr();
      (*completion_fim)->inode = (*req)->inode;
      (*completion_fim)->segment = (*req)->dsg_off / kSegmentSize;
      Enqueue(completion_fim, boost::shared_ptr<IFimSocket>());
    }
  }
}

bool Worker::HandleRevertWrite(const FIM_PTR<RevertWriteFim>& fim,
                               const boost::shared_ptr<IFimSocket>& peer) {
  (void) peer;  // Should always be empty
  boost::shared_ptr<WorkerMemo> memo = GetMemo((*fim)->memo_id);
  RevertWriteMemo* rw_memo = 0;
  if (memo)
    rw_memo = memo->CheckCast<RevertWriteMemo>(kRevertWriteFim);
  if (!rw_memo)
    return true;
  FIM_PTR<ChecksumUpdateFim> cu_fim = rw_memo->checksum_update_req;
  ds()->tracer()->Log(__func__, (*cu_fim)->inode, kNotClient);
  ds()->store()->ApplyDelta(
      (*cu_fim)->inode, (*cu_fim)->optime,
      (*cu_fim)->last_off, DsgToDataOffset((*cu_fim)->dsg_off),
      cu_fim->tail_buf(), cu_fim->tail_buf_size(), false);
  CompleteReplOp(rw_memo->req_entry, rw_memo->peer, (*cu_fim)->inode);
  if (rw_memo->distress)
    DoDistressedWriteComplete((*cu_fim)->inode,
                              (*cu_fim)->dsg_off / kSegmentSize);
  return true;
}

bool Worker::HandleDistressedWriteCompletion(
    const FIM_PTR<DistressedWriteCompletionFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  (void) peer;  // Always null
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  DoDistressedWriteComplete((*fim)->inode, (*fim)->segment);
  return true;
}

void Worker::DoDistressedWriteComplete(InodeNum inode, uint64_t segment) {
  if (resync_deferment_.ProcessCallbackDefer(
          inode, boost::bind(&Worker::DoDistressedWriteComplete,
                             this, inode, segment)))
    return;
  if (inode_defer_mgr_->IsLocked(inode)) {
    LOG(debug, Fim, "Deferring FIM processing while inode is being locked");
    inode_defer_released_[inode].connect(
        boost::bind(&Worker::DoDistressedWriteComplete, this, inode, segment));
    return;
  }
  boost::shared_ptr<IDegradedCacheHandle> empty_handle;
  for (;;) {
    if (!distress_defer_mgr_->SegmentHasFimEntry(inode, segment)) {
      distress_defer_mgr_->SetLocked(inode, segment, false);
      return;
    }
    DeferredFimEntry entry =
        distress_defer_mgr_->GetSegmentNextFimEntry(inode, segment);
    if (entry.fim()->type() == kReadFim) {
      FIM_PTR<ReadFim> fim = boost::static_pointer_cast<ReadFim>(entry.fim());
      DoRead(fim, entry.socket(), empty_handle);
      continue;
    }
    FIM_PTR<WriteFim> fim = boost::static_pointer_cast<WriteFim>(entry.fim());
    if (!DoWrite(fim, entry.socket(), empty_handle))
      return;
  }
}

bool Worker::HandleRead(const FIM_PTR<ReadFim>& fim,
                        const boost::shared_ptr<IFimSocket>& peer) {
  InodeNum inode = (*fim)->inode;
  uint64_t dsg_off = (*fim)->dsg_off;
  ds()->tracer()->Log(__func__, inode,
                      peer->GetReqTracker()->peer_client_num());
  boost::shared_ptr<IDegradedCacheHandle> cache_handle;
  if (DegradedModeCheck(fim, peer, (*fim)->target_role, (*fim)->checksum_role,
                        inode, dsg_off, &cache_handle))
    return true;
  if (distressed_ && !cache_handle) {
    uint64_t segment = dsg_off / kSegmentSize;
    if (distress_defer_mgr_->IsLocked(inode, segment)) {
      distress_defer_mgr_->AddFimEntry(inode, segment, fim, peer);
      return true;
    }
  }
  DoRead(fim, peer, cache_handle);
  return true;
}

void Worker::DoRead(const FIM_PTR<ReadFim>& fim,
                    const boost::shared_ptr<IFimSocket>& peer,
                    const boost::shared_ptr<IDegradedCacheHandle>&
                    cache_handle) {
  DSReplyOnExit replier(fim, peer);
  FIM_PTR<DataReplyFim> reply = DataReplyFim::MakePtr((*fim)->size);
  replier.SetNormalReply(reply);
  int result = 0;
  if (cache_handle) {
    LOG(debug, Degraded, "Completing degraded read ", PVal(fim));
    cache_handle->Read((*fim)->dsg_off % kSegmentSize, reply->tail_buf(),
                       reply->tail_buf_size());
  } else {
    result = ds()->store()->Read(
        (*fim)->inode, false, DsgToDataOffset((*fim)->dsg_off),
        reply->tail_buf(), reply->tail_buf_size());
  }
  replier.SetResult(result);
  if (result >= 0)
    ds()->cache_tracker()->SetCache((*fim)->inode,
                                    peer->GetReqTracker()->peer_client_num());
}

bool Worker::HandleDataRecovery(
    const FIM_PTR<DataRecoveryFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  FIM_PTR<RecoveryDataFim> data_fim = RecoveryDataFim::MakePtr(kSegmentSize);
  int result = ds()->store()->Read(
      (*fim)->inode, false, DsgToDataOffset((*fim)->dsg_off),
      data_fim->tail_buf(), kSegmentSize);
  (*data_fim)->inode = (*fim)->inode;
  (*data_fim)->dsg_off = (*fim)->dsg_off;
  (*data_fim)->result = result >= 0 ? 0 : -result;
  if (result == 0)
    data_fim->tail_buf_resize(0);
  peer->WriteMsg(data_fim);
  return true;
}

bool Worker::HandleRecoveryData(
    const FIM_PTR<RecoveryDataFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  InodeNum inode = (*fim)->inode;
  std::size_t cg_start = CgStart((*fim)->dsg_off);
  if ((*fim)->result != 0) {
    LOG(error, Degraded,
        "Error while collecting recovery data, errno: ", PINT((*fim)->result));
    ds()->data_recovery_mgr()->DropHandle(inode, cg_start);
    return true;
  }
  boost::shared_ptr<IDataRecoveryHandle> rec_handle =
      ds()->data_recovery_mgr()->GetHandle(inode, cg_start, false);
  if (!rec_handle) {
    LOG(informational, Degraded, "Obsolete recovery data ignored");
    return true;
  }
  char* data = fim->tail_buf_size() ? fim->tail_buf() : 0;
  LOG(debug, Degraded, "Recovery data: inode ", PINT(inode),
      ", segment ", PINT((*fim)->dsg_off));
  if (!rec_handle->AddData(data, peer))  // Some data missing
    return true;
  std::size_t cs_off = DsgToChecksumOffset(cg_start);
  char buf[kSegmentSize];
  int rsize = ds()->store()->Read(inode, true, cs_off, buf, kSegmentSize);
  if (rsize < 0) {
    LOG(error, Degraded,
        "Error while reading recovery data, errno: ", PINT(rsize));
    ds()->data_recovery_mgr()->DropHandle(inode, cg_start);
    return true;
  }
  bool recovered = rec_handle->RecoverData(buf);
  boost::shared_ptr<IDegradedCacheHandle> cache_handle =
      ds()->degraded_cache()->GetHandle(inode, cg_start);
  // The cache handle could be null when the degraded cache is inactive
  if (cache_handle) {
    if (recovered) {
      cache_handle->Allocate();
      std::memcpy(cache_handle->data(), buf, kSegmentSize);
    } else {
      cache_handle->Initialize();
    }
  }
  for (;;) {
    boost::shared_ptr<IFimSocket> q_peer;
    FIM_PTR<IFim> unqueued = rec_handle->UnqueueFim(&q_peer);
    if (!unqueued)
      break;
    try {
      Process_(unqueued, q_peer);
    } catch (const std::exception& ex) {
      LOG(error, Degraded, "Exception when handling deferred Fim ",
          PVal(unqueued), ": ", ex.what());
    }
  }
  ds()->data_recovery_mgr()->DropHandle(inode, cg_start);
  return true;
}

boost::shared_ptr<IDegradedCacheHandle> Worker::DoReqRecovery(
    const FIM_PTR<IFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer,
    InodeNum inode, std::size_t cg_start) {
  boost::shared_ptr<IDegradedCacheHandle> ret =
      ds()->degraded_cache()->GetHandle(inode, cg_start);
  if (ret->initialized())
    return ret;
  // Uninitialized, do recovery
  boost::shared_ptr<IDataRecoveryHandle> rec_handle =
      ds()->data_recovery_mgr()->GetHandle(inode, cg_start, true);
  if (!rec_handle->SetStarted()) {
    LOG(debug, Degraded, "Initiate data recovery");
    GroupId group = ds()->store()->ds_group();
    GroupRole r = GetRole();
    std::size_t recovery_off = cg_start;
    for (unsigned rdiff = 0; rdiff < kNumDSPerGroup - 1; ++rdiff) {
      ++r;
      if (r == kNumDSPerGroup)
        r = 0;
      if (!IsDSFailed(r, inode)) {
        FIM_PTR<DataRecoveryFim> recovery_fim = DataRecoveryFim::MakePtr();
        (*recovery_fim)->inode = inode;
        (*recovery_fim)->dsg_off = recovery_off;
        boost::shared_ptr<IFimSocket> target =
            ds()->tracker_mapper()->GetDSFimSocket(group, r);
        if (!target) {
          LOG(error, Degraded,
              "Connection to peer DS is dropped during data recovery");
          ds()->data_recovery_mgr()->DropHandle(inode, cg_start);
          return boost::shared_ptr<IDegradedCacheHandle>();
        }
        target->WriteMsg(recovery_fim);
      }
      recovery_off += kSegmentSize;
    }
  }
  rec_handle->QueueFim(fim, peer);
  return boost::shared_ptr<IDegradedCacheHandle>();
}

bool Worker::HandleChecksumUpdate(
    const FIM_PTR<ChecksumUpdateFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  std::size_t cg_start = CgStart((*fim)->dsg_off);
  LOG(debug, Degraded, "Checksum update ", PVal(fim));
  boost::shared_ptr<IDataRecoveryHandle> rec_handle =
      ds()->data_recovery_mgr()->GetHandle((*fim)->inode, cg_start, false);
  if (rec_handle && rec_handle->DataSent(peer)) {
    LOG(debug, Degraded, "Deferring checksum update ", PVal(fim));
    rec_handle->QueueFim(fim, peer);
    return true;  // Defer checksum processing until recovery completes
  }
  DSReplyOnExit replier(fim, peer);
  int result = ds()->store()->
      ApplyDelta((*fim)->inode, (*fim)->optime, (*fim)->last_off,
                 DsgToChecksumOffset((*fim)->dsg_off),
                 fim->tail_buf(), fim->tail_buf_size());
  replier.SetResult(result);
  return true;
}

/**
 * Get truncation parameters.
 *
 * @param inode The inode for the truncation
 *
 * @param role The group role
 *
 * @param dsg_off The DSG offset.  If size_ret is set to non-zero,
 * modified to hold the DSG offset of data_off_ret upon return
 *
 * @param data_off_ret Where to return the offset in data file
 *
 * @param checksum_off_ret Where to return the offset in checksum file
 *
 * @param size_ret Where to return the size after data_off_ret to retrieve
 */
void GetTruncateArgs(InodeNum inode, GroupRole role, std::size_t* dsg_off,
                     std::size_t* data_off_ret, std::size_t* checksum_off_ret,
                     std::size_t* size_ret) {
  std::size_t last_use = DsgToChecksumUsage(*dsg_off);
  std::size_t cg = *dsg_off / kChecksumGroupSize;
  *checksum_off_ret = RoleDsgToChecksumUsage(inode, role, *dsg_off);
  *data_off_ret = RoleDsgToDataOffset(inode, role, dsg_off);
  std::size_t cg2 = *dsg_off / kChecksumGroupSize;
  if (cg != cg2)  // New offset in different checksum group, don't send
    *size_ret = 0;
  else  // In same checksum group, update checksum up to last usage
    *size_ret = last_use - DsgToChecksumOffset(*dsg_off);
}

bool Worker::HandleTruncateData(
    const FIM_PTR<TruncateDataFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  GroupRole role = GetRole();
  if (role == (*fim)->checksum_role
      && (*fim)->target_role != (*fim)->checksum_role) {
    DegradedTruncateData(fim, peer);
    return true;
  }
  // This lock needs to be put before the replier, to guarantee that
  // the reply is sent before the reply to the replication is handled.
  // This prevent a possible case where final reply is sent before the
  // initial reply.
  boost::unique_lock<MUTEX_TYPE> repl_lock;
  boost::scoped_ptr<DSReplyOnExit> replier(new DSReplyOnExit(fim, peer));
  std::size_t dsg_off = (*fim)->dsg_off, data_off, cs_off, size;
  GetTruncateArgs((*fim)->inode, role, &dsg_off, &data_off, &cs_off, &size);
  FIM_PTR<ChecksumUpdateFim> update_fim = ChecksumUpdateFim::MakePtr(size);
  update_fim->set_req_id(fim->req_id());
  int result = ds()->store()->TruncateData(
      (*fim)->inode, (*fim)->optime, data_off, cs_off, size,
      update_fim->tail_buf());
  replier->SetResult(result);
  IDegradedCache* degraded_cache = ds()->degraded_cache();
  // Truncate cache only for non-checksum role: the checksum role
  // cache truncation needs to be done in DegradedTruncateData to
  // properly update the checksum.
  if (degraded_cache->IsActive()
      && (*fim)->target_role != (*fim)->checksum_role)
    degraded_cache->Truncate((*fim)->inode, (*fim)->dsg_off);
  if (result == 0) {
    cache_invalidator_.InvalidateClients((*fim)->inode, true, kNotClient);
    if (size == 0 || IsDSFailed((*fim)->checksum_role, (*fim)->inode))
      return true;
    (*update_fim)->inode = (*fim)->inode;
    (*update_fim)->optime = (*fim)->optime;
    (*update_fim)->dsg_off = dsg_off;
    (*update_fim)->last_off = (*fim)->last_off;
    boost::shared_ptr<IOpCompletionChecker> checker =
        ds()->op_completion_checker_set()->Get((*fim)->inode);
    boost::shared_ptr<IReqTracker> tracker = ds()->tracker_mapper()->
        GetDSTracker(ds()->store()->ds_group(), (*fim)->checksum_role);
    replier->SetReplicating();
    replier.reset();  // Reply first, replicate later to avoid race condition
    boost::shared_ptr<IReqEntry> entry =
        MakeReplReqEntry(tracker, update_fim);
    if (tracker->AddRequestEntry(entry, &repl_lock)) {
      entry->OnAck(boost::bind(&Worker::CompleteReplOp, this, _1, peer,
                               (*fim)->inode));
      checker->RegisterOp(entry.get());
    } else {
      SendFinalReply(fim->req_id(), peer);
    }
  }
  return true;
}

void Worker::DegradedTruncateData(
    const FIM_PTR<TruncateDataFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  boost::shared_ptr<IDegradedCacheHandle> cache_handle;
  std::size_t dsg_off = (*fim)->dsg_off, data_off, cs_off, size;
  GetTruncateArgs((*fim)->inode, (*fim)->target_role,
                  &dsg_off, &data_off, &cs_off, &size);
  if (size != 0)
    if (DegradedModeCheck(fim, peer, (*fim)->target_role, (*fim)->checksum_role,
                          (*fim)->inode, dsg_off, &cache_handle))
      return;
  // No need to modify others, just notify the truncate on cache and reply
  DSReplyOnExit replier(fim, peer);
  if (size) {
    char buf[kSegmentSize], cs_buf[kSegmentSize];
    std::memset(buf, '\0', size);
    cache_handle->Write(data_off % kSegmentSize, buf, size, cs_buf);
    ds()->store()->ApplyDelta((*fim)->inode, (*fim)->optime, (*fim)->last_off,
                              DsgToChecksumOffset(dsg_off), cs_buf, size);
  }
  replier.SetResult(0);
  ds()->degraded_cache()->Truncate((*fim)->inode, (*fim)->dsg_off);
  cache_invalidator_.InvalidateClients((*fim)->inode, true, kNotClient);
}

bool Worker::HandleMtimeUpdate(const FIM_PTR<MtimeUpdateFim>& fim,
                               const boost::shared_ptr<IFimSocket>& peer) {
  DSReplyOnExit replier(fim, peer);
  FIM_PTR<AttrUpdateReplyFim> reply = AttrUpdateReplyFim::MakePtr();
  replier.SetNormalReply(reply);
  (*reply)->mtime = (*fim)->mtime;
  replier.SetResult(ds()->store()->UpdateMtime((*fim)->inode, (*fim)->mtime,
                                               &(*reply)->size));
  return true;
}

bool Worker::HandleFreeData(
    const FIM_PTR<FreeDataFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  DSReplyOnExit replier(fim, peer);
  ds()->degraded_cache()->FreeInode((*fim)->inode);
  replier.SetResult(ds()->store()->FreeData((*fim)->inode, false));
  ds()->cleaner()->RemoveInodeLater((*fim)->inode);
  return true;
}

bool Worker::HandleAttrUpdateFim(const FIM_PTR<AttrUpdateFim>& fim,
                                 const boost::shared_ptr<IFimSocket>& peer) {
  ds()->tracer()->Log(__func__, (*fim)->inode, kNotClient);
  DSReplyOnExit replier(fim, peer);
  FIM_PTR<AttrUpdateReplyFim> reply = AttrUpdateReplyFim::MakePtr();
  replier.SetNormalReply(reply);
  (*reply)->mtime.sec = 0;
  (*reply)->mtime.ns = 0;
  (*reply)->size = 0;
  uint64_t state_change_id;
  GroupRole failed_role;
  DSGroupState curr_state = ds()->dsg_state(&state_change_id, &failed_role);
  bool resyncing = curr_state == kDSGResync && failed_role == GetRole() &&
      ds()->is_inode_to_resync((*fim)->inode);
  if (curr_state == kDSGReady || curr_state == kDSGDegraded ||
      (curr_state == kDSGResync && !resyncing)) {
    // Ignore error -- rely on the behavior that the mtime and size is not
    // changed in case of error
    ds()->store()->GetAttr((*fim)->inode, &(*reply)->mtime, &(*reply)->size);
    replier.SetResult(0);
  }
  return true;
}

}  // namespace

IWorker* MakeWorker() {
  return new Worker();
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
