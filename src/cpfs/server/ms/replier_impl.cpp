/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IReplier.
 */

#include "server/ms/replier_impl.hpp"

#include <stdint.h>

#include <algorithm>
#include <list>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
// IWYU pragma: no_forward_declare boost::function
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "timed_list.hpp"
#include "tracker_mapper.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/inode_mutex.hpp"
#include "server/ms/replier.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/reply_set.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implement the IReplier interface.
 *
 * If an inode I is read when processing a request R1, and a change
 * request R2 to I is currently being replicated in an incomplete
 * replication P2, where the change is made either by another FC or is
 * made by the same FC but the reply is not yet sent, the reply to R1
 * must wait until P2 is completed (i.e., reply received or standby MS
 * disconnected).  This prevent the FC from depending on the
 * successful execution and completion of R2, which might not be the
 * case if the active MS fails (R2 might not succeed at all, or may be
 * reconfirmed later than R1).
 *
 * Note that if P2 is changed by the same FC as the peer requesting
 * R1, and R2 has already been replied to the FC, we can proceed in
 * replying R1 (if no other request is stopping it).  This is called
 * the same-peer optimization.
 *
 * All incomplete replications (like P2) are kept in a pending_repls
 * field, which records the request number of the corresponding
 * request (R2), the FimSocket to send the reply to, and a list of
 * pending replies (like reply to R1) and peers that should be sent
 * when the replication completes.  The request number is a
 * monotonically increasing number used to find know which replication
 * is the latest, so that we can add a reply to the right
 * pending_repls entry.
 *
 * Each inode (like I) changed by a request with pending replication
 * (R2) has an entry in the pending_inodes field.  This records the
 * last request replication that has changed the inode, as well as a
 * peer that the same-peer optimization can be applied (may be empty
 * if no such peer exists, e.g., R2 is not replied).
 */
class Replier : public IReplier {
 public:
  /**
   * @param server The meta server using the replier
   */
  explicit Replier(BaseMetaServer* server)
      : server_(server), data_mutex_(MUTEX_INIT), next_repl_num_(1) {}

  void DoReply(const FIM_PTR<IFim>& req,
               const FIM_PTR<IFim>& reply,
               const boost::shared_ptr<IFimSocket>& peer,
               const OpContext* op_context,
               ReplierCallback active_complete_callback);

  void ExpireCallbacks(int age) {
    repl_callback_list_.Expire(age);
  }

  void RedoCallbacks() {
    BOOST_FOREACH(ReplierCallback cb, repl_callback_list_.FetchAndClear()) {
      cb();
    }
  }

 private:
  BaseMetaServer* server_; /**< The server to find related components */
  MUTEX_TYPE data_mutex_; /**< Protect all fields below */
  uint64_t next_repl_num_; /**< Next replication number */

  typedef std::pair<FIM_PTR<IFim>,
                    boost::shared_ptr<IFimSocket> > PendingReqEntry;
  typedef std::list<PendingReqEntry> PendingReplyList;
  /**
   * An entry recording a replication in progress.
   */
  struct PendingReplEntry {
    uint64_t repl_num; /**< Replication number */
    boost::shared_ptr<IFimSocket> peer; /**< Peer to send final reply to */
    std::vector<InodeNum> inodes_changed; /**< List of inodes changed */
    /** Replies to send when this replication is completed */
    PendingReplyList replies;
    /** Callback to run when the replication is replied */
    ReplierCallback complete_callback;
  };

  /**
   * Pending repl entries.
   */
  boost::unordered_map<ReqId, PendingReplEntry> pending_repls_;

  /**
   * Recent callbacks to run when replication replied
   */
  TimedList<ReplierCallback> repl_callback_list_;

  /**
   * Inode number to (request ID, peer)
   */
  typedef std::pair<ReqId, boost::shared_ptr<IFimSocket> > PendingInodeEntry;
  typedef boost::unordered_map<InodeNum, PendingInodeEntry> PendingInodeList;
  PendingInodeList pending_inodes_;

  void CheckReplReply_(const FIM_PTR<IFim>& req,
                       const FIM_PTR<IFim>& reply);

  void HandleReplComplete(const boost::shared_ptr<IReqEntry>& req_entry);
  void HandleReplComplete_(const boost::shared_ptr<IReqEntry>& req_entry);

  /**
   * DoReply() redirects to this function if it finds that the current
   * MS configuration has replication.
   *
   * @return Whether an actual replication is pending.  If not, the
   * complete_callback is also not called yet, and the caller should
   * arrange to call it manually
   */
  bool DoReplyWithRepl_(const FIM_PTR<IFim>& req,
                        const FIM_PTR<IFim>& reply,
                        const boost::shared_ptr<IFimSocket>& peer,
                        const OpContext* op_context,
                        ReplierCallback complete_callback);

  /**
   * Manage the information needed to reply at the right time.
   */
  class ReplyManager {
   public:
    /**
     * @param replier The replier using the manager
     *
     * @param req_id The request ID of the request replied
     *
     * @param repl_entry The replication entry of the request being
     * replied
     *
     * @param peer The peer sending the request to be replied
     */
    ReplyManager(Replier* replier, ReqId req_id,
                 PendingReplEntry* repl_entry,
                 const boost::shared_ptr<IFimSocket>& peer)
        : replier_(replier), req_id_(req_id), repl_entry_(repl_entry),
          peer_(peer), last_repl_num_(0), replied_(false) {}

    /**
     * Process an inode being read when fulfilling the current
     * request.  It should be called before calling ArrangeReply().
     *
     * @param inode_read An inode being read when serving the request
     */
    void AddInodeRead(InodeNum inode_read) {
      if (std::find(processed_read_.begin(), processed_read_.end(),
                    inode_read) != processed_read_.end())
        return;
      PendingInodeList::iterator it =
          replier_->pending_inodes_.find(inode_read);
      if (it == replier_->pending_inodes_.end())
        return;
      PendingInodeEntry& inode_entry = it->second;
      if (inode_entry.second == peer_)
        return;
      ReqId pending_req = inode_entry.first;
      uint64_t repl_num = replier_->pending_repls_[pending_req].repl_num;
      if (repl_num > last_repl_num_) {
        last_repl_req_ = pending_req;
        last_repl_num_ = repl_num;
      }
      processed_read_.push_back(inode_read);
    }

    /**
     * Arrange to make the reply a the right moment: immediately if it
     * is possible, and at the time when a reply to a previous
     * replication is completed if not.
     *
     * @param reply The reply to make
     */
    void ArrangeReply(const FIM_PTR<IFim>& reply) {
      if (last_repl_num_ == 0) {
        peer_->WriteMsg(reply);
        replied_ = true;
      } else {
        replier_->pending_repls_[last_repl_req_].replies.
            push_back(std::make_pair(reply, peer_));
      }
    }

    /**
     * Process an inode being written when fulfilling the current
     * request.  It should be called after calling ArrangeReply().
     *
     * @param inode_changed An inode being changed when serving the
     * request
     */
    void AddInodeChanged(InodeNum inode_changed) {
      if (std::find(processed_change_.begin(), processed_change_.end(),
                    inode_changed) != processed_change_.end())
        return;
      PendingInodeList::iterator it =
          replier_->pending_inodes_.find(inode_changed);
      repl_entry_->inodes_changed.push_back(inode_changed);
      PendingInodeEntry* inode_entry;
      bool new_entry = false;
      if (it == replier_->pending_inodes_.end()) {
        new_entry = true;
        inode_entry = &replier_->pending_inodes_[inode_changed];
      } else {
        inode_entry = &it->second;
      }
      inode_entry->first = req_id_;
      if (new_entry) {
        if (replied_)
          inode_entry->second = peer_;
      } else {
        if (inode_entry->second != peer_)
          inode_entry->second.reset();
      }
      processed_change_.push_back(inode_changed);
    }

   private:
    Replier* replier_;
    ReqId req_id_;
    PendingReplEntry* repl_entry_;
    boost::shared_ptr<IFimSocket> peer_;
    ReqId last_repl_req_;
    uint64_t last_repl_num_;
    std::vector<InodeNum> processed_read_;
    bool replied_;
    std::vector<InodeNum> processed_change_;
  };
};

void Replier::DoReply(const FIM_PTR<IFim>& req,
                      const FIM_PTR<IFim>& reply,
                      const boost::shared_ptr<IFimSocket>& peer,
                      const OpContext* op_context,
                      ReplierCallback active_complete_callback) {
  reply->set_req_id(req->req_id());
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    bool is_replicate_req =
        peer->GetReqTracker()->peer_client_num() == kNotClient;
    boost::shared_ptr<IFimSocket> ms_fim_socket =
        server_->tracker_mapper()->GetMSFimSocket();
    if (active_complete_callback)
      repl_callback_list_.Add(active_complete_callback);
    if (ms_fim_socket && (server_->state_mgr()->GetState() == kStateActive)) {
      if (DoReplyWithRepl_(req, reply, peer, op_context,
                           active_complete_callback))
        active_complete_callback = ReplierCallback();
    } else {
      if (is_replicate_req) {
        server_->recent_reply_set()->AddReply(reply);
        CheckReplReply_(req, reply);
        active_complete_callback = ReplierCallback();
      } else {
        reply->set_final();
      }
      peer->WriteMsg(reply);
    }
  }
  if (active_complete_callback)
    active_complete_callback();
}

void Replier::CheckReplReply_(const FIM_PTR<IFim>& req,
                              const FIM_PTR<IFim>& reply) {
  if (reply->type() == kResultCodeReplyFim) {
    ResultCodeReplyFim& rreply = static_cast<ResultCodeReplyFim&>(*reply);
    if (int err_no = rreply->err_no)
      LOG(error, Replicate, "Error for replicate req ", PVal(req),
          ", err_no = ", PINT(err_no));
  }
}

void Replier::HandleReplComplete(
    const boost::shared_ptr<IReqEntry>& req_entry) {
  boost::function<void(void)> callback;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    ReqId req_id = req_entry->req_id();
    PendingReplEntry& entry = pending_repls_[req_id];
    FIM_PTR<FinalReplyFim> final_reply = FinalReplyFim::MakePtr();
    final_reply->set_req_id(req_id);
    final_reply->set_final();
    (*final_reply)->err_no = 0;
    if (req_entry->reply() && req_entry->reply()->type()
        == kResultCodeReplyFim) {
      ResultCodeReplyFim& rreply = static_cast<ResultCodeReplyFim&>(
          *req_entry->reply());
      (*final_reply)->err_no = rreply->err_no;
    }
    entry.peer->WriteMsg(final_reply);
    for (PendingReplyList::iterator it = entry.replies.begin();
         it != entry.replies.end();
         ++it)
      it->second->WriteMsg(it->first);
    for (unsigned i = 0; i < entry.inodes_changed.size(); ++i) {
      InodeNum inode = entry.inodes_changed[i];
      if (pending_inodes_[inode].first == req_id)
        pending_inodes_.erase(inode);
    }
    callback = entry.complete_callback;
    pending_repls_.erase(req_id);
  }
  if (callback)
    callback();
}

bool Replier::DoReplyWithRepl_(const FIM_PTR<IFim>& req,
                               const FIM_PTR<IFim>& reply,
                               const boost::shared_ptr<IFimSocket>& peer,
                               const OpContext* op_context,
                               ReplierCallback complete_callback) {
  ReqId req_id = req->req_id();
  PendingReplEntry* entry = 0;
  bool replicating = false;
  // Keep mutex until this function completes.  This prevents the
  // reply to the replication to be processed until we are done,
  // preventing a race that may cause final reply to be sent before
  // the initial reply to be sent here.  If this function completes
  // (and the initial reply is deferred), the ordering is guaranteed
  // because only the MS communication thread will handle replication
  // replies which triggers sending the initial reply.
  boost::unique_lock<MUTEX_TYPE> repl_lock;
  if (op_context->inodes_changed.size() > 0) {
    boost::shared_ptr<IReqTracker> ms_tracker =
        server_->tracker_mapper()->GetMSTracker();
    LOG(debug, Replicate, "Replicate req ", PVal(req));
    entry = &pending_repls_[req_id];
    entry->repl_num = next_repl_num_++;
    entry->peer = peer;
    entry->complete_callback = complete_callback;
    boost::shared_ptr<IReqEntry> repl_entry = MakeReplReqEntry(ms_tracker, req);
    if (ms_tracker->AddRequestEntry(repl_entry, &repl_lock)) {
      replicating = true;
      repl_entry->OnAck(boost::bind(&Replier::HandleReplComplete, this, _1));
    } else {  // Late discovery of replication failure
      reply->set_final();
    }
  } else {
    reply->set_final();
  }
  ReplyManager reply_mgr(this, req_id, entry, peer);
  for (std::vector<InodeNum>::const_iterator
           it = op_context->inodes_read.begin();
       it != op_context->inodes_read.end();
       ++it)
    reply_mgr.AddInodeRead(*it);
  for (std::vector<InodeNum>::const_iterator
           it = op_context->inodes_changed.begin();
       it != op_context->inodes_changed.end();
       ++it)
    reply_mgr.AddInodeRead(*it);
  reply_mgr.ArrangeReply(reply);
  for (std::vector<InodeNum>::const_iterator
           it = op_context->inodes_changed.begin();
       it != op_context->inodes_changed.end();
       ++it)
    reply_mgr.AddInodeChanged(*it);
  return replicating;
}

}  // namespace

IReplier* MakeReplier(BaseMetaServer* server) {
  return new Replier(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
