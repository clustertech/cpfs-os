/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement the IWorker interface for the MS.
 */

#include "server/ms/worker_impl.hpp"

#include <fcntl.h>
#include <stdint.h>

#include <fuse/fuse_lowlevel.h>

#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
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
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "tracer.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "server/ccache_tracker.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/dirty_inode.hpp"
#include "server/ms/ds_locker.hpp"
#include "server/ms/dsg_op_state.hpp"
#include "server/ms/failover_mgr.hpp"
#include "server/ms/inode_mutex.hpp"
#include "server/ms/inode_src.hpp"
#include "server/ms/inode_usage.hpp"
#include "server/ms/replier.hpp"
#include "server/ms/startup_mgr.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/store.hpp"
#include "server/ms/topology.hpp"
#include "server/reply_set.hpp"
#include "server/worker_base.hpp"
#include "server/worker_util.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Make an appropriate reply on exit of scope.  To make a successful
 * reply, clients normally either call SetNormalReply to set the
 * reply, or make a call SetResult with a non-negative value.  Any
 * call to SetResult with a negative value will cause the reply to be
 * replaced by a ResultCodeReplyFim with err_no being the negation of
 * the last negative value set.  If neither SetNormalReply nor
 * SetResult is called, the a ResultCodeReplyFim with err_no of EIO
 * will be replied.  This class handle meta-server replication as
 * well, using the meta-replier supplied on construction.  This means
 * that the actual reply is sent only when it is permitted by the
 * meta-replier, which also set the final flag appropriately.
 */
class ReplReplyOnExit : public BaseWorkerReplier {
 public:
  /**
   * @param req The request Fim
   *
   * @param peer The peer sending the request
   *
   * @param replier The meta server replier to use
   *
   * @param op_context The operation context
   */
  explicit ReplReplyOnExit(const FIM_PTR<IFim>& req,
                           const boost::shared_ptr<IFimSocket>& peer,
                           IReplier* replier,
                           const OpContext* op_context)
      : BaseWorkerReplier(req, peer),
        replier_(replier), op_context_(op_context) {}

  ~ReplReplyOnExit() {
    MakeReply();
  }

  /**
   * Completion handler.
   */
  virtual void ReplyComplete() {
    replier_->DoReply(req_, reply_, peer_, op_context_, active_complete_cb_);
  }

  /**
   * Set active completion callback function.
   *
   * @param active_complete_cb The new active completion callback
   * function
   */
  void set_active_complete_cb(boost::function<void()> active_complete_cb) {
    active_complete_cb_ = active_complete_cb;
  }

 private:
  IReplier* replier_;
  const OpContext* op_context_;
  boost::function<void()> active_complete_cb_;
};

/**
 * Implement the IFimProcessor interface for meta data server.
 */
class Worker : public BaseWorker, private MemberFimProcessor<Worker> {
  // Allow casting from MemberFimProcessor<Worker> to Worker
  // within template MemberFimProcessor
  friend class MemberFimProcessor<Worker>;

 public:
  Worker();

  /**
   * Check whether the specified fim is accepted by this worker
   */
  bool Accept(const FIM_PTR<IFim>& fim) const;

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
               const boost::shared_ptr<IFimSocket>& socket);

 private:
  // Generic inode handling
  bool HandleGetattr(const FIM_PTR<GetattrFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer);
  bool HandleSetattr(const FIM_PTR<SetattrFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer);
  bool HandleSetxattr(const FIM_PTR<SetxattrFim>& fim,
                      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleGetxattr(const FIM_PTR<GetxattrFim>& fim,
                      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleListxattr(const FIM_PTR<ListxattrFim>& fim,
                       const boost::shared_ptr<IFimSocket>& peer);
  bool HandleRemovexattr(const FIM_PTR<RemovexattrFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer);

  // Non-directory inode handling
  bool HandleOpen(const FIM_PTR<OpenFim>& fim,
                  const boost::shared_ptr<IFimSocket>& peer);
  bool HandleAccess(const FIM_PTR<AccessFim>& fim,
                    const boost::shared_ptr<IFimSocket>& peer);
  bool HandleAdviseWrite(const FIM_PTR<AdviseWriteFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer);
  bool HandleRelease(const FIM_PTR<ReleaseFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer);
  bool HandleAttrUpdateCompletionFim(
      const FIM_PTR<AttrUpdateCompletionFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleReadlink(const FIM_PTR<ReadlinkFim>& fim,
                      const boost::shared_ptr<IFimSocket>& peer);

  // Read directories
  bool HandleLookup(const FIM_PTR<LookupFim>& fim,
                    const boost::shared_ptr<IFimSocket>& peer);
  bool HandleOpendir(const FIM_PTR<OpendirFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer);
  bool HandleReaddir(const FIM_PTR<ReaddirFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer);

  // Create directory entries
  bool HandleCreate(const FIM_PTR<CreateFim>& fim,
                    const boost::shared_ptr<IFimSocket>& peer);
  bool HandleMkdir(const FIM_PTR<MkdirFim>& fim,
                   const boost::shared_ptr<IFimSocket>& peer);
  bool HandleSymlink(const FIM_PTR<SymlinkFim>& fim,
                     const boost::shared_ptr<IFimSocket>& peer);
  bool HandleMknod(const FIM_PTR<MknodFim>& fim,
                   const boost::shared_ptr<IFimSocket>& peer);

  // Manipulate directory entries
  bool HandleLink(const FIM_PTR<LinkFim>& fim,
                  const boost::shared_ptr<IFimSocket>& peer);
  bool HandleRename(const FIM_PTR<RenameFim>& fim,
                    const boost::shared_ptr<IFimSocket>& peer);

  // Remove directory entries
  bool HandleUnlink(const FIM_PTR<UnlinkFim>& fim,
                    const boost::shared_ptr<IFimSocket>& peer);
  bool HandleRmdir(const FIM_PTR<RmdirFim>& fim,
                   const boost::shared_ptr<IFimSocket>& peer);

  // Failover
  bool HandleReconfirmEndFim(const FIM_PTR<ReconfirmEndFim>& fim,
                             const boost::shared_ptr<IFimSocket>& peer);

  // Resync
  bool HandleResyncInode(const FIM_PTR<ResyncInodeFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer);
  bool HandleResyncDentry(const FIM_PTR<ResyncDentryFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer);
  bool HandleResyncRemoval(const FIM_PTR<ResyncRemovalFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer);
  bool HandleResyncInodeUsagePrep(
      const FIM_PTR<ResyncInodeUsagePrepFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleResyncDirtyInodeFim(
      const FIM_PTR<ResyncDirtyInodeFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleResyncPendingUnlink(
      const FIM_PTR<ResyncPendingUnlinkFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleResyncClientOpened(
      const FIM_PTR<ResyncClientOpenedFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleResyncXattr(
      const FIM_PTR<ResyncXattrFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);
  bool HandleInodeCleanAttr(
      const FIM_PTR<InodeCleanAttrFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer);

  /**
   * @return Meta server using the worker
   */
  BaseMetaServer* ms() {
    return static_cast<BaseMetaServer*>(server());
  }

  /**
   * Call the DSs to truncate their data files and set the mtime for
   * data files corresponding to an inode.
   *
   * @param optime The mtime to set
   *
   * @param inode The inode
   *
   * @param group_ids The group IDs of the DSs allocated for the file
   *
   * @param size The target size of the file
   */
  void DSTruncate(const FSTime& optime, InodeNum inode,
                  const std::vector<GroupId>& group_ids, uint64_t size);
  /**
   * Call the DSs to set the mtime for data files corresponding to an
   * inode.
   *
   * @param optime The mtime to set
   *
   * @param inode The inode
   *
   * @param group_ids The group IDs of the DSs allocated for the file
   *
   * @param size_ret Where to return the updated size
   */
  void DSMtime(const FSTime& optime, InodeNum inode,
               const std::vector<GroupId>& group_ids,
               uint64_t* size_ret);

  /**
   * Call the DSs to get the most updated mtime and file size info
   * synchronously.
   *
   * @param inode The inode to get information for
   *
   * @param reply The reply to update
   */
  void UpdateAttrReplySync(InodeNum inode, const FIM_PTR<AttrReplyFim>& reply);

  /**
   * Call the DSs to free their data files corresponding to an inode.
   *
   * @param inode The inode
   *
   * @param group_ids The group IDs of the DSs allocated for the file
   */
  void DSFreeInode(
      InodeNum inode,
      const boost::shared_ptr<const std::vector<GroupId> >& group_ids);

  /**
   * Set the file server group information in a reply Fim tail buffer.
   *
   * @param reply The reply.
   *
   * @param groups The server group information.
   */
  static void SetReplyGroups(const FIM_PTR<IFim>& reply,
                             const std::vector<GroupId>& groups) {
    reply->tail_buf_resize(groups.size() * sizeof(GroupId));
    GroupId* buf = reinterpret_cast<GroupId*>(reply->tail_buf());
    for (unsigned i = 0; i < groups.size(); ++i)
      buf[i] = groups[i];
  }

  /**
   * Get group list from the end of a Fim.  Such list is appended by
   * the active MS before sending to the standby MS, and the FC after
   * receiving the reply by the active MS.  As such it is always at
   * the end of the Fim.
   *
   * @param num_ds_groups The number of DS groups to get
   *
   * @param fim The fim to get the group list from
   *
   * @param groups Where to put the group list to.  It should be empty
   * before the call
   */
  void GetGroupsFromFim(int num_ds_groups, const FIM_PTR<IFim>& fim,
                        std::vector<GroupId>* groups) {
    if (num_ds_groups == 0)
      return;
    groups->resize(num_ds_groups);
    int num_bytes = groups->size() * sizeof(GroupId);
    std::memcpy(&(*groups)[0],
                fim->tail_buf() + fim->tail_buf_size() - num_bytes,
                num_bytes);
  }

  /**
   * Set group list to the end of a Fim.
   *
   * @param fim The fim to get the group list from
   *
   * @param groups Where to put the group list to.  It should be empty
   * before the call
   */
  void AppendFimReqGroups(const FIM_PTR<IFim>& fim,
                          const std::vector<GroupId>& groups) {
    int orig_size = fim->tail_buf_size();
    fim->tail_buf_resize(orig_size + groups.size() * sizeof(GroupId));
    std::memcpy(fim->tail_buf() + orig_size, groups.data(),
                groups.size() * sizeof(GroupId));
  }

  /**
   * Get inodes from the end of a Fim
   *
   * @param fim The fim to get the inodes from
   *
   * @parma inodes_ret Where to put the inodes to
   */
  void GetInodesFromFim(const IFim& fim,
                        boost::unordered_set<InodeNum>* inodes_ret) {
    const InodeNum* inodes = reinterpret_cast<const InodeNum*>(fim.tail_buf());
    std::size_t num_inodes = fim.tail_buf_size() / sizeof(InodeNum);
    boost::unordered_set<InodeNum> ret(inodes, inodes + num_inodes);
    inodes_ret->swap(ret);
  }

  /**
   * Reply peer sending Getattr request when updates for dirty
   * attribute is received.  This is only used if dirty attribute
   * update is found to be necessary.
   *
   * @param fim The Getattr request
   *
   * @param peer The peer sending the request
   *
   * @param context The operation context of the
   *
   * @param reply The reply prepared.  All the information will be
   * used except the mtime and the size
   *
   * @param mtime The updated mtime
   *
   * @param size The updated file size
   */
  void ReplyDeferredGetattr(const FIM_PTR<GetattrFim>& fim,
                            const boost::shared_ptr<IFimSocket>& peer,
                            boost::shared_ptr<OpContext> context,
                            FIM_PTR<AttrReplyFim> reply,
                            FSTime mtime, uint64_t size);

  /**
   * Reply peer sending Lookup request when updates for dirty
   * attribute is received.  This is only used if dirty attribute
   * update is found to be necessary.
   *
   * @param fim The Getattr request
   *
   * @param peer The peer sending the request
   *
   * @param context The operation context of the
   *
   * @param reply The reply prepared.  All the information will be
   * used except the mtime and the size
   *
   * @param mtime The updated mtime
   *
   * @param size The updated file size
   */
  void ReplyDeferredLookup(const FIM_PTR<LookupFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer,
                           boost::shared_ptr<OpContext> context,
                           FIM_PTR<AttrReplyFim> reply,
                           FSTime mtime, uint64_t size);

  /**
   * Prepare for cleaning up dirty inode attributes.
   *
   * @param inode The inode to clean up
   *
   * @param gen The generation of the attributes when update is triggered
   */
  void PrepareAttrCleanup(InodeNum inode, uint64_t gen);

  /**
   * Handle dirty inode attributes cleanup completion.  This is called
   * in the communication thread, and will arrange handling of the
   * cleanup in the worker thread.
   *
   * @param inode The inode updated
   *
   * @param gen The generation of the attributes when update is triggered
   *
   * @param mtime The updated mtime
   *
   * @param size The updated file size
   */
  void CompleteAttrCleanup(InodeNum inode, uint64_t gen,
                           FSTime mtime, uint64_t size);
};

Worker::Worker() {
  AddHandler(&Worker::HandleGetattr);
  AddHandler(&Worker::HandleSetattr);
  AddHandler(&Worker::HandleSetxattr);
  AddHandler(&Worker::HandleGetxattr);
  AddHandler(&Worker::HandleListxattr);
  AddHandler(&Worker::HandleRemovexattr);
  AddHandler(&Worker::HandleOpen);
  AddHandler(&Worker::HandleAccess);
  AddHandler(&Worker::HandleAdviseWrite);
  AddHandler(&Worker::HandleRelease);
  AddHandler(&Worker::HandleAttrUpdateCompletionFim);
  AddHandler(&Worker::HandleReadlink);
  AddHandler(&Worker::HandleLookup);
  AddHandler(&Worker::HandleOpendir);
  AddHandler(&Worker::HandleReaddir);
  AddHandler(&Worker::HandleCreate);
  AddHandler(&Worker::HandleMkdir);
  AddHandler(&Worker::HandleSymlink);
  AddHandler(&Worker::HandleMknod);
  AddHandler(&Worker::HandleLink);
  AddHandler(&Worker::HandleRename);
  AddHandler(&Worker::HandleUnlink);
  AddHandler(&Worker::HandleRmdir);
  AddHandler(&Worker::HandleReconfirmEndFim);
  AddHandler(&Worker::HandleResyncInode);
  AddHandler(&Worker::HandleResyncDentry);
  AddHandler(&Worker::HandleResyncRemoval);
  AddHandler(&Worker::HandleResyncInodeUsagePrep);
  AddHandler(&Worker::HandleResyncDirtyInodeFim);
  AddHandler(&Worker::HandleResyncPendingUnlink);
  AddHandler(&Worker::HandleResyncClientOpened);
  AddHandler(&Worker::HandleInodeCleanAttr);
  AddHandler(&Worker::HandleResyncXattr);
}

bool Worker::Accept(const FIM_PTR<IFim>& fim) const {
  return MemberFimProcessor<Worker>::Accept(fim);
}

bool Worker::Process(const FIM_PTR<IFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
  FIM_PTR<IFim> reply = ms()->recent_reply_set()->FindReply(fim->req_id());
  if (reply) {
    reply->set_final();
    socket->WriteMsg(reply);
    return true;
  }
  return MemberFimProcessor<Worker>::Process(fim, socket);
}

// Generic inode handling

bool Worker::HandleGetattr(const FIM_PTR<GetattrFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum peer_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, peer_num);
  boost::shared_ptr<OpContext> context = boost::make_shared<OpContext>();
  ReplReplyOnExit replier(fim, peer, ms()->replier(), context.get());
  context->req_context = &(*fim)->context;
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  InodeNum inode = (*reply)->inode = (*fim)->inode;
  int err_no = ms()->store()->
      Attr(context.get(), inode, 0, 0, true, &(*reply)->fa);
  if (err_no == 0 && ms()->dirty_inode_mgr()->IsVolatile(inode)) {
    context->i_mutex_guard.reset();  // Allow other operations to proceed
    ms()->attr_updater()->AsyncUpdateAttr(
        inode, (*reply)->fa.mtime, (*reply)->fa.size,
        boost::bind(&Worker::ReplyDeferredGetattr, this,
                    fim, peer, context, reply, _1, _2));
    replier.SetEnabled(false);
    return true;
  }
  (*reply)->dirty = false;
  replier.SetNormalReply(reply);
  replier.SetResult(err_no);
  if (err_no == 0)
    ms()->cache_tracker()->SetCache(inode, peer_num);
  return true;
}

void Worker::ReplyDeferredGetattr(const FIM_PTR<GetattrFim>& fim,
                                  const boost::shared_ptr<IFimSocket>& peer,
                                  boost::shared_ptr<OpContext> context,
                                  FIM_PTR<AttrReplyFim> reply,
                                  FSTime mtime, uint64_t size) {
  ReplReplyOnExit replier(fim, peer, ms()->replier(), context.get());
  replier.SetNormalReply(reply);
  (*reply)->fa.mtime = mtime;
  (*reply)->fa.size = size;
  (*reply)->dirty = true;
  replier.SetResult(0);
}

bool Worker::HandleSetattr(const FIM_PTR<SetattrFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum peer_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, peer_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  InodeNum inode = (*reply)->inode = (*fim)->inode;
  replier.SetNormalReply(reply);
  int err_no;
  // If replicating, treat lock as already acquired since we (the standby)
  // are not responsible for the DS communication necessitating it.
  bool lock_acquired = peer_num == kNotClient;
  uint64_t new_size = 0;
  try {
    err_no = ms()->store()->
        Attr(&context, inode, &(*fim)->fa, (*fim)->fa_mask,
             lock_acquired, &(*reply)->fa);
  } catch (const MDNeedLock& e) {
    std::vector<GroupId> group_ids;
    ms()->store()->GetFileGroupIds(inode, &group_ids);
    IDSLocker* ds_locker = ms()->ds_locker();
    lock_acquired = true;
    std::vector<boost::shared_ptr<IMetaDSLock> > locks;
    if (!(*fim)->locked || !ms()->inode_usage()->IsSoleWriter(inode, peer_num))
      ds_locker->Lock(inode, group_ids, &locks);
    err_no = ms()->store()->Attr(&context, inode, &(*fim)->fa,
                                 (*fim)->fa_mask, true, &(*reply)->fa);
    if ((*fim)->fa_mask & FUSE_SET_ATTR_SIZE) {
      DSTruncate(context.req_context->optime, inode, group_ids,
                 (*fim)->fa.size);
    } else {
      DSMtime((*fim)->fa.mtime, inode, group_ids, &new_size);
    }
  }
  replier.SetResult(err_no);
  if (err_no == 0) {
    (*reply)->dirty = ms()->dirty_inode_mgr()->IsVolatile((*fim)->inode);
    if (peer_num == kNotClient) {  // Slave just copy master mtime/size
      (*reply)->fa.size = (*fim)->fa.size;
      (*reply)->fa.mtime = (*fim)->fa.mtime;
    } else {
      if ((*reply)->dirty) {
        if (lock_acquired)
          (*reply)->fa.size = std::max((*reply)->fa.size, new_size);
        else
          UpdateAttrReplySync(inode, reply);
      }
      // Send size and mtime to slave upon replication
      (*fim)->fa.size = (*reply)->fa.size;
      (*fim)->fa.mtime = (*reply)->fa.mtime;
      if (!(*reply)->dirty)
        ms()->cache_tracker()->SetCache(inode, peer_num);
      cache_invalidator_.InvalidateClients(inode, false, peer_num);
    }
  }
  return true;
}

bool Worker::HandleSetxattr(const FIM_PTR<SetxattrFim>& fim,
                            const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum peer_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, peer_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  int err_no = ms()->store()->SetXattr(&context, (*fim)->inode,
      fim->tail_buf(), fim->tail_buf() + (*fim)->name_len, (*fim)->value_len,
      (*fim)->flags);
  replier.SetResult(err_no);
  if (err_no == 0) {
    cache_invalidator_.InvalidateClients((*fim)->inode, false, peer_num);
  }
  return true;
}

bool Worker::HandleGetxattr(const FIM_PTR<GetxattrFim>& fim,
                            const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum peer_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, peer_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  boost::scoped_array<char> buf;
  ssize_t buf_size = std::min((*fim)->value_len, kXattrValueMax);
  if (buf_size)
    buf.reset(new char[buf_size]);
  buf_size = ms()->store()->GetXattr(
      &context, (*fim)->inode, fim->tail_buf(), buf.get(), buf_size);
  replier.SetResult(buf_size);
  if (buf_size >= 0) {
    FIM_PTR<GetxattrReplyFim> reply = GetxattrReplyFim::MakePtr();
    (*reply)->value_len = buf_size;
    if ((*fim)->value_len > 0) {
      reply->tail_buf_resize(buf_size);
      std::memcpy(reply->tail_buf(), buf.get(), buf_size);
    }
    replier.SetNormalReply(reply);
  }
  return true;
}

bool Worker::HandleListxattr(const FIM_PTR<ListxattrFim>& fim,
                             const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum peer_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, peer_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  boost::scoped_array<char> buf;
  ssize_t buf_size = std::min((*fim)->size, kXattrListMax);
  if (buf_size)
    buf.reset(new char[buf_size]);
  ssize_t ret_size = ms()->store()->ListXattr(
      &context, (*fim)->inode, buf.get(), buf_size);
  replier.SetResult(ret_size);
  if (ret_size >= 0) {
    FIM_PTR<ListxattrReplyFim> reply = ListxattrReplyFim::MakePtr();
    (*reply)->size = ret_size;
    if (buf_size > 0) {
      reply->tail_buf_resize(ret_size);
      std::memcpy(reply->tail_buf(), buf.get(), ret_size);
    }
    replier.SetNormalReply(reply);
  }
  return true;
}

bool Worker::HandleRemovexattr(const FIM_PTR<RemovexattrFim>& fim,
                               const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum peer_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, peer_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  int err_no = ms()->store()->RemoveXattr(
      &context, (*fim)->inode, fim->tail_buf());
  replier.SetResult(err_no);
  return true;
}

// Non-directory inode handling

bool Worker::HandleOpen(const FIM_PTR<OpenFim>& fim,
                        const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  FIM_PTR<DataReplyFim> reply = DataReplyFim::MakePtr();
  replier.SetNormalReply(reply);
  InodeNum inode = (*fim)->inode;
  std::vector<GroupId> groups;
  bool truncated;
  int err_no = ms()->store()->Open(&context, inode, (*fim)->flags, &groups,
                                   &truncated);
  replier.SetResult(err_no);
  if (err_no != 0)
    return true;
  bool need_trunc = ((*fim)->flags & O_TRUNC) && client_num != kNotClient;
  SetReplyGroups(reply, groups);
  bool dirty_attr = need_trunc ?
      ms()->dirty_inode_mgr()->IsVolatile(inode) : false;
  bool is_write = IsWriteFlag((*fim)->flags);
  ms()->inode_usage()->
      SetFCOpened(fim->req_id() >> (64U - kClientBits), inode,
                  is_write ? kInodeWriteAccess : kInodeReadAccess);
  if (is_write && client_num != kNotClient) {
    cache_invalidator_.InvalidateClients(inode, false, client_num);
    ms()->dirty_inode_mgr()->SetVolatile(inode, true, true, 0);
  }
  if (need_trunc && (dirty_attr || truncated)) {
    std::vector<boost::shared_ptr<IMetaDSLock> > locks;
    ms()->ds_locker()->Lock(inode, groups, &locks);
    DSTruncate(context.req_context->optime, inode, groups, 0);
  }
  return true;
}

bool Worker::HandleAccess(const FIM_PTR<AccessFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  int err_no = ms()->store()->Access(&context, (*fim)->inode, (*fim)->mask);
  replier.SetResult(err_no);
  return true;
}

bool Worker::HandleAdviseWrite(const FIM_PTR<AdviseWriteFim>& fim,
                               const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  int err_no = ms()->store()->
      AdviseWrite(&context, (*fim)->inode, (*fim)->off);
  replier.SetResult(err_no);
  if (err_no >= 0)
    cache_invalidator_.InvalidateClients((*fim)->inode, true, client_num);
  return true;
}

bool Worker::HandleRelease(const FIM_PTR<ReleaseFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  InodeNum inode = (*fim)->inode;
  ms()->store()->OperateInode(&context, inode);
  context.inodes_changed.push_back(inode);
  IInodeUsage* inode_usage = ms()->inode_usage();
  uint64_t gen = 0;
  bool stopped_writing = inode_usage->SetFCClosed(
      fim->req_id() >> (64U - kClientBits), inode, (*fim)->keep_read);
  bool need_attr_cleanup = false;
  bool clean = (*fim)->clean;
  if (stopped_writing) {
    need_attr_cleanup =
        !ms()->dirty_inode_mgr()->SetVolatile(inode, false, clean, &gen);
  } else if (!clean) {
    ms()->dirty_inode_mgr()->SetVolatile(inode, true, false, &gen);
  }
  int err_no = 0;
  if (inode_usage->IsPendingUnlink(inode) && !inode_usage->IsOpened(inode)) {
    boost::shared_ptr<std::vector<GroupId> >
        group_ids(new std::vector<GroupId>);
    ms()->store()->GetFileGroupIds(inode, group_ids.get());
    err_no = ms()->store()->FreeInode(inode);
    inode_usage->RemovePendingUnlink(inode);
    if (err_no == 0)
      replier.set_active_complete_cb(
          boost::bind(&Worker::DSFreeInode, this, inode, group_ids));
  }
  if (need_attr_cleanup)
    PrepareAttrCleanup(inode, gen);
  else if (stopped_writing)
    ms()->dirty_inode_mgr()->Clean(inode, &gen);
  replier.SetResult(err_no);
  return true;
}

void Worker::PrepareAttrCleanup(InodeNum inode, uint64_t gen) {
  FileAttr fa;
  int err_no = ms()->store()->GetInodeAttr(inode, &fa);
  if (err_no != 0) {  // File already lost, no need to update
    gen = uint64_t(-1);
    ms()->dirty_inode_mgr()->Clean(inode, &gen);
    return;
  }
  if (ms()->dirty_inode_mgr()->StartCleaning(inode))
    ms()->attr_updater()->AsyncUpdateAttr(
        inode, fa.mtime, fa.size,
        boost::bind(&Worker::CompleteAttrCleanup, this, inode, gen, _1, _2));
}

void Worker::CompleteAttrCleanup(InodeNum inode, uint64_t gen,
                                 FSTime mtime, uint64_t size) {
  FIM_PTR<AttrUpdateCompletionFim> completion_fim =
      AttrUpdateCompletionFim::MakePtr();
  (*completion_fim)->inode = inode;
  (*completion_fim)->mtime = mtime;
  (*completion_fim)->size = size;
  (*completion_fim)->gen = gen;
  Enqueue(completion_fim, boost::shared_ptr<IFimSocket>());
}

bool Worker::HandleAttrUpdateCompletionFim(
    const FIM_PTR<AttrUpdateCompletionFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  (void) peer;  // Not used
  uint64_t gen = (*fim)->gen;
  {
    boost::unique_lock<MUTEX_TYPE> lock;
    if (!ms()->dirty_inode_mgr()->Clean((*fim)->inode, &gen, &lock)) {
      if (gen)  // Retry
        PrepareAttrCleanup((*fim)->inode, gen);
      return true;
    }
    ms()->store()->UpdateInodeAttr((*fim)->inode,
                                   (*fim)->mtime, (*fim)->size);
  }
  boost::shared_ptr<IReqTracker> ms_tracker =
      ms()->tracker_mapper()->GetMSTracker();
  // Replicate to standby MS if needed
  if (ms()->state_mgr()->GetState() == kStateActive) {
    boost::shared_ptr<IFimSocket> socket = ms_tracker->GetFimSocket();
    if (socket)
      socket->WriteMsg(fim);  // No reply expected
  }
  return true;
}

bool Worker::HandleReadlink(const FIM_PTR<ReadlinkFim>& fim,
                            const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  std::vector<char> buf;
  int err_no = ms()->store()->Readlink(&context, (*fim)->inode, &buf);
  replier.SetResult(err_no);
  if (err_no >= 0) {
    FIM_PTR<DataReplyFim> reply
        = DataReplyFim::MakePtr(buf.size() + 1);
    std::memcpy(reply->tail_buf(), buf.data(), buf.size());
    reply->tail_buf()[buf.size()] = '\0';
    replier.SetNormalReply(reply);
  }
  return true;
}

// Reading directories

bool Worker::HandleLookup(const FIM_PTR<LookupFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  boost::shared_ptr<OpContext> context = boost::make_shared<OpContext>();
  ReplReplyOnExit replier(fim, peer, ms()->replier(), context.get());
  context->req_context = &(*fim)->context;
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  uint64_t version1 = ms()->dirty_inode_mgr()->version();
  uint64_t version2;
  int err_no = ms()->store()->
      Lookup(context.get(), (*fim)->inode, fim->tail_buf(),
             &(*reply)->inode, &(*reply)->fa);
  if (err_no == 0 &&
      ms()->dirty_inode_mgr()->IsVolatile((*reply)->inode, &version2)) {
    context->i_mutex_guard.reset();  // Allow other operations to proceed
    ms()->attr_updater()->AsyncUpdateAttr(
        (*reply)->inode, (*reply)->fa.mtime, (*reply)->fa.size,
        boost::bind(&Worker::ReplyDeferredLookup, this,
                    fim, peer, context, reply, _1, _2));
    replier.SetEnabled(false);
    return true;
  }
  if (version1 != version2)  // Something cleaned, refetch to be safe
    ms()->store()->GetInodeAttr((*reply)->inode, &(*reply)->fa);
  replier.SetNormalReply(reply);
  replier.SetResult(err_no);
  if (err_no == 0) {
    (*reply)->dirty = false;
    ms()->cache_tracker()->SetCache((*fim)->inode, client_num);
    ms()->cache_tracker()->SetCache((*reply)->inode, client_num);
  }
  return true;
}

void Worker::ReplyDeferredLookup(const FIM_PTR<LookupFim>& fim,
                                 const boost::shared_ptr<IFimSocket>& peer,
                                 boost::shared_ptr<OpContext> context,
                                 FIM_PTR<AttrReplyFim> reply,
                                 FSTime mtime, uint64_t size) {
  ReplReplyOnExit replier(fim, peer, ms()->replier(), context.get());
  replier.SetNormalReply(reply);
  (*reply)->fa.mtime = mtime;
  (*reply)->fa.size = size;
  (*reply)->dirty = true;
  ms()->cache_tracker()->
      SetCache((*fim)->inode, peer->GetReqTracker()->peer_client_num());
  replier.SetResult(0);
}

bool Worker::HandleOpendir(const FIM_PTR<OpendirFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  int err_no = ms()->store()->Opendir(&context, (*fim)->inode);
  replier.SetResult(err_no);
  return true;
}

bool Worker::HandleReaddir(const FIM_PTR<ReaddirFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = 0;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  if ((*fim)->size > unsigned(32 * 1024 * 1024))  // Don't alloc huge vector
    return true;  // Reply with default EIO
  std::vector<char> buf((*fim)->size);
  int ret = ms()->store()->
      Readdir(&context, (*fim)->inode, (*fim)->cookie, &buf);
  replier.SetResult(ret);
  if (ret < 0)
    return true;
  FIM_PTR<DataReplyFim> reply = DataReplyFim::MakePtr(ret);
  std::memcpy(reply->tail_buf(), &buf[0], ret);
  replier.SetNormalReply(reply);
  return true;
}

// Create directory entries

bool Worker::HandleCreate(const FIM_PTR<CreateFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  if ((*fim)->req.new_inode == 0)
    (*fim)->req.new_inode = ms()->inode_src()->Allocate((*fim)->inode, true);
  (*reply)->inode = (*fim)->req.new_inode;
  replier.SetNormalReply(reply);
  std::vector<GroupId> groups;
  GetGroupsFromFim((*fim)->num_ds_groups, fim, &groups);
  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  int err_no = ms()->store()->
      Create(&context, (*fim)->inode, fim->tail_buf(), &(*fim)->req,
             &groups, &(*reply)->fa, &removed_info);
  replier.SetResult(err_no);
  if (err_no == 0) {
    if (removed_info) {
      if (removed_info->to_free)
        replier.set_active_complete_cb(
            boost::bind(&Worker::DSFreeInode, this,
                        removed_info->inode, removed_info->groups));
      cache_invalidator_.InvalidateClients(
          removed_info->inode, false, client_num);
    }
    ms()->cache_tracker()->SetCache((*fim)->inode, client_num);
    bool is_write = IsWriteFlag((*fim)->req.flags);
    (*reply)->dirty = is_write;
    ms()->inode_usage()->
        SetFCOpened(fim->req_id() >> (64U - kClientBits), (*fim)->req.new_inode,
                    is_write ? kInodeWriteAccess : kInodeReadAccess);
    if (is_write)
      ms()->dirty_inode_mgr()->SetVolatile((*fim)->req.new_inode, true,
                                           true, 0);
    if ((*fim)->num_ds_groups == 0) {
      (*fim)->num_ds_groups = groups.size();
      AppendFimReqGroups(fim, groups);
    }
    SetReplyGroups(reply, groups);
  }
  return true;
}

bool Worker::HandleMkdir(const FIM_PTR<MkdirFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->parent, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  if ((*fim)->req.new_inode == 0)
    (*fim)->req.new_inode = ms()->inode_src()->Allocate((*fim)->parent, false);
  (*reply)->inode = (*fim)->req.new_inode;
  replier.SetNormalReply(reply);
  int err_no = ms()->store()->
      Mkdir(&context, (*fim)->parent, fim->tail_buf(),
            &(*fim)->req, &(*reply)->fa);
  replier.SetResult(err_no);
  if (err_no >= 0) {
    (*reply)->dirty = false;
    cache_invalidator_.InvalidateClients((*fim)->parent, true, client_num);
  }
  return true;
}

bool Worker::HandleSymlink(const FIM_PTR<SymlinkFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  if ((*fim)->name_len + 1U > fim->tail_buf_size() ||
      fim->tail_buf()[(*fim)->name_len] != '\0' ||
      fim->tail_buf()[fim->tail_buf_size() - 1] != '\0')
    return true;
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  if ((*fim)->new_inode == 0)
    (*fim)->new_inode = ms()->inode_src()->Allocate((*fim)->inode, true);
  (*reply)->inode = (*fim)->new_inode;
  replier.SetNormalReply(reply);
  int err_no = ms()->store()->
      Symlink(&context, (*fim)->inode, fim->tail_buf(), (*fim)->new_inode,
              fim->tail_buf() + (*fim)->name_len + 1, &(*reply)->fa);
  replier.SetResult(err_no);
  if (err_no >= 0) {
    (*reply)->dirty = false;
    cache_invalidator_.InvalidateClients((*fim)->inode, true, client_num);
  }
  return true;
}

bool Worker::HandleMknod(const FIM_PTR<MknodFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  if ((*fim)->new_inode == 0)
    (*fim)->new_inode = ms()->inode_src()->Allocate((*fim)->inode, true);
  (*reply)->inode = (*fim)->new_inode;
  replier.SetNormalReply(reply);
  std::vector<GroupId> groups;
  GetGroupsFromFim((*fim)->num_ds_groups, fim, &groups);
  int err_no = ms()->store()->
      Mknod(&context, (*fim)->inode, fim->tail_buf(), (*fim)->new_inode,
            (*fim)->mode, (*fim)->rdev, &groups, &(*reply)->fa);
  replier.SetResult(err_no);
  if (err_no >= 0) {
    (*reply)->dirty = false;
    reply->tail_buf_resize(groups.size() * sizeof(GroupId));
    std::memcpy(reply->tail_buf(), groups.data(),
                groups.size() * sizeof(GroupId));
    cache_invalidator_.InvalidateClients((*fim)->inode, true, client_num);
    if ((*fim)->num_ds_groups == 0 && groups.size() > 0) {
      (*fim)->num_ds_groups = groups.size();
      AppendFimReqGroups(fim, groups);
    }
  }
  return true;
}

// Manipulate directory entries

bool Worker::HandleLink(const FIM_PTR<LinkFim>& fim,
                        const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  FIM_PTR<AttrReplyFim> reply = AttrReplyFim::MakePtr();
  replier.SetNormalReply(reply);
  InodeNum inode = (*reply)->inode = (*fim)->inode;
  int err_no = ms()->store()->
      Link(&context, inode, (*fim)->dir_inode, fim->tail_buf(), &(*reply)->fa);
  replier.SetResult(err_no);
  if (err_no >= 0) {
    (*reply)->dirty = ms()->dirty_inode_mgr()->IsVolatile(inode);
    cache_invalidator_.InvalidateClients(inode, false, client_num);
    cache_invalidator_.InvalidateClients((*fim)->dir_inode, true, client_num);
    if (!(*reply)->dirty)
      ms()->cache_tracker()->SetCache(inode, client_num);
  }
  return true;
}

bool Worker::HandleRename(const FIM_PTR<RenameFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->new_parent, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  if ((*fim)->name_len + 1U > fim->tail_buf_size() ||
      fim->tail_buf()[(*fim)->name_len] != '\0' ||
      fim->tail_buf()[fim->tail_buf_size() - 1] != '\0')
    return true;
  InodeNum moved;
  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  int err_no = ms()->store()->
      Rename(&context, (*fim)->parent, fim->tail_buf(), (*fim)->new_parent,
             fim->tail_buf() + (*fim)->name_len + 1, &moved, &removed_info);
  replier.SetResult(err_no);
  if (err_no >= 0) {
    if (removed_info) {
      if (removed_info->to_free)
        replier.set_active_complete_cb(
            boost::bind(&Worker::DSFreeInode, this,
                        removed_info->inode, removed_info->groups));
      cache_invalidator_.InvalidateClients(
          removed_info->inode, false, client_num);
    }
    cache_invalidator_.InvalidateClients(moved, false, client_num);
    cache_invalidator_.InvalidateClients((*fim)->parent, true, client_num);
    if ((*fim)->parent != (*fim)->new_parent)
      cache_invalidator_.InvalidateClients(
          (*fim)->new_parent, true, client_num);
  }
  return true;
}

// Remove directory entries

bool Worker::HandleUnlink(const FIM_PTR<UnlinkFim>& fim,
                          const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->inode, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  int err_no = ms()->store()->
      Unlink(&context, (*fim)->inode, fim->tail_buf(), &removed_info);
  replier.SetResult(err_no);
  if (err_no == 0) {
    if (removed_info) {
      if (removed_info->to_free)
        replier.set_active_complete_cb(
            boost::bind(&Worker::DSFreeInode, this,
                        removed_info->inode, removed_info->groups));
      cache_invalidator_.InvalidateClients(
          removed_info->inode, false, client_num);
    }
    cache_invalidator_.InvalidateClients((*fim)->inode, true, client_num);
  }
  return true;
}

bool Worker::HandleRmdir(const FIM_PTR<RmdirFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer) {
  ClientNum client_num = peer->GetReqTracker()->peer_client_num();
  ms()->tracer()->Log(__func__, (*fim)->parent, client_num);
  OpContext context;
  context.req_context = &(*fim)->context;
  ReplReplyOnExit replier(fim, peer, ms()->replier(), &context);
  int err_no = ms()->store()->
      Rmdir(&context, (*fim)->parent, fim->tail_buf());
  replier.SetResult(err_no);
  if (err_no >= 0)
    cache_invalidator_.InvalidateClients((*fim)->parent, true, client_num);
  return true;
}

void Worker::DSTruncate(const FSTime& optime, InodeNum inode,
                        const std::vector<GroupId>& group_ids,
                        uint64_t size) {
  FileCoordManager coord_mgr(inode, group_ids.data(), group_ids.size());
  std::vector<Segment> segments;
  coord_mgr.GetAllGroupEnd(size, &segments);
  ms()->dsg_op_state_mgr()->RegisterInodeOp(inode, &coord_mgr);
  std::vector<boost::shared_ptr<IReqEntry> > req_entries;
  for (unsigned i = 0; i < segments.size(); ++i) {
    GroupId group = segments[i].dsg;
    GroupRole failed;
    DSGroupState state = ms()->topology_mgr()->GetDSGState(group, &failed);
    for (GroupRole role = 0; role < kNumDSPerGroup; ++role) {
      GroupRole target = role;
      FIM_PTR<TruncateDataFim> req = TruncateDataFim::MakePtr();
      (*req)->inode = inode;
      (*req)->optime = optime;
      (*req)->dsg_off = segments[i].dsg_off;
      (*req)->last_off = size;
      (*req)->target_role = role;
      (*req)->checksum_role = segments[i].dsr_checksum;
      if (state == kDSGDegraded && role == failed) {
        if (role == (*req)->checksum_role)
          continue;
        target = (*req)->checksum_role;  // Redirect
      }
      boost::shared_ptr<IReqTracker> ds_tracker = ms()->
          tracker_mapper()->GetDSTracker(group, target);
      req_entries.push_back(ds_tracker->AddRequest(req));
    }
  }
  ms()->dirty_inode_mgr()->NotifyAttrSet(inode);
  for (std::vector<boost::shared_ptr<IReqEntry> >::iterator it
           = req_entries.begin();
       it != req_entries.end();
       ++it)
    try {
      (*it)->WaitReply();  // Original
    } catch (std::runtime_error) {}  // ignore
  ms()->dsg_op_state_mgr()->CompleteInodeOp(inode, &coord_mgr);
}

void Worker::DSMtime(const FSTime& optime, InodeNum inode,
                     const std::vector<GroupId>& group_ids,
                     uint64_t* size_ret) {
  std::vector<boost::shared_ptr<IReqEntry> > req_entries;
  for (unsigned i = 0; i < group_ids.size(); ++i) {
    GroupId g = group_ids[i];
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      FIM_PTR<MtimeUpdateFim> req = MtimeUpdateFim::MakePtr();
      (*req)->inode = inode;
      (*req)->mtime = optime;
      boost::shared_ptr<IReqTracker> ds_tracker = ms()->
          tracker_mapper()->GetDSTracker(g, r);
      boost::shared_ptr<IReqEntry> entry =
          MakeTransientReqEntry(ds_tracker, req);
      if (ds_tracker->AddRequestEntry(entry))
        req_entries.push_back(entry);
    }
  }
  ms()->dirty_inode_mgr()->NotifyAttrSet(inode);
  uint64_t new_size = 0;
  for (std::vector<boost::shared_ptr<IReqEntry> >::iterator it
           = req_entries.begin();
       it != req_entries.end();
       ++it) {
    try {
      FIM_PTR<IFim> reply = (*it)->WaitReply();
      if (reply->type() == kAttrUpdateReplyFim) {
        FIM_PTR<AttrUpdateReplyFim> rreply =
            boost::static_pointer_cast<AttrUpdateReplyFim>(reply);
        new_size = std::max(new_size, (*rreply)->size);
      }
    } catch (const std::exception& ex) {
      LOG(warning, FS, "DS connection lost when waiting for inode ",
          PVal(inode), " mtime update completion, ignored");
    }
  }
  *size_ret = new_size;
}

void Worker::UpdateAttrReplySync(InodeNum inode,
                                 const FIM_PTR<AttrReplyFim>& reply) {
  // TODO(Isaac): Merge this with other methods once C++1x adopted
  std::vector<GroupId> group_ids;
  ms()->store()->GetFileGroupIds(inode, &group_ids);
  std::vector<boost::shared_ptr<IReqEntry> > req_entries;
  for (unsigned i = 0; i < group_ids.size(); ++i) {
    GroupId g = group_ids[i];
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      FIM_PTR<AttrUpdateFim> req = AttrUpdateFim::MakePtr();
      (*req)->inode = inode;
      boost::shared_ptr<IReqTracker> ds_tracker = ms()->
          tracker_mapper()->GetDSTracker(g, r);
      boost::shared_ptr<IReqEntry> entry =
          MakeTransientReqEntry(ds_tracker, req);
      if (ds_tracker->AddRequestEntry(entry))
        req_entries.push_back(entry);
    }
  }
  for (std::vector<boost::shared_ptr<IReqEntry> >::iterator it
           = req_entries.begin();
       it != req_entries.end();
       ++it) {
    try {
      FIM_PTR<IFim> upd_reply = (*it)->WaitReply();
      if (upd_reply && upd_reply->type() == kAttrUpdateReplyFim) {
        AttrUpdateReplyFim& rreply =
            static_cast<AttrUpdateReplyFim&>(*upd_reply);
        if ((*reply)->fa.mtime < rreply->mtime)
          (*reply)->fa.mtime = rreply->mtime;
        (*reply)->fa.size = std::max((*reply)->fa.size, rreply->size);
      }
    } catch (const std::exception& ex) {
    }
  }
}

void Worker::DSFreeInode(
    InodeNum inode,
    const boost::shared_ptr<const std::vector<GroupId> >& group_ids) {
  for (std::vector<GroupId>::const_iterator it = group_ids->begin();
       it != group_ids->end();
       ++it) {
    GroupRole failed;
    DSGroupState state = ms()->topology_mgr()->GetDSGState(*it, &failed);
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      if (state != kDSGReady && failed == r)
        continue;
      FIM_PTR<FreeDataFim> req = FreeDataFim::MakePtr();
      (*req)->inode = inode;
      boost::shared_ptr<IReqTracker> tracker =
          ms()->tracker_mapper()->GetDSTracker(*it, r);
      tracker->AddRequestEntry(MakeTransientReqEntry(tracker, req));
    }
  }
}

bool Worker::HandleReconfirmEndFim(
    const FIM_PTR<ReconfirmEndFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  (void) fim;
  ms()->failover_mgr()->AddReconfirmDone(
      peer->GetReqTracker()->peer_client_num());
  return true;
}

bool Worker::HandleResyncInode(
    const FIM_PTR<ResyncInodeFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  int err_no = ms()->store()->ResyncInode(
      (*fim)->inode, (*fim)->fa, fim->tail_buf(),
      (*fim)->extra_size, fim->tail_buf_size());
  replier.SetResult(err_no);
  return true;
}

bool Worker::HandleResyncDentry(
    const FIM_PTR<ResyncDentryFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  int err_no = ms()->store()->ResyncDentry(
      (*fim)->uid, (*fim)->gid,
      (*fim)->parent, (*fim)->target, (*fim)->type, fim->tail_buf());
  replier.SetResult(err_no);
  return true;
}

bool Worker::HandleResyncRemoval(
    const FIM_PTR<ResyncRemovalFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  int err_no = ms()->store()->ResyncRemoval((*fim)->inode);
  replier.SetResult(err_no);
  return true;
}

bool Worker::HandleResyncInodeUsagePrep(
    const FIM_PTR<ResyncInodeUsagePrepFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  ms()->dirty_inode_mgr()->Reset(false);
  const InodeNum* inodes =
      reinterpret_cast<const InodeNum*>(fim->tail_buf());
  unsigned num_inodes = fim->tail_buf_size() / sizeof(InodeNum);
  ms()->inode_src()->SetLastUsed(
      std::vector<InodeNum>(inodes, inodes + num_inodes));
  replier.SetResult(0);
  return true;
}

bool Worker::HandleResyncDirtyInodeFim(
    const FIM_PTR<ResyncDirtyInodeFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  ms()->dirty_inode_mgr()->SetVolatile((*fim)->inode, false, false, 0);
  replier.SetResult(0);
  return true;
}

bool Worker::HandleResyncPendingUnlink(
    const FIM_PTR<ResyncPendingUnlinkFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  InodeSet inodes;
  GetInodesFromFim(*fim, &inodes);
  if ((*fim)->first)
    ms()->inode_usage()->ClearPendingUnlink();
  ms()->inode_usage()->AddPendingUnlinks(inodes);
  replier.SetResult(0);
  return true;
}

bool Worker::HandleResyncClientOpened(
    const FIM_PTR<ResyncClientOpenedFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  InodeAccessMap access_map;
  const InodeNum* inodes = reinterpret_cast<const InodeNum*>(fim->tail_buf());
  std::size_t num_inodes = fim->tail_buf_size() / sizeof(InodeNum);
  for (std::size_t i = 0; i < (*fim)->num_writable; ++i) {
    access_map[inodes[i]] = kInodeWriteAccess;
    // clean flag already sync'ed by ResyncDirtyInodes
    ms()->dirty_inode_mgr()->SetVolatile(inodes[i], true, true, 0);
  }
  for (std::size_t i = (*fim)->num_writable; i < num_inodes; ++i)
    access_map[inodes[i]] = kInodeReadAccess;
  ms()->inode_usage()->SwapClientOpened((*fim)->client_num, &access_map);
  replier.SetResult(0);
  return true;
}

bool Worker::HandleResyncXattr(
    const FIM_PTR<ResyncXattrFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  ReplyOnExit replier(fim, peer);
  if (std::string((*fim)->name) == "swait")
    ms()->startup_mgr()->Reset(fim->tail_buf());  // Reload
  replier.SetResult(0);
  return true;
}

bool Worker::HandleInodeCleanAttr(
    const FIM_PTR<InodeCleanAttrFim>& fim,
    const boost::shared_ptr<IFimSocket>& peer) {
  (void) peer;  // Should always be null
  PrepareAttrCleanup((*fim)->inode, (*fim)->gen);
  return true;
}

}  // namespace

IWorker* MakeWorker() {
  return new Worker();
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
