/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the VFS interface for common filesystem operations.
 */

#include "client/fs_common_lowlevel.hpp"

#include <fcntl.h>
#include <fuse/fuse_lowlevel.h>

#include <cstring>
#include <ctime>
#include <vector>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

#include "ds_iface.hpp"
#include "dsg_state.hpp"
#include "event.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "op_completion.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_limiter.hpp"
#include "req_tracker.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "client/base_client.hpp"
#include "client/cache.hpp"
#include "client/file_handle.hpp"
#include "client/inode_usage.hpp"

namespace cpfs {
namespace client {

void FSCommonLL::SetClient(BaseFSClient* client) {
  client_ = client;
}

bool FSCommonLL::Open(
    const FSIdentity& identity,
    uint64_t inode, int flags, FSOpenReply* ret) {
  if (flags & O_TRUNC)
    WaitWriteComplete(inode);
  FIM_PTR<OpenFim> rfim = OpenFim::MakePtr();
  (*rfim)->inode = inode;
  (*rfim)->context.uid = identity.uid;
  (*rfim)->context.gid = identity.gid;
  (*rfim)->context.optime.FromNow();
  (*rfim)->flags = flags;
  boost::shared_ptr<IReqEntry> entry;
  bool is_write = IsWriteFlag(flags);
  {
    boost::scoped_ptr<IInodeUsageGuard> guard;
    client_->inode_usage_set()->UpdateOpened(inode, is_write, &guard);
    entry = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
  }
  ret->reply = entry->WaitReply();
  if (ret->reply->type() != kDataReplyFim) {
    DoUpdateClosed(inode, IsWriteFlag(flags));
    return false;
  }
  ret->fh = MakeFH(inode, ret->reply);
  if (flags & O_TRUNC)
    client_->cache_mgr()->InvalidateInode(inode, true);
  return true;
}

bool FSCommonLL::Create(
    const FSIdentity& identity,
    uint64_t parent, const char* name, mode_t mode, int flags,
    FSCreateReply* ret) {
  unsigned len = std::strlen(name) + 1;
  FIM_PTR<CreateFim> rfim = CreateFim::MakePtr(len);
  std::memcpy(rfim->tail_buf(), name, len);
  (*rfim)->inode = parent;
  (*rfim)->req.new_inode = 0;
  (*rfim)->req.mode = mode;
  (*rfim)->req.flags = flags;
  (*rfim)->context.uid = identity.uid;
  (*rfim)->context.gid = identity.gid;
  (*rfim)->context.optime.FromNow();
  (*rfim)->num_ds_groups = 0;
  boost::shared_ptr<IReqEntry> entry;
  {
    boost::unique_lock<MUTEX_TYPE> lock;
    entry = client_->tracker_mapper()->GetMSTracker()->
        AddRequest(rfim, &lock);
    entry->OnAck(boost::bind(&FSCommonLL::CreateReplyCallback, this, _1));
  }
  ret->reply = entry->WaitReply();
  if (ret->reply->type() != kAttrReplyFim)
    return false;
  AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*ret->reply);
  ret->inode = rreply->inode;
  rreply->fa.ToStat(rreply->inode, &ret->attr);
  ret->fh = MakeFH(rreply->inode, ret->reply);
  client_->cache_mgr()->AddEntry(parent, name);
  // TODO(Isaac): May corrupt MS count (race)
  //
  // There is a slight race here, that another thread could possibly
  // get the inode number, e.g., via a readdir() call, use it,
  // release it, and send ReleaseFim asking the MS to treat it as
  // unused, all before this thread get a chance to run
  // UpdateOpened().  Tricky to fix, because we know the new inode
  // number too late.  We probably need to have a state in the MS
  // that an inode is just created and ignore all ReleaseFim of the
  // same FC, and add an extra message to tell the MS that the
  // CreateFim reply has been received so that this state is reset.
  client_->inode_usage_set()->UpdateOpened(rreply->inode, IsWriteFlag(flags));
  return true;
}

bool FSCommonLL::Lookup(
    const FSIdentity& identity,
    uint64_t parent, const char* name, FSLookupReply* ret) {
  unsigned len = std::strlen(name) + 1;
  FIM_PTR<LookupFim> rfim = LookupFim::MakePtr(len);
  std::memcpy(rfim->tail_buf(), name, len);
  (*rfim)->inode = parent;
  (*rfim)->context.uid = identity.uid;
  (*rfim)->context.gid = identity.gid;
  (*rfim)->context.optime.FromNow();
  boost::shared_ptr<ICacheInvRecord> cache_inv_rec;
  cache_inv_rec = client_->cache_mgr()->StartLookup();
  boost::shared_ptr<IReqEntry> entry =
      client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
  ret->reply = entry->WaitReply();
  if (ret->reply->type() != kAttrReplyFim)
    return false;
  AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*ret->reply);
  ret->inode = rreply->inode;
  rreply->fa.ToStat(rreply->inode, &ret->attr);
  MUTEX_LOCK_GUARD(*cache_inv_rec->GetMutex());
  ret->attr_timeout = cache_inv_rec->InodeInvalidated(rreply->inode, false) ||
      rreply->dirty ? 0 : 3600.0;
  ret->entry_timeout = 0;
  if (!cache_inv_rec->InodeInvalidated(parent, true)) {
    ret->entry_timeout = 3600.0;
    client_->cache_mgr()->AddEntry(parent, name, true);
  }
  return true;
}

bool FSCommonLL::Read(
    uint64_t fh, uint64_t inode, std::size_t size, off_t off,
    FSReadReply* ret) {
  FileCoordManager* file_coord = FHFileCoordManager(fh);
  std::vector<Segment> segments;
  file_coord->GetSegments(off, size, &segments);
  std::vector<boost::shared_ptr<IReqEntry> > entries;
  for (unsigned i = 0; i < segments.size(); ++i) {
    Segment& segment = segments[i];
    FIM_PTR<ReadFim> rfim = ReadFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->dsg_off = segment.dsg_off;
    (*rfim)->size = segment.data_size;
    (*rfim)->target_group = segment.dsg;
    (*rfim)->target_role = segment.dsr_data;
    (*rfim)->checksum_role = segment.dsr_checksum;
    entries.push_back(
        GetDSTracker(segment.dsg, segment.dsr_data, segment.dsr_checksum)->
        AddRequest(rfim));
  }
  char* curr = ret->buf;
  for (unsigned i = 0; i < segments.size(); ++i) {
    ret->reply = entries[i]->WaitReply();
    if (ret->reply->type() != kDataReplyFim)
      return false;
    std::memcpy(curr, ret->reply->tail_buf(), segments[i].data_size);
    curr += segments[i].data_size;
    entries[i].reset();
  }
  return true;
}

bool FSCommonLL::Readv(
    uint64_t fh, uint64_t inode, std::size_t size, off_t off,
    FSReadvReply* ret) {
  FileCoordManager* file_coord = FHFileCoordManager(fh);
  std::vector<Segment> segments;
  file_coord->GetSegments(off, size, &segments);
  std::vector<boost::shared_ptr<IReqEntry> > entries;
  for (unsigned i = 0; i < segments.size(); ++i) {
    Segment& segment = segments[i];
    FIM_PTR<ReadFim> rfim = ReadFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->dsg_off = segment.dsg_off;
    (*rfim)->size = segment.data_size;
    (*rfim)->target_group = segment.dsg;
    (*rfim)->target_role = segment.dsr_data;
    (*rfim)->checksum_role = segment.dsr_checksum;
    entries.push_back(
        GetDSTracker(segment.dsg, segment.dsr_data, segment.dsr_checksum)->
        AddRequest(rfim));
  }
  ret->iov.resize(segments.size());
  for (unsigned i = 0; i < segments.size(); ++i) {
    FIM_PTR<IFim> reply = entries[i]->WaitReply();
    ret->replies.push_back(reply);
    if (reply->type() != kDataReplyFim)
      return false;
    ret->iov[i].iov_base = reply->tail_buf();
    ret->iov[i].iov_len = segments[i].data_size;
    entries[i].reset();
  }
  return true;
}

bool FSCommonLL::Write(
    uint64_t fh, uint64_t inode,
    const char* buf, std::size_t size, off_t off,
    FSWriteReply* ret) {
  ret->deferred_errno = FHGetErrno(fh, true);
  if (ret->deferred_errno)
    return false;
  boost::shared_ptr<IOpCompletionChecker> checker =
      client_->op_completion_checker_set()->Get(inode);
  ReqContext context;
  // TODO(Joseph): Values below are not referenced, can they be removed?
  // context.uid = uid;
  // context.gid = gid;
  context.optime.FromNow();

  FileCoordManager* file_coord = FHFileCoordManager(fh);
  std::vector<Segment> segments;
  file_coord->GetSegments(off, size, &segments);
  client_->inode_usage_set()->SetDirty(inode);
  for (unsigned i = 0; i < segments.size(); ++i) {
    Segment& segment = segments[i];
    FIM_PTR<WriteFim> rfim =
        WriteFim::MakePtr(segment.data_size);
    (*rfim)->inode = inode;
    (*rfim)->optime = context.optime;
    (*rfim)->dsg_off = segment.dsg_off;
    (*rfim)->last_off = off + size;
    (*rfim)->target_group = segment.dsg;
    (*rfim)->target_role = segment.dsr_data;
    (*rfim)->checksum_role = segment.dsr_checksum;
    std::memcpy(rfim->tail_buf(), buf + (segment.file_off - off),
                segment.data_size);
    boost::shared_ptr<IReqTracker> tracker =
        GetDSTracker(segment.dsg, segment.dsr_data, segment.dsr_checksum);
    boost::shared_ptr<IReqEntry> entry = MakeDefaultReqEntry(tracker, rfim);
    boost::unique_lock<MUTEX_TYPE> lock;
    tracker->GetReqLimiter()->Send(entry, &lock);
    entry->OnAck(boost::bind(&FSCommonLL::WriteAckCallback,
                             this, _1, inode, fh), true);
    checker->RegisterOp(entry.get());
  }
  return true;
}

void FSCommonLL::Release(uint64_t inode, uint64_t* fh, int flags) {
  WaitWriteComplete(inode);
  DeleteFH(*fh);
  *fh = 0;
  DoUpdateClosed(inode, IsWriteFlag(flags));
}

bool FSCommonLL::Getattr(
    const FSIdentity& identity, uint64_t inode, FSGetattrReply* ret) {
  WaitWriteComplete(inode);
  FIM_PTR<GetattrFim> rfim = GetattrFim::MakePtr();
  (*rfim)->inode = inode;
  (*rfim)->context.uid = identity.uid;
  (*rfim)->context.gid = identity.gid;
  (*rfim)->context.optime.FromNow();
  boost::shared_ptr<IReqEntry> entry
      = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
  ret->reply = entry->WaitReply();
  if (ret->reply->type() != kAttrReplyFim)
    return false;
  AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*ret->reply);
  rreply->fa.ToStat(inode, ret->stbuf);
  return true;
}

bool FSCommonLL::Setattr(
    const FSIdentity& identity, uint64_t inode,
    const struct stat* attr, int to_set, FSSetattrReply* ret) {
  WaitWriteComplete(inode);
  FIM_PTR<SetattrFim> rfim = SetattrFim::MakePtr();
  (*rfim)->inode = inode;
  (*rfim)->context.uid = identity.uid;
  (*rfim)->context.gid = identity.gid;
  (*rfim)->context.optime.FromNow();
  std::memset(&(*rfim)->fa, '\0', sizeof((*rfim)->fa));
  if ((to_set & FUSE_SET_ATTR_ATIME) && !(to_set & FUSE_SET_ATTR_ATIME_NOW)) {
      (*rfim)->fa.atime.sec = attr->st_atim.tv_sec;
      (*rfim)->fa.atime.ns = attr->st_atim.tv_nsec;
  }
  if ((to_set & FUSE_SET_ATTR_ATIME) && !(to_set & FUSE_SET_ATTR_ATIME_NOW)) {
      (*rfim)->fa.mtime.sec = attr->st_mtim.tv_sec;
      (*rfim)->fa.mtime.ns = attr->st_mtim.tv_nsec;
  }
  if (to_set & (FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW)) {
    if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
      to_set |= FUSE_SET_ATTR_ATIME;
      (*rfim)->fa.atime = (*rfim)->context.optime;
    }
    if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
      to_set |= FUSE_SET_ATTR_MTIME;
      (*rfim)->fa.mtime = (*rfim)->context.optime;
    }
  }
  (*rfim)->fa.mode = attr->st_mode;
  (*rfim)->fa.uid = attr->st_uid;
  (*rfim)->fa.gid = attr->st_gid;
  (*rfim)->fa.size = attr->st_size;
  (*rfim)->fa_mask = to_set;
  boost::scoped_ptr<IInodeUsageGuard> guard;
  if (to_set & (FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_SIZE))
    (*rfim)->locked =
        client_->inode_usage_set()->StartLockedSetattr(inode, &guard);
  else
    (*rfim)->locked = 0;
  boost::shared_ptr<IReqEntry> entry
      = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
  ret->reply = entry->WaitReply();
  if (ret->reply->type() != kAttrReplyFim)
    return false;
  AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*ret->reply);
  rreply->fa.ToStat(inode, &ret->stbuf);
  bool need_clear_pages = S_ISDIR(ret->stbuf.st_mode) &&
      (to_set & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID | FUSE_SET_ATTR_MODE));
  if (need_clear_pages)
    client_->cache_mgr()->InvalidateInode(inode, true);
  return true;
}

void FSCommonLL::Flush(uint64_t inode) {
  WaitWriteComplete(inode);
}

void FSCommonLL::Fsync(uint64_t inode) {
  WaitWriteComplete(inode);
}

inline void FSCommonLL::CreateReplyCallback(
    const boost::shared_ptr<IReqEntry>& ent) {
  const FIM_PTR<IFim>& reply = ent->reply();
  if (reply->type() != kAttrReplyFim)
    return;
  AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*reply);
  const FIM_PTR<IFim>& req = ent->request();
  int orig_size = req->tail_buf_size();
  int groups_size = rreply.tail_buf_size();
  int num_ds_groups = groups_size / sizeof(GroupId);
  switch (req->type()) {
    case kCreateFim:
      {
        CreateFim& creq = static_cast<CreateFim&>(*req);
        creq->req.new_inode = rreply->inode;
        creq->num_ds_groups = num_ds_groups;
        break;
      }
    case kMkdirFim:
      {
        MkdirFim& dreq = static_cast<MkdirFim&>(*req);
        dreq->req.new_inode = rreply->inode;
        return;
      }
    case kSymlinkFim:
      {
        SymlinkFim& sreq = static_cast<SymlinkFim&>(*req);
        sreq->new_inode = rreply->inode;
        return;
      }
    case kMknodFim:
      {
        MknodFim& nreq = static_cast<MknodFim&>(*req);
        nreq->new_inode = rreply->inode;
        nreq->num_ds_groups = num_ds_groups;
        break;
      }
    default:
      { /* do nothing */ }
  }
  req->tail_buf_resize(orig_size + groups_size);
  std::memcpy(req->tail_buf() + orig_size, rreply.tail_buf(), groups_size);
}

void FSCommonLL::DoUpdateClosed(InodeNum inode, bool is_write) {
  boost::scoped_ptr<IInodeUsageGuard> guard;
  bool clean;
  int usage = client_->inode_usage_set()->UpdateClosed(inode, is_write,
                                                       &clean, &guard);
  if (usage != kClientAccessUnchanged) {
    FIM_PTR<ReleaseFim> rfim = ReleaseFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->keep_read = usage;
    (*rfim)->clean = clean;
    client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
  }
}

void FSCommonLL::WaitWriteComplete(InodeNum ino) {
  Event ev;
  client_->op_completion_checker_set()->OnCompleteAll(
      ino, boost::bind(&Event::Invoke, &ev));
  ev.Wait();
}

boost::shared_ptr<IReqTracker> FSCommonLL::GetDSTracker(
    GroupId group, GroupRole role, GroupRole checksum) {
  GroupRole failed;
  DSGroupState state = client_->dsg_state(group, &failed);
  if ((state == kDSGDegraded || state == kDSGResync) && role == failed) {
    LOG(debug, Degraded, "Sending degraded operation for ", PINT(role),
        " to ", PINT(checksum));
    role = checksum;
  }
  return client_->tracker_mapper()->GetDSTracker(group, role);
}

void FSCommonLL::WriteAckCallback(
    const boost::shared_ptr<IReqEntry>& entry, InodeNum inode, uint64_t fh) {
  const FIM_PTR<IFim>& reply = entry->reply();
  if (reply && reply->type() == kResultCodeReplyFim) {
    ResultCodeReplyFim& efim = static_cast<ResultCodeReplyFim&>(*reply);
    if (efim->err_no)
      FHSetErrno(fh, efim->err_no);
  }
  client_->op_completion_checker_set()->CompleteOp(inode, entry.get());
}

}  // namespace client
}  // namespace cpfs
