/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of utilities of VFS interface for common filesystem operations.
 */

#include "client/fs_common_lowlevel.hpp"

#include <cstring>

#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "req_entry.hpp"

namespace cpfs {
namespace client {

void CreateReplyCallback(const boost::shared_ptr<IReqEntry>& ent) {
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

}  // namespace client
}  // namespace cpfs
