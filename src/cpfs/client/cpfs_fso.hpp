#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Filesystem object for CPFS.
 */
#include <stdint.h>

#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>

#include <sys/stat.h>
#include <sys/statvfs.h>

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "connector.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "fuseobj.hpp"
#include "io_service_runner.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_tracker.hpp"
#include "thread_fim_processor.hpp"
#include "tracker_mapper.hpp"
#include "version.hpp"
#include "client/base_client.hpp"
#include "client/cache.hpp"
#include "client/conn_mgr.hpp"
#include "client/file_handle.hpp"
#include "client/fs_common_lowlevel.hpp"
#include "client/fs_common_lowlevel_impl.hpp"

struct fuse_chan;

namespace cpfs {

namespace client {

/**
 * Options handled by CPFS.
 */
enum {
  KEY_HELP,
  KEY_HELP_NOHEADER,
  KEY_VERSION,
};

/**
 * Fuse option object for CpfsFuseObj.
 */
struct CpfsFuseOpts {
  char* meta_servers; /**< The meta data servers to use */
  char* log_path; /**< The log path */
  char* log_level; /**< The log level specification */
  char* msg_buf_per_conn; /**< Bytes of unanswered messages per connection */
  char* heartbeat_interval; /**< The interval of heartbeat */
  char* socket_read_timeout; /**< The read stream idle timeout */
  char* init_conn_retry; /**< The retry count for initial connections */
  char* disable_xattr; /**< Whether xattr support is enabled */
};

/**
 * Fuse option specification for CpfsFuseObj.
 */
const struct fuse_opt cpfs_opt_descs[] = {
  { "meta-server=%s", offsetof(CpfsFuseOpts, meta_servers), 1 },
  { "log-path=%s", offsetof(CpfsFuseOpts, log_path), 1},
  { "log-level=%s", offsetof(CpfsFuseOpts, log_level), 1},
  { "msg-buf-per-conn=%s", offsetof(CpfsFuseOpts, msg_buf_per_conn), 1},
  { "heartbeat-interval=%s", offsetof(CpfsFuseOpts, heartbeat_interval), 1},
  { "socket-read-timeout=%s", offsetof(CpfsFuseOpts, socket_read_timeout), 1},
  { "init-conn-retry=%s", offsetof(CpfsFuseOpts, init_conn_retry), 1},
  { "disable-xattr=%s", offsetof(CpfsFuseOpts, disable_xattr), 1},
  FUSE_OPT_KEY("-h", KEY_HELP),
  FUSE_OPT_KEY("--help", KEY_HELP),
  FUSE_OPT_KEY("-V", KEY_VERSION),
  FUSE_OPT_END
};

/**
 * Help message for CPFS when run as FUSE client.
 */
const char kOptDesc[] =
    "CPFSv2 options:\n"
    "  -o meta-server=SERVERS List of meta data servers, comma-separated\n"
    "  -o log-path=PATH       The full path of the log file\n"
    "  -o log-level=LEVEL     The logging level (0 - 7). Default: 5\n"
    "                         May also be specified for particular category\n"
    "                         E.g., 5:Fim=7 use 7 for Fim, 5 for others\n"
    "  -o msg-buf-per-conn=N  Allow N bytes of unanswered msgs per connection\n"
    "  -o init-conn-retry=N   Max retry count for init connection. Default: 3\n"
    "  -o disable-xattr=N     Set to 1 to disable xattr\n"
    "\n"
    "FUSE low-level options\n";

/**
 * Option processing function for CpfsFuseObj.  See the definition of
 * fuse_opt_proc_t in fuse_opt.h for more information.
 *
 * @param data The CpfsFuseOpts object.
 *
 * @param arg The whole argument or option.
 *
 * @param key The key defined in cpfs_opt_descs.
 *
 * @param outargs The current output argument list.
 */
inline int CpfsOptProc(void* data, const char* arg, int key,
                       struct fuse_args* outargs) {
  (void) data;
  (void) arg;
  switch (key) {
    case KEY_HELP:
      std::fprintf(stderr, kOptDesc);
      return fuse_opt_add_arg(outargs, "-h");
    case KEY_VERSION:
      std::fprintf(stderr, "CPFS version: %s\n", CPFS_VERSION);
      return 1;
  }
  return 1;
}

/**
 * @param stage What is the stage of the call
 *
 * @param fn The name of the function running
 *
 * @param ino The inode number being operated on
 */
#define DEBUG_CPFS(stage, fn, ino, ...)                                 \
  LOG(debug, FS,                                                        \
      "FUSE ", PVal(GetReqUniq(req)), ": " stage " ",                   \
      fn, "(", PHex(ino), ")", ##__VA_ARGS__);

/**
 * Fuse object for CPFS.
 *
 * @tparam TFuseMethodPolicy The policy class to use for FUSE
 * interactions.
 */
template <typename TFuseMethodPolicy = FuseMethodPolicy>
class CpfsFuseObj : public BaseFuseObj<TFuseMethodPolicy> {
 public:
  /**
   * @param fuse_method_policy Fuse method policy object to use.
   */
  explicit CpfsFuseObj(TFuseMethodPolicy fuse_method_policy
                       = TFuseMethodPolicy())
      : BaseFuseObj<TFuseMethodPolicy>(fuse_method_policy),
        fs_ll_(MakeFSCommonLL()) {}

  /**
   * Set the client object.
   */
  void SetClient(BaseFSClient* client) {
    client_ = client;
    fs_ll_->SetClient(client);
  }

  void ParseFsOpts(fuse_args* args) {
    CpfsFuseOpts cpfs_opts;
    std::memset(&cpfs_opts, 0, sizeof(cpfs_opts));
    if (fuse_opt_parse(args, &cpfs_opts, cpfs_opt_descs, CpfsOptProc) == -1)
      throw std::runtime_error("Failed parsing arguments");
    if (cpfs_opts.log_path) {
      SetLogPath(cpfs_opts.log_path);
      std::free(cpfs_opts.log_path);
    } else {
      SetLogPath("/dev/stderr");
    }
    if (cpfs_opts.log_level) {
      log_level_ = cpfs_opts.log_level;
      std::free(cpfs_opts.log_level);
    }
    LOG(notice, FS, "CPFS client ", CPFS_VERSION, " is running");
    if (cpfs_opts.meta_servers) {
      std::string meta_servers(cpfs_opts.meta_servers);
      boost::split(meta_servers_, meta_servers, boost::is_any_of(","));
      std::free(cpfs_opts.meta_servers);
    }
    if (cpfs_opts.msg_buf_per_conn) {
      client_->tracker_mapper()->SetLimiterMaxSend(
          boost::lexical_cast<uint64_t>(cpfs_opts.msg_buf_per_conn));
      std::free(cpfs_opts.msg_buf_per_conn);
    }
    if (cpfs_opts.heartbeat_interval) {
      char* p_end;
      heartbeat_interval_ = std::strtod(cpfs_opts.heartbeat_interval, &p_end);
      std::free(cpfs_opts.heartbeat_interval);
      if (heartbeat_interval_ == 0.0) {
        throw std::runtime_error("Invalid heartbeat interval");
      }
    } else {
      heartbeat_interval_ = kDefaultHeartbeatInterval;
    }
    if (cpfs_opts.socket_read_timeout) {
      char* p_end;
      socket_read_timeout_ = std::strtod(cpfs_opts.socket_read_timeout, &p_end);
      std::free(cpfs_opts.socket_read_timeout);
      if (socket_read_timeout_ == 0.0) {
        throw std::runtime_error("Invalid socket read timeout");
      }
    } else {
      socket_read_timeout_ = kDefaultSocketReadTimeout;
    }
    if (cpfs_opts.init_conn_retry) {
      client_->conn_mgr()->SetInitConnRetry(
          boost::lexical_cast<unsigned>(cpfs_opts.init_conn_retry));
      std::free(cpfs_opts.init_conn_retry);
    }
    if (cpfs_opts.disable_xattr) {
      SetUseXattr(cpfs_opts.disable_xattr[0] != '1');
      std::free(cpfs_opts.disable_xattr);
    } else {
      SetUseXattr(true);
    }
  }

  /**
   * Set whether extended attributes should be supported.
   *
   * @param use_xattr Whether to support
   */
  void SetUseXattr(bool use_xattr) {
    use_xattr_ = use_xattr;
  }

  void SetFuseChannel(fuse_chan* chan) {
    client_->cache_mgr()->SetFuseChannel(chan);
  }

  virtual void Init(struct fuse_conn_info* conn) {
    (void) conn;
    SetSeverityCeiling(log_level_);
    client_->cache_mgr()->Init();
    client_->connector()->set_heartbeat_interval(heartbeat_interval_);
    client_->connector()->set_socket_read_timeout(socket_read_timeout_);
    client_->conn_mgr()->Init(meta_servers_);
    client_->service_runner()->Run();
    client_->ms_fim_processor_thread()->Start();
  }

  /**
   * The FUSE getattr method.
   */
  void Getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info*) {
    DEBUG_CPFS("Run", "Getattr", ino);
    struct stat stbuf;
    FSGetattrReply ret;
    ret.stbuf = &stbuf;
    if (!fs_ll_->Getattr(GetFSIdentity(req), ino, &ret)) {
      return HandleUnexpectedReply(req, ino, ret.reply, "Getattr");
    }
    DEBUG_CPFS("Ret", "Getattr", ino);
    AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*ret.reply);
    this->ReplyAttr(req, ret.stbuf, rreply->dirty ? 0 : 3600.0);
  }

  /**
   * The FUSE setattr method.
   */
  void Setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
               int to_set, fuse_file_info* fi) {
    (void) fi;
    DEBUG_CPFS("Run", "Setattr", ino);
    FSSetattrReply ret;
    if (!fs_ll_->Setattr(GetFSIdentity(req), ino, attr, to_set, &ret)) {
      return HandleUnexpectedReply(req, ino, ret.reply, "Setattr");
    }
    DEBUG_CPFS("Ret", "Setattr", ino);
    AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*ret.reply);
    this->ReplyAttr(req, &ret.stbuf, rreply->dirty ? 0 : 3600.0);
  }

  /**
   * The FUSE open method.
   */
  void Open(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
    DEBUG_CPFS("Run", "Open", ino);
    FSOpenReply ret;
    if (!fs_ll_->Open(GetFSIdentity(req), ino, fi->flags, &ret)) {
      return HandleUnexpectedReply(req, ino, ret.reply, "Open");
    }
    DEBUG_CPFS("Ret", "Open", ino);
    fi->fh = ret.fh;
    fi->keep_cache = 1;
    this->ReplyOpen(req, fi);
  }

  /**
   * The FUSE create method.
   */
  void Create(fuse_req_t req, fuse_ino_t ino, const char* name, mode_t mode,
              fuse_file_info* fi) {
    DEBUG_CPFS("Run", "Create", ino, ": ", name);
    FSCreateReply ret;
    if (!fs_ll_->Create(GetFSIdentity(req),
                            ino, name, mode, fi->flags, &ret)) {
      return HandleUnexpectedReply(req, ino, ret.reply, "Create");
    }
    DEBUG_CPFS("Ret", "Create", ino);
    fuse_entry_param entry_param;
    entry_param.ino = ret.inode;
    entry_param.generation = 1;
    entry_param.attr = ret.attr;
    entry_param.attr_timeout = entry_param.entry_timeout = 10.0;
    fi->fh = ret.fh;
    fi->keep_cache = 1;
    this->ReplyCreate(req, &entry_param, fi);
  }

  /**
   * The FUSE flush method.
   */
  void Flush(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
    fs_ll_->Flush(ino);
    this->ReplyErr(req, FHGetErrno(fi->fh, false));
  }

  /**
   * The FUSE release method.
   */
  void Release(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
    fs_ll_->Release(ino, &fi->fh, fi->flags);
    this->ReplyErr(req, 0);
  }

  /**
   * The FUSE flush method.
   */
  void Fsync(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi) {
    (void) fi;
    (void) datasync;
    fs_ll_->Fsync(ino);
    this->ReplyErr(req, 0);
  }

  /**
   * The FUSE link method.
   */
  void Link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
            const char* newname) {
    DEBUG_CPFS("Run", "Link", ino, ": ", PHex(newparent), ", ", newname);
    unsigned len = std::strlen(newname) + 1;
    FIM_PTR<LinkFim> rfim = LinkFim::MakePtr(len);
    std::memcpy(rfim->tail_buf(), newname, len);
    (*rfim)->inode = ino;
    (*rfim)->dir_inode = newparent;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    if (reply->type() != kAttrReplyFim)
      return HandleUnexpectedReply(req, ino, reply, "Link");
    DEBUG_CPFS("Ret", "Link", ino);
    AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*reply);
    client_->cache_mgr()->AddEntry(newparent, newname);
    client_->cache_mgr()->InvalidateInode(ino, false);
    ReplyEntryWithAttr(req, ino, rreply->fa, rreply->dirty);
  }

  /**
   * The FUSE unlink method.
   */
  void Unlink(fuse_req_t req, fuse_ino_t ino, const char* name) {
    DEBUG_CPFS("Run", "Unlink", ino, ": ", name);
    unsigned len = std::strlen(name) + 1;
    FIM_PTR<UnlinkFim> rfim = UnlinkFim::MakePtr(len);
    std::memcpy(rfim->tail_buf(), name, len);
    (*rfim)->inode = ino;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    client_->cache_mgr()->InvalidateInode(ino, true);
    return HandleUnexpectedReply(req, ino, reply, "Unlink", true);
  }

  /**
   * The FUSE rmdir method.
   */
  void Rmdir(fuse_req_t req, fuse_ino_t parent, const char* name) {
    DEBUG_CPFS("Run", "Rmdir", parent, ": ", PHex(parent), ", ", name);
    FIM_PTR<RmdirFim> rfim =
        RmdirFim::MakePtr(std::strlen(name) + 1);
    std::memcpy(rfim->tail_buf(), name, std::strlen(name) + 1);
    (*rfim)->parent = parent;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    return HandleUnexpectedReply(req, parent, reply, "Rmdir", true);
  }

  /**
   * The FUSE rename method.
   */
  void Rename(fuse_req_t req, fuse_ino_t parent, const char* name,
              fuse_ino_t newparent, const char* newname) {
    DEBUG_CPFS("Run", "Rename", parent, ": ", name);
    unsigned len = std::strlen(name) + std::strlen(newname) + 2;
    FIM_PTR<RenameFim> rfim = RenameFim::MakePtr(len);
    std::memcpy(rfim->tail_buf(), name, std::strlen(name) + 1);
    std::memcpy(rfim->tail_buf() + std::strlen(name) + 1,
                newname, std::strlen(newname) + 1);
    (*rfim)->parent = parent;
    (*rfim)->new_parent = newparent;
    (*rfim)->name_len = std::strlen(name);
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    return HandleUnexpectedReply(req, parent, reply, "Rename", true);
  }

  /**
   * The FUSE lookup method.
   */
  void Lookup(fuse_req_t req, fuse_ino_t ino, const char* name) {
    DEBUG_CPFS("Run", "Lookup", ino, ": ", name);
    FSLookupReply ret;
    if (!fs_ll_->Lookup(GetFSIdentity(req), ino, name, &ret))
      return HandleUnexpectedReply(req, ino, ret.reply, "Lookup");
    DEBUG_CPFS("Ret", "Lookup", ino);
    fuse_entry_param entry_param;
    entry_param.ino = ret.inode;
    entry_param.attr = ret.attr;
    entry_param.generation = 1;
    entry_param.attr_timeout = ret.attr_timeout;
    entry_param.entry_timeout = ret.entry_timeout;
    this->ReplyEntry(req, &entry_param);
  }

  /**
   * The FUSE readdir method.
   */
  void Readdir(fuse_req_t req, fuse_ino_t ino, std::size_t size,
               off_t off, fuse_file_info* fi) {
    (void) fi;
    DEBUG_CPFS("Run", "Readdir", ino);
    FIM_PTR<ReaddirFim> rfim = ReaddirFim::MakePtr();
    (*rfim)->inode = ino;
    (*rfim)->size = size;
    (*rfim)->cookie = off;
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    if (reply->type() != kDataReplyFim)
      return HandleUnexpectedReply(req, ino, reply, "Readdir");
    DEBUG_CPFS("Ret", "Readdir", ino);
    const char* data = reply->tail_buf();
    int data_size = reply->tail_buf_size();
    std::vector<char> buf(size);
    int buf_pos = 0;
    struct stat stbuf;
    while (data_size > 0) {
      if (unsigned(data_size) < sizeof(ReaddirRecord))
        throw std::runtime_error("Short direntry found");
      const ReaddirRecord* rec = reinterpret_cast<const ReaddirRecord*>(data);
      data += rec->GetLen();
      data_size -= rec->GetLen();
      if (data_size < 0 || !rec->IsValid())
        throw std::runtime_error("Invalid direntry found");
      stbuf.st_ino = rec->inode;
      stbuf.st_mode = rec->file_type << 12;
      std::size_t ret = this->AddDirentry(req, &buf[buf_pos], size - buf_pos,
                                          rec->name, &stbuf, rec->cookie);
      if (ret > size - buf_pos) {
        if (buf_pos)
          break;
        return void(this->ReplyErr(req, EINVAL));
      }
      buf_pos += ret;
    }
    this->ReplyBuf(req, &buf[0], buf_pos);
  }

  /**
   * The FUSE write method.
   */
  void Write(fuse_req_t req, fuse_ino_t ino, const char* buf, std::size_t size,
            off_t off, fuse_file_info* fi) {
    (void) ino;  // Already in fi
    DEBUG_CPFS("Run", "Write", ino, ": ", PINT(size), ", ", PINT(off));
    FSWriteReply ret;
    if (!fs_ll_->Write(fi->fh, ino, buf, size, off, &ret)) {
      DEBUG_CPFS("Err", "Write", ino, " deferred ", PVal(ret.deferred_errno));
      this->ReplyErr(req, ret.deferred_errno);
      return;
    }
    DEBUG_CPFS("Ret", "Write", ino);
    this->ReplyWrite(req, size);
  }

  /**
   * The FUSE read method.
   */
  void Read(fuse_req_t req, fuse_ino_t ino, std::size_t size,
            off_t off, fuse_file_info* fi) {
    (void) ino;  // Already in fi
    DEBUG_CPFS("Run", "Read", ino, ": ", PINT(size), ", ", PINT(off));
    FSReadvReply ret;
    if (!fs_ll_->Readv(fi->fh, ino, size, off, &ret))
      return HandleUnexpectedReply(req, ino, ret.replies.back(), "Read");
    DEBUG_CPFS("Ret", "Read", ino);
    this->ReplyIov(req, ret.iov.data(), ret.iov.size());
  }

  /**
   * The FUSE mkdir method.
   */
  void Mkdir(fuse_req_t req, fuse_ino_t parent,
             const char* name, mode_t mode) {
    DEBUG_CPFS("Run", "Mkdir", parent, ": ", name);
    FIM_PTR<MkdirFim> rfim =
        MkdirFim::MakePtr(std::strlen(name) + 1);
    std::memcpy(rfim->tail_buf(), name, std::strlen(name) + 1);
    (*rfim)->parent = parent;
    (*rfim)->req.new_inode = 0;
    (*rfim)->req.mode = mode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry;
    {
      boost::unique_lock<MUTEX_TYPE> lock;
      entry = client_->tracker_mapper()->GetMSTracker()->
          AddRequest(rfim, &lock);
      entry->OnAck(boost::bind(&CreateReplyCallback, _1));
    }
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    if (reply->type() != kAttrReplyFim)
      return HandleUnexpectedReply(req, parent, reply, "Mkdir");
    DEBUG_CPFS("Ret", "Mkdir", parent);
    AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*reply);
    client_->cache_mgr()->AddEntry(parent, name);
    ReplyEntryWithAttr(req, rreply->inode, rreply->fa, false);
  }

  /**
   * The FUSE symlink method.
   */
  void Symlink(fuse_req_t req, const char* link, fuse_ino_t parent,
               const char* name) {
    DEBUG_CPFS("Run", "Symlink", parent, ": ", name);
    FIM_PTR<SymlinkFim> rfim =
        SymlinkFim::MakePtr(std::strlen(name) + std::strlen(link) + 2);
    std::memcpy(rfim->tail_buf(), name, std::strlen(name) + 1);
    std::memcpy(rfim->tail_buf() + std::strlen(name) + 1,
                link, std::strlen(link) + 1);
    (*rfim)->inode = parent;
    (*rfim)->name_len = std::strlen(name);
    (*rfim)->new_inode = 0;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry;
    {
      boost::unique_lock<MUTEX_TYPE> lock;
      entry = client_->tracker_mapper()->GetMSTracker()->
          AddRequest(rfim, &lock);
      entry->OnAck(boost::bind(&CreateReplyCallback, _1));
    }
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    if (reply->type() != kAttrReplyFim)
      return HandleUnexpectedReply(req, parent, reply, "Symlink");
    DEBUG_CPFS("Ret", "Symlink", parent);
    AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*reply);
    client_->cache_mgr()->AddEntry(parent, name);
    ReplyEntryWithAttr(req, rreply->inode, rreply->fa, false);
  }

  /**
   * The FUSE symlink method.
   */
  void Readlink(fuse_req_t req, fuse_ino_t inode) {
    DEBUG_CPFS("Run", "Readlink", inode);
    LOG(debug, FS, PVal(GetReqUniq(req)), " Readlink(", PHex(inode), ")");
    FIM_PTR<ReadlinkFim> rfim = ReadlinkFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    if (reply->type() != kDataReplyFim)
      return HandleUnexpectedReply(req, inode, reply, "Readlink");
    DEBUG_CPFS("Ret", "Readlink", inode);
    DataReplyFim& rreply = static_cast<DataReplyFim&>(*reply);
    this->ReplyReadlink(req, rreply.tail_buf());
  }

  /**
   * The FUSE mknod method.
   */
  void Mknod(fuse_req_t req, fuse_ino_t parent, const char* name,
             mode_t mode, dev_t rdev) {
    DEBUG_CPFS("Run", "Mknod", parent, ": ", name);
    FIM_PTR<MknodFim> rfim =
        MknodFim::MakePtr(std::strlen(name) + 1);
    std::memcpy(rfim->tail_buf(), name, std::strlen(name) + 1);
    (*rfim)->inode = parent;
    (*rfim)->new_inode = 0;
    (*rfim)->mode = mode;
    (*rfim)->rdev = rdev;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    (*rfim)->num_ds_groups = 0;
    boost::shared_ptr<IReqEntry> entry;
    {
      boost::unique_lock<MUTEX_TYPE> lock;
      entry = client_->tracker_mapper()->GetMSTracker()->
          AddRequest(rfim, &lock);
      entry->OnAck(boost::bind(&CreateReplyCallback, _1));
    }
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    if (reply->type() != kAttrReplyFim)
      return HandleUnexpectedReply(req, parent, reply, "Mknod");
    DEBUG_CPFS("Ret", "Mknod", parent);
    AttrReplyFim& rreply = static_cast<AttrReplyFim&>(*reply);
    client_->cache_mgr()->AddEntry(parent, name);
    ReplyEntryWithAttr(req, rreply->inode, rreply->fa, false);
  }

  /**
   * The FUSE opendir method.
   */
  void Opendir(fuse_req_t req, fuse_ino_t inode, struct fuse_file_info *fi) {
    DEBUG_CPFS("Run", "Opendir", inode);
    FIM_PTR<OpendirFim> rfim = OpendirFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    if (reply->type() != kResultCodeReplyFim ||
        static_cast<ResultCodeReplyFim&>(*reply)->err_no != 0)
      return HandleUnexpectedReply(req, inode, reply, "Opendir");
    DEBUG_CPFS("Ret", "Opendir", inode);
    this->ReplyOpen(req, fi);
  }

  /**
   * The FUSE access method.
   */
  void Access(fuse_req_t req, fuse_ino_t inode, int mask) {
    DEBUG_CPFS("Run", "Access", inode);
    FIM_PTR<AccessFim> rfim = AccessFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    (*rfim)->mask = mask;
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    return HandleUnexpectedReply(req, inode, reply, "Access", true);
  }

  /**
   * The FUSE statfs method.
   */
  void Statfs(fuse_req_t req, fuse_ino_t inode) {
    DEBUG_CPFS("Run", "Statfs", inode);
    FIM_PTR<FCStatFSFim> rfim = FCStatFSFim::MakePtr();
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    const FCStatFSReplyFim& rreply =
        static_cast<FCStatFSReplyFim&>(*reply);
    if (reply->type() != kFCStatFSReplyFim)
      return HandleUnexpectedReply(req, inode, reply, "Statfs");
    struct statvfs stvfsbuf;
    stvfsbuf.f_bsize = 1024;
    stvfsbuf.f_frsize = 1024;
    stvfsbuf.f_blocks = rreply->total_space / 1024;
    stvfsbuf.f_bfree = rreply->free_space / 1024;
    stvfsbuf.f_bavail = rreply->free_space / 1024;
    stvfsbuf.f_files = rreply->total_inodes;
    stvfsbuf.f_ffree = rreply->free_inodes;
    stvfsbuf.f_favail = rreply->free_inodes;
    stvfsbuf.f_fsid = 0;
    stvfsbuf.f_flag = 0;
    stvfsbuf.f_namemax = 255;
    this->ReplyStatfs(req, &stvfsbuf);
  }

  /**
   * The FUSE setxattr method.
   */
  void Setxattr(fuse_req_t req, fuse_ino_t inode, const char *name,
                const char *value, std::size_t size, int flags) {
    if (!use_xattr_) {
      this->ReplyErr(req, EOPNOTSUPP);
      return;
    }
    DEBUG_CPFS("Run", "Setxattr", inode);
    if (std::strncmp(name, "security.", 9) == 0) {
      this->ReplyErr(req, EOPNOTSUPP);
      return;
    }
    std::size_t name_len = std::strlen(name) + 1;
    FIM_PTR<SetxattrFim> rfim = SetxattrFim::MakePtr(name_len + size);
    (*rfim)->inode = inode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    std::memcpy(rfim->tail_buf(), name, name_len);
    std::memcpy(rfim->tail_buf() + name_len, value, size);
    (*rfim)->name_len = name_len;
    (*rfim)->value_len = size;
    (*rfim)->flags = flags;
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    return HandleUnexpectedReply(req, inode, reply, "Setxattr", true);
  }

  /**
   * The FUSE getxattr method.
   */
  void Getxattr(fuse_req_t req, fuse_ino_t inode, const char *name,
                std::size_t size) {
    if (!use_xattr_) {
      this->ReplyErr(req, EOPNOTSUPP);
      return;
    }
    DEBUG_CPFS("Run", "Getxattr", inode);
    if (std::strncmp(name, "security.", 9) == 0) {
      this->ReplyErr(req, EOPNOTSUPP);
      return;
    }
    std::size_t name_len = strlen(name) + 1;
    FIM_PTR<GetxattrFim> rfim = GetxattrFim::MakePtr(name_len);
    (*rfim)->inode = inode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    std::memcpy(rfim->tail_buf(), name, name_len);
    (*rfim)->value_len = size;
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    const GetxattrReplyFim& rreply = static_cast<GetxattrReplyFim&>(*reply);
    if (reply->type() != kGetxattrReplyFim)
      return HandleUnexpectedReply(req, inode, reply, "Getxattr");
    if (size > 0)
      this->ReplyBuf(req, rreply.tail_buf(), rreply->value_len);
    else
      this->ReplyXattr(req, rreply->value_len);
  }

  /**
   * The FUSE listxattr method.
   */
  void Listxattr(fuse_req_t req, fuse_ino_t inode, std::size_t size) {
    if (!use_xattr_) {
      this->ReplyErr(req, EOPNOTSUPP);
      return;
    }
    DEBUG_CPFS("Run", "Listxattr", inode);
    FIM_PTR<ListxattrFim> rfim = ListxattrFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    (*rfim)->size = size;
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    const ListxattrReplyFim& rreply = static_cast<ListxattrReplyFim&>(*reply);
    if (reply->type() != kListxattrReplyFim)
      return HandleUnexpectedReply(req, inode, reply, "Listxattr");
    if (size > 0)
      this->ReplyBuf(req, rreply.tail_buf(), rreply->size);
    else
      this->ReplyXattr(req, rreply->size);
  }

  /**
   * The FUSE remove xattr method.
   */
  void Removexattr(fuse_req_t req, fuse_ino_t inode, const char* name) {
    if (!use_xattr_) {
      this->ReplyErr(req, EOPNOTSUPP);
      return;
    }
    DEBUG_CPFS("Run", "Removexattr", inode);
    if (std::strncmp(name, "security.", 9) == 0) {
      this->ReplyErr(req, EOPNOTSUPP);
      return;
    }
    std::size_t name_len = strlen(name) + 1;
    FIM_PTR<RemovexattrFim> rfim = RemovexattrFim::MakePtr(name_len);
    (*rfim)->inode = inode;
    (*rfim)->context.FromFuseCtx(this->ReqCtx(req));
    std::memcpy(rfim->tail_buf(), name, name_len);
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    return HandleUnexpectedReply(req, inode, reply, "Removexattr", true);
  }

  /**
   * Get the specified list of meta servers.
   */
  const std::vector<std::string>& GetMetaServers() const {
    return meta_servers_;
  }

  /**
   * Get the log-level
   */
  const char* GetLogLevel() const {
    return log_level_.c_str();
  }

 private:
  BaseFSClient* client_;

  std::vector<std::string> meta_servers_;
  std::string log_level_;
  double heartbeat_interval_;
  double socket_read_timeout_;
  bool use_xattr_;
  boost::scoped_ptr<IFSCommonLL> fs_ll_;
  /**
   * Common routine for handling unexpected replies.
   *
   * @param req The FUSE request being handled.
   *
   * @param ino The inode number of the file operated.
   *
   * @param reply The reply.
   *
   * @param allow_zero Whether a ResultCodeReplyFim of zero err_no
   * (indicating no error) is allowed.  If not, the err_no will be
   * changed to EIO instead, and an error is logged.
   */
  void HandleUnexpectedReply(fuse_req_t req, fuse_ino_t ino,
                             FIM_PTR<IFim> reply,
                             const char* call_name, bool allow_zero = false) {
    int err_no = EIO;
    if (reply->type() == kResultCodeReplyFim) {
      err_no = static_cast<ResultCodeReplyFim&>(*reply)->err_no;
      if (err_no == 0 && !allow_zero) {
        err_no = EIO;
        LOG(error, FS, "FUSE ", PVal(GetReqUniq(req)),
            ": Unexpected zero error from ", call_name, "(", PHex(ino), ")");
      } else if (err_no != 0) {
        DEBUG_CPFS("Err", call_name, ino, ": errno = ", PINT(err_no));
      } else {
        DEBUG_CPFS("Ret", call_name, ino);
      }
    } else {
      LOG(error, FS, "FUSE ", PVal(GetReqUniq(req)),
          ": Unexpected reply from ", call_name, "(", PHex(ino),
          "), type = ", PINT(reply->type()));
    }
    this->ReplyErr(req, err_no);
  }

  /**
   * Make a reply with file attributes.
   *
   * @param inode The inode owning the file attributes
   *
   * @param fa The file attributes
   *
   * @param attr_dirty Whether to prevent caching of the attribute
   */
  void ReplyEntryWithAttr(fuse_req_t req, InodeNum inode, const FileAttr& fa,
                          bool attr_dirty) {
    fuse_entry_param entry_param;
    fa.ToStat(inode, &entry_param.attr);
    entry_param.ino = inode;
    entry_param.generation = 1;
    entry_param.attr_timeout = attr_dirty ? 0 : 3600.0;
    entry_param.entry_timeout = 3600.0;
    this->ReplyEntry(req, &entry_param);
  }

  /**
   * Get unique ID.  FUSE provide no such API, so we have to hack our own.
   */
  static uint64_t GetReqUniq(fuse_req_t req) {
    char* req2 = reinterpret_cast<char*>(req);
    int* ptr;
    req2 += sizeof(ptr);
    return *reinterpret_cast<uint64_t*>(req2);
  }

  /**
   * Helper function to get FSIdentity
   */
  FSIdentity GetFSIdentity(fuse_req_t req) {
    return FSIdentity(this->ReqCtx(req)->uid, this->ReqCtx(req)->gid);
  }
};

#undef DEBUG_CPFS

}  // namespace client
}  // namespace cpfs
