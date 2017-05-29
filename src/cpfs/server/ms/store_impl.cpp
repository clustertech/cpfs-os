/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IStore interface.
 */

#include "server/ms/store_impl.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>

#include <fuse/fuse_lowlevel.h>

#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/xattr.h>

#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <list>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/scope_exit.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "dir_iterator_impl.hpp"
#include "finfo.hpp"
#include "logger.hpp"
#include "store_util.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/ms/dsg_alloc.hpp"
#include "server/ms/fs_compat.hpp"  // IWYU pragma: keep
#include "server/ms/inode_mutex.hpp"
#include "server/ms/inode_src.hpp"
#include "server/ms/inode_usage.hpp"
#include "server/ms/store.hpp"
#include "server/ms/ugid_handler.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Number of inode locks.  They are currently used only when
 * performing directory operations on directory inodes.
 */
const int kNumInodeLocks = 16384;

/**
 * Open flags that is meaningful when applying to the inode file.
 */
const int kInodeOpenFlags = (O_ACCMODE | O_APPEND | O_DIRECT | O_LARGEFILE |
                             O_NOATIME | O_TRUNC);

/**
 * Implement interface for meta-server directory operations.  Store
 * the meta-data information in two levels: the first level called
 * inode file / directory, and the second level called dentry symlink
 * / directory.  Each inode file / directory has a corresponding
 * control file for storing extra information.  The inode files /
 * directories are created and removed with server permission, and is
 * chown'ed and chmod'ed appropriately afterwards.  The control files
 * are also modified in server permission.  Every other operation is
 * performed with context user permission.
 */
class Store : public IStore {
 public:
  /**
   * @param data_path The data directory to use.
   */
  explicit Store(std::string data_path)
      : data_path_(data_path), uid_(geteuid()), gid_(getegid()),
        durable_range_(0), inode_mutex_array_(kNumInodeLocks),
        resync_dt_pending_(false) {}
  bool IsInitialized();
  void Initialize();
  void PrepareUUID();
  std::string GetUUID() const;
  std::map<std::string, std::string> LoadAllUUID();
  void PersistAllUUID(const std::map<std::string, std::string>& uuids);
  void SetUgidHandler(IUgidHandler* handler) {
    ugid_handler_ = handler;
  }
  void SetInodeSrc(IInodeSrc* inode_src) {
    inode_src_ = inode_src;
  }
  void SetDSGAllocator(IDSGAllocator* allocator) {
    dsg_allocator_ = allocator;
  }
  void SetInodeUsage(IInodeUsage* inode_usage) {
    inode_usage_ = inode_usage;
  }
  void SetInodeRemovalTracker(IInodeRemovalTracker* tracker) {
    inode_removal_tracker_ = tracker;
  }
  void SetDurableRange(IDurableRange* durable_range) {
    durable_range_ = durable_range;
  }
  IDirIterator* List(const std::string& path);
  IDirIterator* InodeList(const std::vector<InodeNum>& ranges);
  int GetInodeAttr(InodeNum inode, FileAttr* fa_ret);
  int UpdateInodeAttr(InodeNum inode, FSTime mtime, uint64_t size);
  int GetFileGroupIds(InodeNum inode, std::vector<GroupId>* group_ids);
  int SetLastResyncDirTimes();
  int RemoveInodeSince(const std::vector<InodeNum>& ranges, uint64_t ctime);

  void OperateInode(OpContext* context, InodeNum inode);
  int Attr(OpContext* context, InodeNum inode,
           const FileAttr* fa, uint32_t fa_mask, bool locked, FileAttr* fa_ret);
  int SetXattr(OpContext* context, InodeNum inode,
               const char* name, const char* value,
               std::size_t size, int flags);
  int GetXattr(OpContext* context, InodeNum inode,
               const char* name, char* buf, std::size_t buf_size);
  int ListXattr(OpContext* context, InodeNum inode,
                char* buf, std::size_t buf_size);
  int RemoveXattr(OpContext* context, InodeNum inode, const char* name);

  int Open(OpContext* context, InodeNum inode, int32_t flags,
           std::vector<GroupId>* group_ids, bool* truncated_ret);
  int Access(OpContext* context, InodeNum inode, int32_t mask);
  int AdviseWrite(OpContext* context, InodeNum inode, uint64_t off);
  int FreeInode(InodeNum inode);

  int Lookup(OpContext* context, InodeNum inode, const char* name,
             InodeNum* inode_found, FileAttr* fa);
  int Opendir(OpContext* context, InodeNum inode);
  int Readdir(OpContext* context, InodeNum inode, DentryCookie cookie,
              std::vector<char>* buf);
  int Readlink(OpContext* context, InodeNum inode,
               std::vector<char>* buf);

  int Create(OpContext* context, InodeNum inode, const char* name,
             const CreateReq* create_req, std::vector<GroupId>* group_ids,
             FileAttr* fa_ret,
             boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret);
  int Mkdir(OpContext* context, InodeNum inode, const char* name,
            const CreateDirReq* create_req, FileAttr* fa_ret);
  int Symlink(OpContext* context, InodeNum parent, const char* name,
              InodeNum new_inode, const char* target, FileAttr* fa_ret);
  int Mknod(OpContext* context, InodeNum parent, const char* name,
            InodeNum new_inode, uint64_t mode, uint64_t rdev,
            std::vector<GroupId>* group_ids, FileAttr* fa_ret);
  int ResyncInode(InodeNum inode, const FileAttr& fa,
                  const char* buf, std::size_t extra_size,
                  std::size_t buf_size);
  int ResyncDentry(uint32_t uid, uint32_t gid,
                   InodeNum parent, InodeNum target, unsigned char ftype,
                   const char* dentry_name);
  int ResyncRemoval(InodeNum inode);

  int Link(OpContext* context, InodeNum inode, InodeNum dir_inode,
           const char* name, FileAttr* fa);
  int Rename(OpContext* context, InodeNum parent, const char* name,
             InodeNum newparent, const char* newname, InodeNum* moved_ret,
             boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret);

  int Unlink(OpContext* context, InodeNum parent, const char* name,
             boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret);
  int Rmdir(OpContext* context, InodeNum parent, const char* name);
  int Stat(uint64_t* total_inodes, uint64_t* free_inodes);

 private:
  const std::string data_path_;
  const uid_t uid_;
  const gid_t gid_;
  IUgidHandler* ugid_handler_; /**< Change uid, gid and supplementary groups */
  IDSGAllocator* dsg_allocator_; /**< Allocate groups */
  IInodeSrc* inode_src_; /**< Record used inodes */
  IInodeUsage* inode_usage_; /**< Track inode usage by FCs */
  IInodeRemovalTracker* inode_removal_tracker_; /**< Track removed inodes */
  IDurableRange* durable_range_; /**< Track recently modified inode ranges */
  /**
   * Avoid unwanted concurrency of inodes.  For directory, this lock
   * is held when entries to be modified, to avoid races that the
   * entry is invalidated by the time the operation is performed.  It
   * is also held when the directory is to be removed, so as to avoid
   * races that one operation create entries and another tries to
   * remove the directory.  For files, this lock is held when
   * directory entries are created or removed for it, so as to ensure
   * correct handling of suffixed inode files.
   */
  InodeMutexArray inode_mutex_array_;
  /**
   * Time values of the last resync directory
   */
  bool resync_dt_pending_;  /**< Whether time set is pending */
  std::string resync_dt_inode_path_;  /**< The inode path */
  FSTime resync_dt_atim_;  /**< The atime */
  FSTime resync_dt_mtim_;  /**< The mtime */
  FSTime resync_dt_ctim_;  /**< The ctime */
  uint64_t last_change_; /**< Last change time persisted */
  /**
   * The UUID identity for this server
   */
  std::string uuid_;

  /**
   * Set the FS ids.
   */
  UNIQUE_PTR<IUgidSetterGuard> SetFsIds(OpContext* context) {
    return ugid_handler_->SetFsIds(context->req_context->uid,
                                   context->req_context->gid);
  }

  /**
   * Allocate DS groups to a newly created file.  If group_ids is
   * already filled, use it.  Otherwise, use dsg_allocator to
   * allocate a set of group ids.
   *
   * @param inode_path The path to the inode file
   *
   * @param parent_path The path to the parent directory
   *
   * @param group_ids The group IDs
   *
   * @return On success, 0, otherwise -errno
   */
  int AllocateGroup(const std::string& inode_path,
                     const std::string& parent_path,
                     std::vector<GroupId>* group_ids) {
    if (group_ids->size() == 0) {
      std::size_t num_groups = 1;
      char buf[4] = {'\0'};
      int buf_len = lgetxattr(parent_path.c_str(), "user.ndsg", buf, 4);
      if (buf_len > 0 && buf_len < 4) {  // Use at most 3 digits
        char* endptr;
        errno = 0;
        unsigned long ret  // NOLINT(runtime/int)
            = std::strtoul(buf, &endptr, 10);
        num_groups = (buf[0] == '\0' || errno || *endptr != '\0' || ret == 0)
                      ? 1 : ret;
      }
      *group_ids = dsg_allocator_->Allocate(num_groups);
    }
    if (group_ids->empty())
      return -ENOSPC;
    SetFileGroupIds(inode_path, *group_ids);
    return 0;
  }

  void CreateEmptyDirectory(const std::string& dir) {
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST)
      throw std::runtime_error(
          (boost::format("Cannot create directory %s") % dir.c_str()).str());
  }

  void CreateEmptyFile(const std::string& filename) {
    int fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd > 0)  // Ignore error, will trigger other errors later anyway
      close(fd);
  }

  std::string GetRoot(const std::string& bucket = "000") {
    return data_path_ + "/" + bucket;
  }

  std::string GetInodePath(InodeNum inode) {
    std::string inode_s = GetInodeStr(inode);
    return GetRoot(inode_s.substr(0, 3)) + "/" + inode_s;
  }

  /**
   * Update some of the file times according to the optime in the
   * context.
   *
   * @param inode The inode to have file times modified.
   *
   * @param context Where to find the current time as seen by client.
   *
   * @param mask Bitwise or of FUSE_SET_ATTR_ATIME and
   * FUSE_SET_ATTR_MTIME to specify which time is to be set.  The
   * ctime is always updated.
   */
  void UpdateFileTimes(InodeNum inode, OpContext* context,
                       uint32_t mask) {
    std::string inode_path = GetInodePath(inode);
    const FSTime& optime = context->req_context->optime;
    SetFileTimes(inode_path, mask, optime, optime, optime);
  }

  /**
   * Set file time to specific values.
   *
   * @param mask Bitwise or of FUSE_SET_ATTR_ATIME and
   * FUSE_SET_ATTR_MTIME to specify which time is to be set.  The
   * ctime is always updated.
   *
   * @param inode_path The path of the inode file
   *
   * @param atime The new atime
   *
   * @param mtime The new mtime
   *
   * @param ctime The new ctime
   */
  void SetFileTimes(const std::string& inode_path, uint32_t mask,
                    const FSTime& atime, const FSTime& mtime,
                    const FSTime& ctime) {
    // The ctime is always updated.
    if (!SetCtrlCtime(inode_path, ctime))
      LOG(warning, Store, "SetFileTimes: lsetxattr on ", inode_path,
          "x user.ct failed with error ", PVal(errno));

    if (!(mask & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)))
      return;
    struct timespec time_buf[2] = {{0, UTIME_OMIT}, {0, UTIME_OMIT}};
    if ((mask & FUSE_SET_ATTR_ATIME))
      atime.ToTimeSpec(&time_buf[0]);
    if ((mask & FUSE_SET_ATTR_MTIME))
      mtime.ToTimeSpec(&time_buf[1]);
    utimensat(AT_FDCWD, inode_path.c_str(), time_buf, AT_SYMLINK_NOFOLLOW);
  }

  /**
   * Operations to do after creating an inode file.
   *
   * @param inode The inode number of the inode file
   *
   * @param context The operation context under which the file is created
   *
   * @param mode The target permission of the file.  If uint64_t(-1),
   * don't set the permission (typically, for symlinks).
   */
  void PostCreateInode(InodeNum inode, OpContext* context,
                       uint64_t mode) {
    std::string inode_path = GetInodePath(inode);
    lchown(inode_path.c_str(), context->req_context->uid,
           context->req_context->gid);
    if (mode != uint64_t(-1))
      chmod(inode_path.c_str(), mode);
    std::string ctrl_path = inode_path + "x";
    CreateEmptyFile(ctrl_path);
    UpdateFileTimes(inode, context, FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME);
  }

  /**
   * Create a dentry.  Would fail if dentry already exist.
   *
   * @param context The operation context under which the operation is
   * performed
   *
   * @param dir_inode The inode of the directory holding the dentry
   *
   * @param name The name of the dentry
   *
   * @param inode The inode number of the dentry
   *
   * @param ftype The <dirent.h> type of the dentry
   *
   * @return On success, 0, otherwise -errno
   */
  int CreateDentry(OpContext* context, InodeNum dir_inode,
                   const char* name, unsigned char ftype, InodeNum inode) {
    std::string dentry_path = GetInodePath(dir_inode) + "/" + name;
    UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
    context->i_mutex_guard.reset(new InodeMutexGuard(
        &inode_mutex_array_, dir_inode, inode));
    if (symlink(GetInodeFTStr(inode, ftype).c_str(), dentry_path.c_str()) != 0)
      return -errno;
    return 0;
  }

  /**
   * Check if an inode directory is empty.  Also return true if a
   * normal error occurs.
   *
   * This function assume that appropriate mutex are already held.
   *
   * @param inode The inode number of the directory
   */
  bool IsInodeDirEmpty(InodeNum inode) {
    std::string inode_path = GetInodePath(inode);
    boost::scoped_ptr<IDirIterator> dir_iter(MakeDirIterator(inode_path));
    std::string entry_name;
    bool entry_isdir;
    return !(dir_iter->GetNext(&entry_name, &entry_isdir));
  }

  /**
   * Work to be done before removing entries in a directory.
   * Currently it handles the case of removing entries in a sticky
   * directory, where the inode ownership must be copied to the entry.
   *
   * @param parent_path The inode directory of the sticky directory
   *
   * @param dentry_path The path to the directory entry to be removed
   *
   * @param inode The inode number of the directory entry
   */
  void PreRemoveDentry(const std::string& parent_path,
                       const std::string& dentry_path, InodeNum inode) {
    struct stat stbuf;
    lstat(parent_path.c_str(), &stbuf);
    if (stbuf.st_mode & S_ISVTX) {  // Parent sticky, copy inode ownership
      lstat(GetInodePath(inode).c_str(), &stbuf);
      lchown(dentry_path.c_str(), stbuf.st_uid, -1);
    }
  }

  /**
   * Unlink inode file routine, to be called after a dentry is
   * removed.  This removes one of the suffixed / unsuffixed inode
   * file, unless it is the last one and some FC has opened it.  If
   * the unsuffixed inode file is removed, a RemovedInodeInfo entry is
   * created to return information of the removed file.
   *
   * This function assume that appropriate mutex are already held.
   *
   * @param context The operation context under which the unlink is
   * performed, used to modify the file ctime if nlink is updated and
   * the file is not removed
   *
   * @param file_inode The inode removed
   *
   * @param removed_info_ret Where to put the unlinked file info
   */
  void PostUnlinkDentry(OpContext* context, InodeNum file_inode,
                        boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret);
};

bool Store::IsInitialized() {
  struct stat buf;
  if (stat(data_path_.c_str(), &buf) == -1)
    return false;
  if (stat((GetRoot() + "/" + GetInodeStr(1)).c_str(), &buf) == -1)
    return false;
  if (!S_ISDIR(buf.st_mode))
    return false;
  return true;
}

void Store::Initialize() {
  CreateEmptyDirectory(data_path_);
  for (unsigned d_idx = 0; d_idx < kNumBaseDir; ++d_idx) {
    char buf[4];
    std::snprintf(buf, sizeof(buf), "%03x", d_idx);
    CreateEmptyDirectory(data_path_ + "/" + std::string(buf));
  }
  std::string inode_str = GetInodeStr(1);
  std::string root_inode_path = GetRoot() + "/" + inode_str;
  CreateEmptyDirectory(root_inode_path);
  CreateEmptyFile(root_inode_path + "x");
  if (!IsInitialized())
    throw std::runtime_error("Failed initializing meta directory");
  ReqContext req_context;
  req_context.optime.FromNow();
  OpContext op_context;
  op_context.req_context = &req_context;
  SetDirParent(root_inode_path, inode_str);
  UpdateFileTimes(1, &op_context, FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME);
}

void Store::PrepareUUID() {
  uuid_ = LoadOrCreateUUID(data_path_);
  LOG(notice, Server, "Meta Server UUID: ", uuid_.c_str());
}

std::string Store::GetUUID() const {
  return uuid_;
}

std::map<std::string, std::string> Store::LoadAllUUID() {
  std::map<std::string, std::string> ret;
  {
    FILE* fp = fopen((data_path_ + "/u").c_str(), "r");
    if (!fp)
      return ret;
    BOOST_SCOPE_EXIT(&fp) {
      fclose(fp);
    } BOOST_SCOPE_EXIT_END;
    while (!feof(fp)) {
      char line[64] = {'\0'};
      fgets(line, 64U, fp);
      unsigned line_len = strlen(line);
      if (line_len < 40)
        continue;
      std::string line_s(line, 0, line_len - 1);  // Ignore '\n'
      std::string uuid = line_s.substr(0, 36U);
      std::string role = line_s.substr(37U);
      ret[role] = uuid;
    }
  }
  return ret;
}

void Store::PersistAllUUID(const std::map<std::string, std::string>& uuids) {
  FILE* fp = fopen((data_path_ + "/u.tmp").c_str(), "w");
  if (!fp)
    throw std::runtime_error("Cannot persist UUID information");
  BOOST_SCOPE_EXIT(&data_path_, &fp) {
    if (fp) {
      std::fflush(fp);
      fsync(fileno(fp));
      fclose(fp);
      rename((data_path_ + "/u.tmp").c_str(), (data_path_ + "/u").c_str());
    }
  } BOOST_SCOPE_EXIT_END;
  for (std::map<std::string, std::string>::const_iterator itr = uuids.begin();
       itr != uuids.end(); ++itr) {
    const std::string& role = itr->first;
    const std::string& uuid = itr->second;
    // Persist in readable text format
    fprintf(fp, "%s %s\n", uuid.c_str(), role.c_str());
  }
}

IDirIterator* Store::List(const std::string& path) {
  std::string dir_path = path == "" ? data_path_ : data_path_ + "/" + path;
  return path == "" ?
      MakeLeafViewDirIterator(dir_path) : MakeDirIterator(dir_path);
}

IDirIterator* Store::InodeList(const std::vector<InodeNum>& ranges) {
  return MakeInodeRangeDirIterator(data_path_, ranges);
}

int Store::GetInodeAttr(InodeNum inode, FileAttr* fa_ret) {
  return GetFileAttr(GetInodePath(inode), fa_ret);
}

int Store::UpdateInodeAttr(InodeNum inode, FSTime mtime, uint64_t size) {
  std::string path = GetInodePath(inode);
  int ret = 0;
  if (truncate(path.c_str(), size) == -1)
    ret = -errno;
  SetFileTimes(path, FUSE_SET_ATTR_MTIME, mtime, mtime, mtime);
  return ret;
}

int Store::GetFileGroupIds(InodeNum inode, std::vector<GroupId>* group_ids) {
  return cpfs::GetFileGroupIds(GetInodePath(inode), group_ids);
}

int Store::SetLastResyncDirTimes() {
  if (resync_dt_pending_) {
    SetFileTimes(resync_dt_inode_path_,
                 FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME,
                 resync_dt_atim_, resync_dt_mtim_, resync_dt_ctim_);
    resync_dt_pending_ = false;
  }
  return 0;
}

int Store::RemoveInodeSince(const std::vector<InodeNum>& ranges,
                            uint64_t ctime) {
  inode_removal_tracker_->SetPersistRemoved(true);
  durable_range_->SetConservative(true);
  boost::scoped_ptr<IDirIterator> dir_iter(InodeList(ranges));
  dir_iter->SetFilterCTime(ctime);
  std::string inode_path;
  bool is_dir;
  int last_err = 0;
  while (dir_iter->GetNext(&inode_path, &is_dir)) {
    std::string inode_name = StripPrefixInodeStr(inode_path);
    InodeNum inode;
    ParseInodeNum(inode_name.c_str(), inode_name.length(), &inode);
    inode_removal_tracker_->RecordRemoved(inode);
    std::string to_remove = data_path_ + "/" + inode_path;
    int ret = is_dir ? RemoveDirInode(to_remove) : unlink(to_remove.c_str());
    if (ret != 0)
      last_err = ret;
  }
  return last_err;
}

void Store::OperateInode(OpContext* context, InodeNum inode) {
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
}

int Store::Attr(OpContext* context, InodeNum inode,
                const FileAttr* fa, uint32_t fa_mask,
                bool locked, FileAttr* fa_ret) {
  std::string path = GetInodePath(inode);
  context->inodes_read.push_back(inode);
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  if (fa) {
    UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
    if ((fa_mask & FUSE_SET_ATTR_MTIME) && !locked) {
      context->i_mutex_guard.reset();
      throw MDNeedLock("Lock is required for setting mtime");
    }
    // For checking links and getting missing atime / mtime
    struct stat file_stat;
    if (lstat(path.c_str(), &file_stat) == -1)
      return -errno;
    // Set ids first to trigger errors early
    unsigned ids_to_set = fa_mask & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID);
    if (ids_to_set) {
      uid_t uid = fa->uid;
      uid_t gid = fa->gid;
      if (ids_to_set != (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
        if (ids_to_set & FUSE_SET_ATTR_UID)  // gid not set
          gid = -1;
        else
          uid = -1;
      }
      if (lchown(path.c_str(), uid, gid) == -1)
        return -errno;
    }
    if (!S_ISLNK(file_stat.st_mode) && (fa_mask & FUSE_SET_ATTR_SIZE)) {
      if (!locked) {
        context->i_mutex_guard.reset();
        throw MDNeedLock("Lock is required for truncate");
      }
      if (truncate(path.c_str(), fa->size) == -1)
        return -errno;
    }
    if (!S_ISLNK(file_stat.st_mode) && (fa_mask & FUSE_SET_ATTR_MODE))
      if (chmod(path.c_str(), fa->mode) == -1)
        return -errno;  // Test non-owner chmod, can't test in unit test

    guard.reset();
    if (durable_range_)
      durable_range_->Add(inode);
    SetFileTimes(path, fa_mask, fa->atime, fa->mtime,
                 context->req_context->optime);
    context->inodes_changed.push_back(inode);
  }
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  return GetFileAttr(path, fa_ret);
}

int Store::SetXattr(OpContext* context, InodeNum inode,
                    const char* name, const char* value,
                    std::size_t size, int flags) {
  std::string inode_path = GetInodePath(inode);
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  int ret = lsetxattr(inode_path.c_str(), name, value, size, flags);
  if (ret != 0)
    return ret;
  if (durable_range_)
    durable_range_->Add(inode);
  context->inodes_changed.push_back(inode);
  return 0;
}

int Store::GetXattr(OpContext* context, InodeNum inode,
                    const char* name, char* buf, std::size_t buf_size) {
  std::string inode_path = GetInodePath(inode);
  context->inodes_read.push_back(inode);
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  ssize_t ret = lgetxattr(inode_path.c_str(), name, buf, buf_size);
  return ret == -1 ? -errno : ret;
}

int Store::ListXattr(OpContext* context, InodeNum inode,
                     char* buf, std::size_t buf_size) {
  std::string inode_path = GetInodePath(inode);
  context->inodes_read.push_back(inode);
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  boost::scoped_array<char> local_buf;
  ssize_t rsize;
  while (errno != ERANGE) {
    rsize = llistxattr(inode_path.c_str(), 0, 0);
    if (rsize == -1)
      return -errno;
    local_buf.reset(new char[rsize]);
    rsize = llistxattr(inode_path.c_str(), local_buf.get(), rsize);
    if (rsize != -1)
      break;
  }
  char* src = local_buf.get();
  char* dest = src;
  char* end = src + rsize;
  while (src < end) {
    size_t n = std::strlen(src);
    if (std::strncmp(src, "security.", 9) != 0) {
      std::memmove(dest, src, n + 1);
      dest += n + 1;
    }
    src += n + 1;
  }
  rsize = dest - local_buf.get();
  if (buf_size == 0)
    return rsize;
  if (std::size_t(rsize) > buf_size)
    return -ERANGE;
  std::memcpy(buf, local_buf.get(), rsize);
  return rsize;
}

int Store::RemoveXattr(OpContext* context, InodeNum inode, const char* name) {
  std::string inode_path = GetInodePath(inode);
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  if (durable_range_)
    durable_range_->Add(inode);
  int ret = lremovexattr(inode_path.c_str(), name);
  if (ret != 0)
    return -errno;
  context->inodes_changed.push_back(inode);
  return 0;
}

int Store::Open(OpContext* context, InodeNum inode, int32_t flags,
                std::vector<GroupId>* group_ids, bool* truncated_ret) {
  std::string inode_path = GetInodePath(inode);
  const char* inode_path_s = inode_path.c_str();
  context->inodes_read.push_back(inode);
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  struct stat stbuf;
  {
    // Guard against client bug
    if (lstat(inode_path_s, &stbuf) != 0 || !S_ISREG(stbuf.st_mode))
      return -EIO;
    int fd = open(inode_path_s, flags & kInodeOpenFlags);
    if (fd < 0)
      return -errno;
    BOOST_SCOPE_EXIT(&fd) {
      close(fd);
    } BOOST_SCOPE_EXIT_END;
    int ret = GetFileGroupIds(inode, group_ids);
    if (ret < 0)
      return -EIO;
  }
  guard.reset();
  *truncated_ret = false;
  if (flags & O_TRUNC) {
    if (durable_range_)
      durable_range_->Add(inode);
    if (stbuf.st_size != 0)
      *truncated_ret = true;
    int acc_mode = flags & O_ACCMODE;
    if (acc_mode == O_WRONLY || acc_mode == O_RDWR)
      UpdateFileTimes(inode, context, FUSE_SET_ATTR_MTIME);
  }
  context->inodes_changed.push_back(inode);
  return 0;
}

int Store::Access(OpContext* context, InodeNum inode, int32_t mask) {
  std::string inode_path = GetInodePath(inode);
  context->inodes_read.push_back(inode);
  struct stat stbuf;
  // For root access, special rules are required, and we don't want to
  // implement them.  Just directly call access() instead.  We trust
  // that FUSE dereferenced symlinks for us already.
  if (context->req_context->uid == 0)
    return access(inode_path.c_str(), mask) == 0 ? 0 : -errno;
  int ret = lstat(inode_path.c_str(), &stbuf);
  if (ret)
    return -errno;
  if (mask == F_OK)
    return 0;
  mask = mask & 7;
  if (stbuf.st_uid == context->req_context->uid)
    return (mask & int32_t(stbuf.st_mode >> 6)) == mask ? 0 : -EACCES;
  if (stbuf.st_gid == context->req_context->gid
      || ugid_handler_->HasSupplementaryGroup(context->req_context->uid,
                                              stbuf.st_gid))
    return (mask & int32_t(stbuf.st_mode >> 3)) == mask ? 0 : -EACCES;
  return (mask & int32_t(stbuf.st_mode)) == mask ? 0 : -EACCES;
}

int Store::AdviseWrite(OpContext* context, InodeNum inode, uint64_t off) {
  // Do not use a IUgidSetterGuard here: Write does not need extra
  // permission, once Open for write succeeds one should be able to do
  // all Write's it wants
  std::string inode_path = GetInodePath(inode);
  context->inodes_read.push_back(inode);
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  struct stat stbuf;
  if (lstat(inode_path.c_str(), &stbuf) != 0)
    return -errno;
  if (!S_ISREG(stbuf.st_mode))  // Only regular file can be written
    return -EIO;
  if (uint64_t(stbuf.st_size) < off && truncate(inode_path.c_str(), off) != 0)
    return -errno;
  if (durable_range_)
    durable_range_->Add(inode);
  context->inodes_changed.push_back(inode);
  UpdateFileTimes(inode, context, FUSE_SET_ATTR_MTIME);
  return 0;
}

int Store::FreeInode(InodeNum inode) {
  std::string inode_path = GetInodePath(inode);
  int ret = unlink(inode_path.c_str());
  unlink((inode_path + "x").c_str());
  if (ret != 0) {
    LOG(warning, Store, "Cannot remove ", inode_path, ", error ", PVal(ret));
    return ret;
  }
  inode_src_->NotifyRemoved(inode);
  inode_removal_tracker_->RecordRemoved(inode);
  return 0;
}

int Store::Readlink(OpContext* context, InodeNum inode,
                    std::vector<char>* buf_ret) {
  std::string inode_path = GetInodePath(inode);
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->inodes_read.push_back(inode);
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  return ReadLinkTarget(GetInodePath(inode), buf_ret);
}

int Store::Lookup(OpContext* context, InodeNum inode, const char* name,
                  InodeNum* inode_found, FileAttr* fa) {
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->inodes_read.push_back(inode);
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  struct stat stbuf;
  int d2i_ret = DentryToInode(GetInodePath(inode) + "/" + name, inode_found,
                              &stbuf);
  if (d2i_ret)
    return d2i_ret;
  std::string inode_path = GetInodePath(*inode_found);
  return GetFileAttr(inode_path, fa);
}


int Store::Opendir(OpContext* context, InodeNum inode) {
  DIR* dir;
  std::string inode_s = GetInodePath(inode);
  UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
  context->inodes_read.push_back(inode);
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  if ((dir = opendir(inode_s.c_str())) == NULL)
    return -errno;
  BOOST_SCOPE_EXIT(&dir) {
    closedir(dir);
  } BOOST_SCOPE_EXIT_END;
  return 0;
}

int Store::Readdir(OpContext* context, InodeNum inode, DentryCookie cookie,
                   std::vector<char>* buf) {
  DIR* dir;
  std::string inode_s = GetInodePath(inode);
  const char* inode_str = inode_s.c_str();
  context->inodes_read.push_back(inode);
  context->i_mutex_guard.reset(new InodeMutexGuard(&inode_mutex_array_, inode));
  if ((dir = opendir(inode_str)) == NULL)
    return -errno;
  BOOST_SCOPE_EXIT(&dir) {
    closedir(dir);
  } BOOST_SCOPE_EXIT_END;
  if (cookie != 0)
    seekdir(dir, cookie);
  int name_max = pathconf(inode_str, _PC_NAME_MAX);
  name_max = name_max == -1 ? NAME_MAX : name_max;
  // +8 instead of +1 to avoid Valgrind problems due to odd allocation size
  boost::scoped_array<char>
      entrya(new char[offsetof(dirent, d_name) + name_max + 8]);
  dirent* entry = reinterpret_cast<dirent*>(&entrya[0]);
  dirent* result;

  int pos = 0;
  for (;;) {
    int ret2 = readdir_r(dir, entry, &result);
    if (ret2 != 0 || result == NULL)
      return (ret2 == 0 || pos) ? pos : -errno;
    std::size_t ret_len = ReaddirRecord::GetLenForName(entry->d_name);
    if (ret_len + pos > buf->size())
      return pos ? pos : -EINVAL;
    ReaddirRecord& rec = reinterpret_cast<ReaddirRecord&>((*buf)[pos]);
    pos += ret_len;
    std::string dentry_path = inode_s + '/' + result->d_name;
    if (std::strcmp(entry->d_name, ".") == 0) {
      rec.inode = inode;
    } else if (std::strcmp(entry->d_name, "..") == 0) {
      rec.inode = GetDirParent(inode_s);
    } else {
      int x2i_ret;
      if (result->d_type == DT_DIR) {
        x2i_ret = XattrToInode(dentry_path, &rec.inode);
      } else {
        x2i_ret = SymlinkToInodeFT(dentry_path, &rec.inode, &result->d_type);
        if (x2i_ret)  // Unknown. Guess ino (directory)
          x2i_ret = XattrToInode(dentry_path, &rec.inode);
      }
      if (x2i_ret)
        return x2i_ret;
    }
    rec.cookie = result->d_off;
    rec.name_len = std::strlen(result->d_name);
    rec.file_type = result->d_type;
    strncpy(rec.name, result->d_name, rec.name_len + 1);
  }
}

int Store::Create(OpContext* context, InodeNum inode, const char* name,
                  const CreateReq* create_req,
                  std::vector<GroupId>* group_ids,
                  FileAttr* fa_ret,
                  boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret) {
  std::string new_inode_path = GetInodePath(create_req->new_inode);
  int fd = open(new_inode_path.c_str(),
                O_CREAT | O_EXCL | (create_req->flags & kInodeOpenFlags),
                create_req->mode);
  if (fd == -1)
    return -errno;
  close(fd);
  PostCreateInode(create_req->new_inode, context, create_req->mode);
  int ret = -EIO;
  BOOST_SCOPE_EXIT(&ret, &new_inode_path) {
    if (ret != 0) {
      unlink(new_inode_path.c_str());
      unlink((new_inode_path + "x").c_str());
    }
  } BOOST_SCOPE_EXIT_END;
  ret = AllocateGroup(new_inode_path, GetInodePath(inode), group_ids);
  if (ret != 0)
    return ret;
  // Get this before the file can be touched by another thread via new dentry
  GetFileAttr(new_inode_path, fa_ret);
  context->inodes_read.push_back(inode);
  if (durable_range_)
    durable_range_->Add(inode, create_req->new_inode);
  ret = CreateDentry(context, inode, name, DT_REG, create_req->new_inode);
  if (ret == -EEXIST && !(create_req->flags & O_EXCL)) {
    ret = Unlink(context, inode, name, removed_info_ret);
    if (ret != 0)
      return ret;
    ret = CreateDentry(context, inode, name, DT_REG, create_req->new_inode);
  }
  if (ret != 0)
    return ret;
  context->inodes_changed.push_back(inode);
  context->inodes_changed.push_back(create_req->new_inode);
  UpdateFileTimes(inode, context, FUSE_SET_ATTR_MTIME);
  inode_src_->NotifyUsed(create_req->new_inode);
  return 0;
}

int Store::Mkdir(OpContext* context, InodeNum parent, const char* name,
                 const CreateDirReq* create_req, FileAttr* fa_ret) {
  std::string dentry_path = GetInodePath(parent) + "/" + name;
  const char* dentry_path_s = dentry_path.c_str();
  std::string inode_path = GetInodePath(create_req->new_inode);
  const char* inode_path_s = inode_path.c_str();
  // Create directory with server permission, since parent of
  // inode_path is owned by server.
  if (mkdir(inode_path_s, create_req->mode) == -1)
    return -errno;
  // If user.ndsg exists from parent, inherit from it
  char dsg_buf[4] = {'\0'};
  int dsg_buf_len =
      lgetxattr(GetInodePath(parent).c_str(), "user.ndsg", dsg_buf, 4);
  if (dsg_buf_len > 0)
    lsetxattr(inode_path_s, "user.ndsg", dsg_buf, dsg_buf_len, 0);
  int ret = -1;
  BOOST_SCOPE_EXIT(&ret, &inode_path, &inode_path_s) {
    if (ret < 0) {
      rmdir(inode_path_s);
      unlink((inode_path + "x").c_str());
    }
  } BOOST_SCOPE_EXIT_END;
  PostCreateInode(create_req->new_inode, context, create_req->mode);
  GetFileAttr(inode_path, fa_ret);
  {
    context->inodes_read.push_back(parent);
    UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
    context->i_mutex_guard.reset(new InodeMutexGuard(
        &inode_mutex_array_, parent));
    if (durable_range_)
      durable_range_->Add(parent, create_req->new_inode);
    if (mkdir(dentry_path_s, 0777) == -1)
      return ret = -errno;
    context->inodes_changed.push_back(parent);
    context->inodes_changed.push_back(create_req->new_inode);
  }
  inode_src_->NotifyUsed(create_req->new_inode);
  UpdateFileTimes(parent, context, FUSE_SET_ATTR_MTIME);
  std::string pinode_s = GetInodeStr(parent);
  SetDirParent(inode_path, pinode_s);
  std::string inode_s = GetInodeStr(create_req->new_inode);
  return ret = lsetxattr(dentry_path_s, "user.ino",
                         inode_s.c_str(), inode_s.size(), 0) == 0 ?
      0 : -EIO;
}

int Store::Symlink(OpContext* context, InodeNum parent,
                   const char* name, InodeNum new_inode, const char* target,
                   FileAttr* fa_ret) {
  std::string inode_path = GetInodePath(new_inode);
  if (symlink(target, inode_path.c_str()) == -1)
    return -errno;
  PostCreateInode(new_inode, context, uint64_t(-1));
  GetFileAttr(inode_path, fa_ret);
  int ret = -EIO;
  BOOST_SCOPE_EXIT(&ret, &inode_path) {
    if (ret != 0) {
      unlink(inode_path.c_str());
      unlink((inode_path + "x").c_str());
    }
  } BOOST_SCOPE_EXIT_END;
  context->inodes_read.push_back(parent);
  if (durable_range_)
    durable_range_->Add(parent, new_inode);
  if ((ret = CreateDentry(context, parent, name, DT_LNK, new_inode)) != 0)
    return ret;
  context->inodes_changed.push_back(parent);
  context->inodes_changed.push_back(new_inode);
  UpdateFileTimes(parent, context, FUSE_SET_ATTR_MTIME);
  inode_src_->NotifyUsed(new_inode);
  return ret;
}

int Store::ResyncInode(InodeNum inode, const FileAttr& fa,
                       const char* buf, std::size_t extra_size,
                       std::size_t buf_size) {
  // Inode is created with server permission
  std::string inode_path = GetInodePath(inode);
  // Create control file, unlink if exists
  unlink((inode_path + "x").c_str());
  CreateEmptyFile(inode_path + "x");
  // Remove inode if exists
  if (S_ISDIR(fa.mode))
    RemoveDirInode(inode_path);
  else
    unlink(inode_path.c_str());

  if (S_ISREG(fa.mode)) {  // Regular file
    int fd = open(inode_path.c_str(), O_CREAT | O_EXCL | O_WRONLY, fa.mode);
    if (fd == -1)
      return -errno;
    if (ftruncate(fd, fa.size) == -1)
      return -errno;  // Can't cover
    close(fd);
    // Set Server group
    const GroupId* groups = reinterpret_cast<const GroupId*>(buf);
    size_t num_groups = extra_size / sizeof(GroupId);
    SetFileGroupIds(inode_path,
                    std::vector<GroupId>(groups, groups + num_groups));
  } else if (S_ISDIR(fa.mode)) {  // Directory
    if (mkdir(inode_path.c_str(), fa.mode) == -1)
      return -errno;
    const InodeNum& parent = reinterpret_cast<const InodeNum&>(*buf);
    SetDirParent(inode_path, GetInodeStr(parent));
  } else if (S_ISLNK(fa.mode)) {  // Symlink
    if (symlink(buf, inode_path.c_str()) == -1)
      return -errno;
  } else {  // Special file
    if (mknod(inode_path.c_str(), fa.mode, fa.rdev) == -1)
      return -errno;
  }
  // Restore extended attributes
  std::string xattrs_buf(buf + extra_size, buf + buf_size);
  BOOST_FOREACH(const XAttrList::value_type& elt,
                BufferToXattrList(xattrs_buf)) {
    LSetUserXattr(inode_path.c_str(), elt.first, elt.second);
  }
  // Ensure correct ownership
  lchown(inode_path.c_str(), fa.uid, fa.gid);
  // Ensure correct permission
  chmod(inode_path.c_str(), fa.mode & 07777);
  // File times
  if (S_ISDIR(fa.mode)) {
    SetLastResyncDirTimes();
    // Directory times to be set on next ResyncInode() for dir
    resync_dt_pending_ = true;
    resync_dt_inode_path_ = inode_path;
    resync_dt_atim_ = fa.atime;
    resync_dt_mtim_ = fa.mtime;
    resync_dt_ctim_ = fa.ctime;
  } else {
    // Create hard link(s)
    for (uint32_t i = 1; i < fa.nlink; ++i) {
      std::string link_name = (boost::format("%s.%d") % inode_path % i).str();
      if (link(inode_path.c_str(), link_name.c_str()) == -1)
        return -errno;  // Can't cover in unit test, but better to have anyway
    }
    SetFileTimes(inode_path, FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME,
                 fa.atime, fa.mtime, fa.ctime);
  }
  return 0;
}

int Store::ResyncDentry(uint32_t uid, uint32_t gid,
                        InodeNum parent, InodeNum target, unsigned char ftype,
                        const char* dentry_name) {
  UNIQUE_PTR<IUgidSetterGuard> guard(ugid_handler_->SetFsIds(uid, gid));
  std::string parent_path = GetInodePath(parent);
  std::string dentry_path = parent_path + "/" + std::string(dentry_name);
  if (ftype == 'D' || ftype == DT_DIR) {
    std::string inode_attr = GetInodeStr(target);
    rmdir(dentry_path.c_str());
    if (mkdir(dentry_path.c_str(), 0777) == -1)
      return -errno;
    return lsetxattr(dentry_path.c_str(), "user.ino",
                     inode_attr.c_str(), inode_attr.length(), 0) == 0 ?
        0 : -EIO;
  } else {
    std::string target_s = GetInodeFTStr(target, ftype);
    unlink(dentry_path.c_str());
    if (symlink(target_s.c_str(), dentry_path.c_str()) != 0)
      return -errno;
  }
  return 0;
}

int Store::ResyncRemoval(InodeNum inode) {
  std::string inode_path = GetInodePath(inode);
  if (unlink((inode_path + "x").c_str()) == -1)
    return -errno;
  struct stat stbuf;
  if (lstat(inode_path.c_str(), &stbuf) == -1)
    return -errno;  // Can't cover in unit test, but better to have anyway
  if (S_ISDIR(stbuf.st_mode))
    return RemoveDirInode(inode_path.c_str());
  else
    return unlink(inode_path.c_str());
}

int Store::Mknod(OpContext* context, InodeNum parent, const char* name,
                 InodeNum new_inode, uint64_t mode, uint64_t rdev,
                 std::vector<GroupId>* group_ids,
                 FileAttr* fa_ret) {
  std::string inode_path = GetInodePath(new_inode);
  if (context->req_context->uid != 0 && !S_ISREG(mode) && !S_ISFIFO(mode)
      & !S_ISSOCK(mode))
    return -EPERM;
  if (mknod(inode_path.c_str(), mode, rdev) == -1)
    return -errno;
  int ret = -EIO;
  PostCreateInode(new_inode, context, mode & ALLPERMS);
  BOOST_SCOPE_EXIT(&ret, &inode_path) {
    if (ret != 0) {
      unlink(inode_path.c_str());
      unlink((inode_path + "x").c_str());
    }
  } BOOST_SCOPE_EXIT_END;
  GetFileAttr(inode_path, fa_ret);
  if (S_ISREG(mode)) {
    ret = AllocateGroup(inode_path, GetInodePath(parent), group_ids);
    if (ret != 0)
      return ret;
  }
  context->inodes_read.push_back(parent);
  if (durable_range_)
    durable_range_->Add(parent, new_inode);
  if ((ret = CreateDentry(context, parent, name, Mod2FT(mode), new_inode)) != 0)
    return ret;
  context->inodes_changed.push_back(parent);
  context->inodes_changed.push_back(new_inode);
  UpdateFileTimes(parent, context, FUSE_SET_ATTR_MTIME);
  inode_src_->NotifyUsed(new_inode);
  return ret;
}

int Store::Link(OpContext* context, InodeNum inode, InodeNum dir_inode,
                const char* name, FileAttr* fa_ret) {
  std::string inode_path = GetInodePath(inode);
  context->inodes_read.push_back(inode);
  context->inodes_read.push_back(dir_inode);
  struct stat stbuf;
  if (lstat(inode_path.c_str(), &stbuf) < 0)
    return -errno;
  int ret = CreateDentry(context, dir_inode, name, Mod2FT(stbuf.st_mode),
                         inode);
  if (ret != 0)
    return ret;
  if (durable_range_)
    durable_range_->Add(dir_inode, inode);
  link(inode_path.c_str(),
       (boost::format("%s.%d") % inode_path % stbuf.st_nlink).str().c_str());
  context->inodes_changed.push_back(inode);
  context->inodes_changed.push_back(dir_inode);
  UpdateFileTimes(inode, context, 0);
  UpdateFileTimes(dir_inode, context, FUSE_SET_ATTR_MTIME);
  return GetFileAttr(inode_path, fa_ret);
}

// Note: in designated thread of newparent
int Store::Rename(OpContext* context, InodeNum parent, const char *name,
                  InodeNum newparent, const char *newname,
                  InodeNum* moved_ret,
                  boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret) {
  std::string parent_path = GetInodePath(parent);
  std::string dentry_path = parent_path + "/" + name;
  std::string new_parent_path = GetInodePath(newparent);
  std::string new_dentry_path = new_parent_path + "/" + newname;
  InodeNum to_remove;
  struct stat new_dentry_stat;
  if (DentryToInode(new_dentry_path, &to_remove, &new_dentry_stat) != 0)
    to_remove = parent;  // Indicate nothing to remove
  context->inodes_read.push_back(parent);
  context->inodes_read.push_back(newparent);
  context->i_mutex_guard.reset(new InodeMutexGuard(
      &inode_mutex_array_, parent, newparent, to_remove));
  // Retry to ensure that to_remove is not removed during locking
  if (DentryToInode(new_dentry_path, &to_remove, &new_dentry_stat) != 0)
    to_remove = parent;  // Indicate nothing to remove
  // Now that parent is locked, can safely read the content of parent
  struct stat dentry_stat;
  if (int d2i_ret = DentryToInode(dentry_path, moved_ret, &dentry_stat))
    return d2i_ret;
  PreRemoveDentry(parent_path, dentry_path, *moved_ret);
  if (to_remove != parent)
    PreRemoveDentry(new_parent_path, new_dentry_path, to_remove);
  if (durable_range_)
    durable_range_->Add(parent, newparent, to_remove);
  if (S_ISDIR(dentry_stat.st_mode) && to_remove != parent) {
    if (!S_ISDIR(new_dentry_stat.st_mode))
      return -ENOTDIR;
    else if (!IsInodeDirEmpty(to_remove))  // Check directory emptiness early
      return -ENOTEMPTY;
  }
  {
    UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
    // Okay to rename
    if (std::rename(dentry_path.c_str(), new_dentry_path.c_str()) == -1)
      return -errno;
  }
  context->inodes_changed.push_back(parent);
  context->inodes_changed.push_back(newparent);
  if (to_remove != parent) {
    if (S_ISDIR(new_dentry_stat.st_mode)) {
      rmdir(GetInodePath(to_remove).c_str());
      unlink((GetInodePath(to_remove) + "x").c_str());
      inode_src_->NotifyRemoved(to_remove);
      inode_removal_tracker_->RecordRemoved(to_remove);
      context->inodes_changed.push_back(to_remove);
    } else {
      PostUnlinkDentry(context, to_remove, removed_info_ret);
    }
  }
  if (S_ISDIR(dentry_stat.st_mode))
    SetDirParent(GetInodePath(*moved_ret), GetInodeStr(newparent));
  UpdateFileTimes(*moved_ret, context, 0);
  UpdateFileTimes(parent, context, FUSE_SET_ATTR_MTIME);
  UpdateFileTimes(newparent, context, FUSE_SET_ATTR_MTIME);
  return 0;
}

int Store::Unlink(OpContext* context, InodeNum parent, const char* name,
                  boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret) {
  std::string parent_path = GetInodePath(parent);
  std::string dentry_path = parent_path + "/" + name;
  InodeNum file_inode;
  context->inodes_read.push_back(parent);
  unsigned char ftype;
  int s2i_ret = SymlinkToInodeFT(dentry_path, &file_inode, &ftype);
  if (s2i_ret != 0)
    return s2i_ret == -EIO ? -EISDIR : s2i_ret;
  // The fims.hpp guarantee mean that file_inode will not point to a
  // different inode
  if (!context->i_mutex_guard)  // Only if not called from Store methods
    context->i_mutex_guard.reset(new InodeMutexGuard(
        &inode_mutex_array_, parent, file_inode));
  // Check again, in case it is removed before we can get the lock.
  s2i_ret = SymlinkToInodeFT(dentry_path, &file_inode, &ftype);
  if (s2i_ret != 0)
    return s2i_ret == -EIO ? -EISDIR : s2i_ret;  // Test race, can't cover
  PreRemoveDentry(parent_path, dentry_path, file_inode);
  if (durable_range_)
    durable_range_->Add(parent, file_inode);
  {
    UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
    if (unlink(dentry_path.c_str()) != 0)
      return -errno;
  }
  context->inodes_changed.push_back(parent);
  UpdateFileTimes(parent, context, FUSE_SET_ATTR_MTIME);
  PostUnlinkDentry(context, file_inode, removed_info_ret);
  return 0;
}

void Store::PostUnlinkDentry(
    OpContext* context, InodeNum file_inode,
    boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret) {
  context->inodes_changed.push_back(file_inode);
  std::string file_inode_path = GetInodePath(file_inode);
  struct stat stbuf;
  removed_info_ret->reset(new RemovedInodeInfo);
  (*removed_info_ret)->inode = file_inode;
  (*removed_info_ret)->to_free = false;
  if (lstat(file_inode_path.c_str(), &stbuf) == 0
      && stbuf.st_nlink > 1) {
    unlink((boost::format("%s.%d") % file_inode_path % (stbuf.st_nlink - 1))
        .str().c_str());
    UpdateFileTimes(file_inode, context, 0);  // Update ctime for nlink change
    return;
  }

  if (inode_usage_->IsOpened(file_inode)) {
    inode_usage_->AddPendingUnlink(file_inode);
  } else {
    (*removed_info_ret)->to_free = true;
    (*removed_info_ret)->groups.reset(new std::vector<GroupId>);
    GetFileGroupIds(file_inode, (*removed_info_ret)->groups.get());
    FreeInode(file_inode);
  }
}

int Store::Rmdir(OpContext* context, InodeNum parent, const char* name) {
  std::string parent_path = GetInodePath(parent);
  std::string dentry_path = parent_path + "/" + name;
  context->inodes_read.push_back(parent);
  InodeNum to_remove;
  if (int x2i_ret = XattrToInode(dentry_path, &to_remove))
    return x2i_ret == -EIO ? -ENOTDIR : x2i_ret;
  // The dentry can now never point to a different one.
  context->i_mutex_guard.reset(new InodeMutexGuard(
      &inode_mutex_array_, parent, to_remove));
  // Ensure the directory is not removed by a Rename during locking
  if (int x2i_ret = XattrToInode(dentry_path, &to_remove))
    return x2i_ret == -EIO ? -ENOTDIR : x2i_ret;  // Test race, can't cover
  PreRemoveDentry(parent_path, dentry_path, to_remove);
  if (durable_range_)
    durable_range_->Add(parent, to_remove);
  {
    UNIQUE_PTR<IUgidSetterGuard> guard(SetFsIds(context));
    // Check emptiness before we do any damage
    if (!IsInodeDirEmpty(to_remove))
      return -ENOTEMPTY;
    if (rmdir(dentry_path.c_str()) == -1)
      return -errno;
  }
  context->inodes_changed.push_back(parent);
  context->inodes_changed.push_back(to_remove);
  inode_src_->NotifyRemoved(to_remove);
  inode_removal_tracker_->RecordRemoved(to_remove);
  rmdir(GetInodePath(to_remove).c_str());
  unlink((GetInodePath(to_remove) + "x").c_str());
  UpdateFileTimes(parent, context, FUSE_SET_ATTR_MTIME);
  return 0;
}

int Store::Stat(uint64_t* total_inodes, uint64_t* free_inodes) {
  struct statvfs stvfsbuf;
  int ret = statvfs(data_path_.c_str(), &stvfsbuf);
  if (ret != 0)
    return ret;
  *total_inodes = double(stvfsbuf.f_files) / 3;
  *free_inodes = double(stvfsbuf.f_favail) / 3;
  return 0;
}

}  // namespace

IStore* MakeStore(std::string data_path) {
  return new Store(data_path);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
