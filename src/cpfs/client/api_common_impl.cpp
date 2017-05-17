/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of APICommon.
 */

#include "client/api_common.hpp"

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>

#include <algorithm>
#include <cstring>

#include <boost/bind.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "concurrent_queue.hpp"
#include "cpfs_api.h"  // NOLINT(build/include)
#include "client/fs_common_lowlevel.hpp"

namespace cpfs {
namespace client {

namespace {

const unsigned kMaxPathLength = 256;  /**< Maximum length of path */
const char kDirSep = '/';  /**< Directory separator */

/**
 * Implementation for the file system common layer
 */
class APICommon : public IAPICommon {
 public:
  /**
   * @param fs The lowlevel filesystem layer
   */
  explicit APICommon(IFSCommonLL* fs)
      : fs_(fs), identity_(FSIdentity(getuid(), getgid())) {}

  int Open(const char* path, int flags, mode_t mode, CpfsFileHandle* handle) {
    if (handle->opened)
      return 0;
    // TODO(Joseph): Handle O_TMPFILE
    if (flags & O_CREAT) {
      return CreateDentry(path, mode, flags, handle);
    } else {
      struct stat stbuf;  // TODO(Joseph): Cache in the handle?
      if (Lookup(path, &handle->inode, &stbuf) == -1)
        return -1;
    }
    FSOpenReply ret;
    if (fs_->Open(identity_, handle->inode, flags, &ret)) {
      handle->flags = flags;
      handle->fh = ret.fh;
      handle->opened = 1;
      return 0;
    } else {
      return -1;
    }
  }

  int Close(CpfsFileHandle* handle) {
    if (handle->opened) {
      fs_->Release(handle->inode, &handle->fh, handle->flags);
      handle->opened = 0;
    }
    return 0;
  }

  int Read(char* buf, uint32_t size, off_t off, CpfsFileHandle* handle) {
    if (!handle->opened)
      return -1;
    FSReadReply ret;
    ret.buf = buf;
    return fs_->Read(handle->fh, handle->inode, size, off, &ret) ? 0 : -1;
  }

  int Write(const char* buf, uint32_t size, off_t off,
            const CpfsFileHandle* handle) {
    if (!handle->opened)
      return -1;
    FSWriteReply ret;
    return fs_->Write(handle->fh, handle->inode, buf, size, off, &ret)
        ? 0 : -1;
  }

  int Getattr(struct stat* stbuf, CpfsFileHandle* handle) {
    if (!handle->opened)
      return -1;
    FSGetattrReply ret;
    ret.stbuf = stbuf;
    return fs_->Getattr(identity_, handle->inode, &ret) ? 0 : -1;
  }

  int Setattr(struct stat* stbuf, int to_set, CpfsFileHandle* handle) {
    if (!handle->opened)
      return -1;
    FSSetattrReply ret;
    return fs_->Setattr(identity_,
                        handle->inode, stbuf, to_set, &ret) ? 0 : -1;
  }

 private:
  IFSCommonLL* fs_;  /*< The lowlevel filesystem layer */
  FSIdentity identity_;  /*< The uid and gid */

  /**
   * Wrapper for safer strcpy on path
   */
  static void CopyPath(char* dest, const char* src,
                       unsigned num_to_copy = kMaxPathLength - 1) {
    strncpy(dest, src, std::min(num_to_copy, kMaxPathLength - 1));
  }

  /**
   * Used internally by Open() and Create()
   *
   * @param path The path to lookup
   *
   * @param ret_inode The inode returned
   *
   * @param ret_stbuf The stat for the returned inode
   *
   * @return 0 on success, -ve otherwise
   */
  int Lookup(const char* path, uint64_t* ret_inode, struct stat* ret_stbuf) {
    char temp_path[kMaxPathLength] = {'\0'};
    CopyPath(temp_path, path);
    return _Lookup(1, temp_path, ret_inode, ret_stbuf) ? 0 : -1;
  }

  /**
   * Used internally by Lookup()
   */
  bool _Lookup(uint64_t parent, char* path,
               uint64_t* ret_inode, struct stat* ret_stbuf) {
    if (!path || *path == '\0')
      return true;
    char* dir_sep = strchr(path, kDirSep);
    char* subdir_path = NULL;
    if (dir_sep) {
      *dir_sep = '\0';
      subdir_path = dir_sep + 1;
    }
    FSLookupReply ret;
    if (fs_->Lookup(identity_, parent, path, &ret)) {
      *ret_inode = ret.inode;
      *ret_stbuf = ret.attr;
      if (!S_ISDIR(ret.attr.st_mode))
        return true;
      return _Lookup(ret.inode, subdir_path, ret_inode, ret_stbuf);
    } else {
      return false;
    }
  }

  /**
   * Used internally by Open()
   *
   * @return 0 on success, -ve otherwise
   */
  int CreateDentry(const char* path, mode_t mode, int flags,
                   CpfsFileHandle* handle) {
    const char* last_dir_sep = strrchr(path, kDirSep);
    const char* name = path;
    uint64_t parent = 1;
    if (last_dir_sep) {  // Lookup the parent inode
      char dir_path[kMaxPathLength] = {'\0'};
      CopyPath(dir_path, path, last_dir_sep - path);
      struct stat parent_attr;
      if (Lookup(dir_path, &parent, &parent_attr) < 0 ||
          !S_ISDIR(parent_attr.st_mode))
        return -1;
      name = last_dir_sep + 1;
    }
    // Create a new dentry on the parent directory
    FSCreateReply ret;
    if (fs_->Create(identity_, parent, name, mode, flags, &ret)) {
      handle->flags = flags;
      handle->inode = ret.inode;
      handle->fh = ret.fh;
      handle->opened = 1;
      return 0;
    } else {
      return -1;
    }
  }
};

/**
 * Implementation for the FSAsyncRWExectutor
 */
class FSAsyncRWExecutor : public IFSAsyncRWExecutor {
 public:
  /**
   * @param api_common API common object to perform requests
   */
  explicit FSAsyncRWExecutor(IAPICommon* api_common)
      : api_common_(api_common) {}

  ~FSAsyncRWExecutor() {
    Stop();
    IAPIRequest* req;
    while ((req = requests_.try_dequeue()))
      delete req;
  }

  void Start(unsigned num_workers) {
    for (unsigned i = 0; i < num_workers; ++i) {
      IOWorker* worker;
      workers_.push_back(worker = new IOWorker(this));
      worker->Start();
    }
  }

  void Stop() {
    for (unsigned i = 0; i < workers_.size(); ++i)
      requests_.enqueue(NULL);  // Notify exit
    for (unsigned i = 0; i < workers_.size(); ++i)
      workers_[i].Stop();
  }

  void Add(IAPIRequest* req) {
    requests_.enqueue(req);
  }

 private:
  /**
   * The worker thread for I/O
   */
  class IOWorker {
   public:
    /**
     * @param fs The filesystem common
     *
     * @param executor The FSAsyncRWExecutor
     */
    explicit IOWorker(FSAsyncRWExecutor* executor) : executor_(executor) {}

    ~IOWorker() {
      Stop();
    }

    void Start() {
      if (!thread_.get())
        thread_.reset(new boost::thread(boost::bind(&IOWorker::Run, this)));
    }

    void Stop() {
      if (thread_.get()) {
        thread_->join();
        thread_.reset();
      }
    }

    void Run() {
      IAPIRequest* req_r;
      while ((req_r = executor_->GetRequest())) {
        boost::scoped_ptr<IAPIRequest> req(req_r);
        req->Execute(executor_->api_common());
      };
    }

   private:
    boost::scoped_ptr<boost::thread> thread_; /**< The running thread. */
    FSAsyncRWExecutor* executor_;
  };

  /**
   * Dequeue one request. Blocked if there is no request.
   */
  IAPIRequest* GetRequest() {
    return requests_.dequeue();
  }

  /**
   * Get the API layer
   */
  IAPICommon* api_common() {
    return api_common_;
  }

  ConcurrentQueue<IAPIRequest> requests_;  /**< All I/O requests */
  boost::ptr_vector<IOWorker> workers_;
  IAPICommon* api_common_;
};

}  // namespace

IAPICommon* MakeAPICommon(IFSCommonLL* fs) {
  return new APICommon(fs);
}

IFSAsyncRWExecutor* MakeAsyncRWExecutor(IAPICommon* api_common) {
  return new FSAsyncRWExecutor(api_common);
}

}  // namespace client
}  // namespace cpfs
