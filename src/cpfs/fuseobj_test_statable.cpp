/* Copyright 2013 ClusterTech Ltd */
#include <fuse/fuse_lowlevel.h>

#include <sys/stat.h>

#include <cstring>

#include "fuseobj.hpp"

namespace {

/**
 * A FUSE object that generates a fake inode on getattr calls.
 */
class RootStatableFuseObj : public cpfs::BaseFuseObj<> {
 public:
  /**
   * @param req The FUSE request
   *
   * @param ino The inode number
   *
   * @param fi The file info
   */
  void Getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    (void) fi;
    struct stat stbuf;
    std::memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    stbuf.st_mode = S_IFDIR | 0500;  // FUSE set G and O parts to 0
    stbuf.st_nlink = 2;
    ReplyAttr(req, &stbuf, 1.0);
  }
};

}  // namespace

int main(int argc, char* argv[]) {
  RootStatableFuseObj root_statable_fuse_obj;
  cpfs::BaseFuseRunner<RootStatableFuseObj> runner(&root_statable_fuse_obj);
  return runner.Main(argc, argv);
}
