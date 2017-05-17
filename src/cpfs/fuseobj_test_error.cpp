/* Copyright 2013 ClusterTech Ltd */
#include <fuse/fuse_lowlevel.h>

#include <stdexcept>

#include "fuseobj.hpp"

namespace {

/**
 * A FUSE object that generates errors on getattr calls.
 */
class ErrorFuseObj : public cpfs::BaseFuseObj<> {
 public:
  /**
   * @param req The FUSE request
   *
   * @param ino The inode number
   *
   * @param fi The file info
   */
  void Getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    (void) req;
    (void) ino;
    (void) fi;
    throw std::runtime_error("Error");
  }
};

}  // namespace

int main(int argc, char* argv[]) {
  ErrorFuseObj root_statable_fuse_obj;
  cpfs::BaseFuseRunner<ErrorFuseObj> runner(&root_statable_fuse_obj);
  return runner.Main(argc, argv);
}
