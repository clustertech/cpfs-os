/* Copyright 2013 ClusterTech Ltd */
#include <unistd.h>

#include <boost/thread/thread.hpp>

#include "fuseobj.hpp"

cpfs::BaseFuseObj<> empty_fuse_obj;
cpfs::BaseFuseRunner<cpfs::BaseFuseObj<> > runner(&empty_fuse_obj);

void Run() {
  sleep(1);
  runner.Shutdown();
}

int main(int argc, char* argv[]) {
  runner.Init();
  runner.SetArgs(argc, argv);
  boost::thread th(Run);
  return runner.Run();
}
