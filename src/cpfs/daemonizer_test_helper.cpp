/* Copyright 2014 ClusterTech Ltd */
#include "daemonizer.hpp"

#include <cstdio>

#include <boost/scoped_ptr.hpp>

#include "daemonizer_impl.hpp"

int main(int argc, char* argv[]) {
  if (argc < 2)
    return 1;
  const char* file_path = argv[1];
  boost::scoped_ptr<cpfs::IDaemonizer> daemonizer(cpfs::MakeDaemonizer());
  daemonizer->Daemonize();
  // Create an empty file to indicate that daemonizer is started successfully
  FILE* file = fopen(file_path, "w+");
  fclose(file);
  return 0;
}
