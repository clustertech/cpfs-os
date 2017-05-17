/* Copyright 2014 ClusterTech Ltd */
#include <unistd.h>

#include <cstdlib>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mock_actions.hpp"

using ::testing::Lt;

namespace cpfs {
namespace {

TEST(Daemonizer, Daemonize) {
  MockDataPathMgr data_path("/tmp/server_main-XXXXXX");
  std::string test_file = data_path.GetPath() + std::string("/test_file");
  char buf[1024];
  int status;
  if (ForkCaptureResult res = ForkCapture(buf, 1024, &status)) {
    execl("tbuild/cpfs/daemonizer_test_helper",
          "daemon_test_helper", test_file.c_str(), NULL);
    std::exit(1);
  }
  int retry = 0;
  while (true) {
    ASSERT_THAT(++retry, Lt(10));
    Sleep(0.2)();
    if (access(test_file.c_str(), F_OK) != -1)
      break;
  }
}

}  // namespace
}  // namespace cpfs
