/* Copyright 2013 ClusterTech Ltd */
#include "mock_actions.hpp"

#include <unistd.h>

#include <sys/resource.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

using ::testing::_;
using ::testing::ElementsAre;

namespace cpfs {
namespace {

TEST(MockActionsTest, MockDataPathMgr) {
  MockDataPathMgr mgr("/tmp/mock_actions_test_XXXXXX");
  EXPECT_EQ(0, memcmp("/tmp/mock_actions_test_", mgr.GetPath(),
                      strlen("/tmp/mock_actions_test_")));
}

TEST(MockActionsTest, MakeArgs) {
  const char* args[] = {"a", "b", NULL};
  std::vector<std::string> sv = MakeArgVector(args);
  EXPECT_THAT(sv, ElementsAre("a", "b"));
  std::vector<char*> cpv = MakeArgs(&sv);
  ASSERT_EQ(2U, cpv.size());
  EXPECT_STREQ("a", cpv[0]);
  EXPECT_STREQ("b", cpv[1]);
}

class Callback1 {
 public:
  MOCK_METHOD1(func, void(int* arr));
};

TEST(MockActionsTest, SaveArgArray) {
  Callback1 cb;
  int val[2];
  EXPECT_CALL(cb, func(_))
      .WillOnce(SaveArgArray<0>(val, 2));

  int args[] = {42, 100, 80};
  cb.func(args);
  EXPECT_EQ(42, val[0]);
  EXPECT_EQ(100, val[1]);
}

class Callback2 {
 public:
  MOCK_METHOD1(func, void(boost::shared_ptr<int>* ptr_ret));
};

TEST(MockActionsTest, ResetSmartPointerArg) {
  boost::shared_ptr<int> ptr;
  Callback2 cb;
  EXPECT_CALL(cb, func(_))
      .WillOnce(ResetSmartPointerArg<0>(new int(5)));

  cb.func(&ptr);
  EXPECT_EQ(5, *ptr);
}

TEST(MockActionsTest, Sleep) {
  Sleep(0.01)();
}

TEST(MockActionsTest, ForkCaptureNormal) {
  char buf[1024];
  int status;
  if (ForkCaptureResult res = ForkCapture(buf, 1024, &status)) {
    std::cout << "Hello\n";
    std::exit(0);
  }
  EXPECT_EQ("Hello\n", std::string(buf));
  EXPECT_EQ(0, status);
}

TEST(MockActionsTest, ForkCaptureMisuse) {
  char buf[1024];
  int status;
  if (ForkCaptureResult res = ForkCapture(buf, 1024, &status))
    dup2(1, 3);
  EXPECT_EQ("ForkCapture child not exited properly!\n", std::string(buf));
  EXPECT_EQ(1, WEXITSTATUS(status));
}

void DoForkCaptureSegfault() {
  char buf[1024];
  int status;
  if (ForkCaptureResult res = ForkCapture(buf, 1024, &status)) {
    struct rlimit limit = { 0, 0 };
    setrlimit(RLIMIT_CORE, &limit);
    execl("tbuild/cpfs/mock_actions_test_segv", "segv", NULL);
    std::exit(1);
  }
}

TEST(MockActionsTest, ForkCaptureSegfault) {
  EXPECT_NONFATAL_FAILURE(DoForkCaptureSegfault(),
                          "ForkCapture child received SIGSEGV/SIGBUS");
}

}  // namespace
}  // namespace cpfs
