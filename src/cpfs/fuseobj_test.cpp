/* Copyright 2013 ClusterTech Ltd */
#include "fuseobj.hpp"

#include <signal.h>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <cstdlib>
#include <cstring>

#include <gtest/gtest.h>

#include "mock_actions.hpp"

namespace cpfs {

// Explicit instantiate class template to better detect missing coverage
template class BaseFuseObj<>;
template class BaseFuseRunner<BaseFuseObj<> >;

namespace {

const char* kMntDir = "/tmp/fuseobj_test_XXXXXX";

class FuseobjTest : public ::testing::Test {
 protected:
  char* mnt_dir_;
  static bool first;

  virtual void SetUp() {
    mnt_dir_ = strdup(kMntDir);
    mkdtemp(mnt_dir_);
  }

  virtual void TearDown() {
    rmdir(mnt_dir_);
    std::free(mnt_dir_);
    first = false;
  }

  void WaitFS() {
    // Wait longer for the first test for library loading
    Sleep(first ? 1.2 : 0.3)();
  }
};

bool FuseobjTest::first = true;

TEST_F(FuseobjTest, EmptyFS) {
  pid_t child;
  if ((child = fork()) > 0) {
    int status;
    waitpid(child, &status, 0);
    EXPECT_EQ(0, status);
  } else {
    execl("tbuild/cpfs/fuseobj_test_empty", "empty_fs",
          "-s", "-f", mnt_dir_, NULL);
    std::exit(1);
  }
}

TEST_F(FuseobjTest, EmptyFSMT) {
  pid_t child;
  if ((child = fork()) > 0) {
    int status;
    waitpid(child, &status, 0);
    EXPECT_EQ(0, status);
  } else {
    execl("tbuild/cpfs/fuseobj_test_empty", "empty_fs",
          "-f", mnt_dir_, NULL);
    std::exit(1);
  }
}

TEST_F(FuseobjTest, EmptyFSBadArg) {
  char buf[1024];
  int status;
  if (ForkCaptureResult res = ForkCapture(buf, 1024, &status)) {
    execl("tbuild/cpfs/fuseobj_test_empty", "empty_fs", "-s", "-f", NULL);
    std::exit(1);
  }
  EXPECT_NE(reinterpret_cast<char*>(0),
            std::strstr(buf, "missing mountpoint"));
  EXPECT_EQ(1, WEXITSTATUS(status));
}

TEST_F(FuseobjTest, EmptyFSVersion) {
  char buf[1024];
  int status;
  if (ForkCaptureResult res = ForkCapture(buf, 1024, &status)) {
    execl("tbuild/cpfs/fuseobj_test_empty", "empty_fs", "-V", NULL);
    std::exit(1);
  }
  EXPECT_NE(reinterpret_cast<char*>(0), std::strstr(buf, "version"));
  EXPECT_EQ(0, status);
}

TEST_F(FuseobjTest, FailMountFS) {
  char buf[1024];
  int status;
  if (ForkCaptureResult res = ForkCapture(buf, 1024, &status)) {
    char* nonexist = strdup(mnt_dir_);
    nonexist[std::strlen(nonexist) - 1] = 0;
    struct rlimit limit = { 0, 0 };
    setrlimit(RLIMIT_CORE, &limit);
    execl("tbuild/cpfs/fuseobj_test_empty", "empty_fs",
          "-s", "-f", nonexist, NULL);
    std::exit(1);
  }
  EXPECT_NE(reinterpret_cast<char*>(0),
            std::strstr(buf, "bad mount point"));
  EXPECT_NE(0, WTERMSIG(status));
}

TEST_F(FuseobjTest, RootStatableFS) {
  pid_t child;
  if ((child = fork()) > 0) {
    WaitFS();
    struct stat buf;
    EXPECT_EQ(0, stat(mnt_dir_, &buf));
    EXPECT_EQ(1U, buf.st_ino);
    EXPECT_EQ(mode_t(S_IFDIR | 0500), buf.st_mode);
    kill(child, SIGINT);
    int status;
    waitpid(child, &status, 0);
    EXPECT_EQ(0, status);
  } else {
    execl("tbuild/cpfs/fuseobj_test_statable", "statable_fs",
          "-s", "-f", mnt_dir_, NULL);
    std::exit(1);
  }
}

TEST_F(FuseobjTest, ErrorFS) {
  pid_t child;
  if ((child = fork()) > 0) {
    WaitFS();
    struct stat buf;
    EXPECT_EQ(-1, stat(mnt_dir_, &buf));
    EXPECT_EQ(EIO, errno);
    kill(child, SIGINT);
    int status;
    waitpid(child, &status, 0);
    EXPECT_EQ(0, status);
  } else {
    execl("tbuild/cpfs/fuseobj_test_error", "error_fs",
          "-s", "-f", mnt_dir_, NULL);
    std::exit(1);
  }
}

}  // namespace
}  //  namespace cpfs
