/* Copyright 2013 ClusterTech Ltd */
#include "posix_fs_impl.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <string>

#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "mock_actions.hpp"
#include "posix_fs.hpp"

namespace cpfs {
namespace {

const char* kDataPath = "/tmp/posix_fs_test_XXXXXX";

class PosixFSTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  boost::scoped_ptr<IPosixFS> posix_fs_;

  PosixFSTest() : data_path_mgr_(kDataPath), posix_fs_(MakePosixFS()) {}

  std::string GetPath(std::string filename) {
    std::string path = data_path_mgr_.GetPath();
    path += "/";
    path += filename;
    return path;
  }
};

TEST_F(PosixFSTest, PReadWrite) {
  std::string path = GetPath("hello.txt");
  int fd = open(path.c_str(), O_WRONLY | O_CREAT, 0777);
  ASSERT_NE(-1, fd);
  EXPECT_EQ(6, posix_fs_->Pwrite(fd, "hello\n", 6, 0));
  close(fd);
  fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(-1, fd);
  char buf[6] = {'\0'};
  EXPECT_EQ(5, posix_fs_->Pread(fd, buf, 5, 1));
  EXPECT_STREQ("ello\n", buf);
  close(fd);
  EXPECT_EQ(0, posix_fs_->Lsetxattr(path.c_str(), "user.test", "hello", 5, 0));
}

TEST_F(PosixFSTest, Sync) {
  posix_fs_->Sync();
}

}  // namespace
}  // namespace cpfs
