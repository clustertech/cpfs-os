/* Copyright 2013 ClusterTech Ltd */
#include "dir_iterator_impl.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>

#include <algorithm>
#include <ctime>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "mock_actions.hpp"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Mock;
using ::testing::Return;

namespace cpfs {
namespace {

const char* kDataPath = "/tmp/dir_iterator_test_XXXXXX";

class DirIteratorTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  DirIteratorTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()) {}

  std::string GetFilename(const char* filename) const {
    return std::string(data_path_) + "/" + filename;
  }
};

TEST_F(DirIteratorTest, BaseList) {
  std::string paths[4];
  paths[0] = GetFilename("a");
  paths[1] = GetFilename("hello");
  paths[2] = GetFilename("b");
  paths[3] = GetFilename("test");

  mkdir(paths[0].c_str(), 0755);
  symlink("1234567890", paths[1].c_str());
  close(open(paths[2].c_str(), O_CREAT | O_WRONLY, 0666));
  mkdir(paths[3].c_str(), 0755);

  boost::scoped_ptr<IDirIterator> dir_iterator(MakeDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  std::string name;
  bool is_dir;
  boost::unordered_map<std::string, bool> entries;
  struct stat stbuf;
  while (dir_iterator->GetNext(&name, &is_dir, &stbuf)) {
    entries[name] = is_dir;
    EXPECT_EQ(is_dir, S_ISDIR(stbuf.st_mode));
  }

  ASSERT_EQ(4U, entries.size());
  EXPECT_EQ(true, entries.at("a"));
  EXPECT_EQ(false, entries.at("hello"));
  EXPECT_EQ(false, entries.at("b"));
  EXPECT_EQ(true, entries.at("test"));

  // Error case: no entry
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir));
}

TEST_F(DirIteratorTest, BaseFilterCTime) {
  std::string paths[2];
  paths[0] = GetFilename("a");
  paths[1] = GetFilename("b");

  mkdir(paths[0].c_str(), 0755);
  symlink("1234567890", paths[1].c_str());

  boost::scoped_ptr<IDirIterator> dir_iterator(MakeDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  time_t now = std::time(0);
  dir_iterator->SetFilterCTime(now + 60);
  std::string name;
  bool is_dir;
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir));

  // All entries match the ctime
  dir_iterator.reset(MakeDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  dir_iterator->SetFilterCTime(1);
  boost::unordered_map<std::string, bool> entries;
  while (dir_iterator->GetNext(&name, &is_dir))
    entries[name] = is_dir;

  ASSERT_EQ(2U, entries.size());
  EXPECT_EQ(true, entries.at("a"));
  EXPECT_EQ(false, entries.at("b"));
}

TEST_F(DirIteratorTest, BaseOpenAndGetNextError) {
  // Error case: no permission to opendir()
  chmod(data_path_, 0);
  boost::scoped_ptr<IDirIterator> dir_iterator(MakeDirIterator(data_path_));
  EXPECT_TRUE(dir_iterator->missing());
  std::string name;
  bool is_dir;
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir));
}

TEST_F(DirIteratorTest, BaseGetNextStatError) {
  boost::scoped_ptr<IDirIterator> dir_iterator(MakeDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  mkdir(GetFilename("test").c_str(), 0755);
  // Error case: no permission to use lstat()
  chmod(data_path_, 0);
  std::string name;
  bool is_dir;
  struct stat st_buf;
  EXPECT_THROW(dir_iterator->GetNext(&name, &is_dir, &st_buf),
               std::runtime_error);
}

TEST_F(DirIteratorTest, BaseStatAvoidance) {
  boost::scoped_ptr<IDirIterator> dir_iterator(MakeDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());

  mkdir(GetFilename("test").c_str(), 0777);
  // won't try lstat(), so the following does not cause an error
  chmod(data_path_, 0);

  std::string name;
  bool is_dir;
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ("test", name);
  EXPECT_TRUE(is_dir);
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir));

  chmod(data_path_, 0755);  // Clean up

  // Try file as well
  rmdir(GetFilename("test").c_str());
  close(open(GetFilename("test").c_str(), O_WRONLY | O_CREAT, 0666));

  dir_iterator.reset(MakeDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  chmod(data_path_, 0);

  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ("test", name);
  EXPECT_FALSE(is_dir);
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir));
}

TEST_F(DirIteratorTest, LeafViewNormal) {
  std::vector<std::string> paths;
  paths.push_back(GetFilename("000"));
  paths.push_back(GetFilename("001"));
  paths.push_back(GetFilename("123"));
  paths.push_back(GetFilename("c"));
  paths.push_back(GetFilename("000/s1"));
  paths.push_back(GetFilename("000/s2"));
  paths.push_back(GetFilename("001/s3"));

  mkdir(paths[0].c_str(), 0755);  // 000 (parent dir)
  mkdir(paths[1].c_str(), 0755);  // 001 (parent dir)
  symlink("a", paths[2].c_str());  // 123 (file)
  mkdir(paths[3].c_str(), 0755);  // c (parent dir)
  symlink("c", paths[4].c_str());  // 000/s1 (subdir file)
  mkdir(paths[5].c_str(), 0755);  // 000/s2 (subdir dir)
  symlink("e", paths[6].c_str());  // 001/s3 (subdir file)

  boost::scoped_ptr<IDirIterator> dir_iterator(
      MakeLeafViewDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  std::vector<std::string> found;
  std::vector<std::string> dirs;
  std::string name;
  bool is_dir;
  while (dir_iterator->GetNext(&name, &is_dir)) {
    found.push_back(name);
    if (is_dir)
      dirs.push_back(name);
  }
  std::sort(found.begin(), found.end());
  std::sort(dirs.begin(), dirs.end());
  // Root "123" and "ff" will be skipped
  ASSERT_THAT(found, ElementsAre("000/s1", "000/s2", "001/s3", "123", "c"));
  ASSERT_THAT(dirs, ElementsAre("000/s2", "c"));
}

TEST_F(DirIteratorTest, LeafViewFilterCTime) {
  std::string paths[5];
  paths[0] = GetFilename("000");
  paths[1] = GetFilename("000/123");
  paths[2] = GetFilename("000/456");
  paths[3] = GetFilename("001");
  paths[4] = GetFilename("001/123");

  mkdir(paths[0].c_str(), 0755);
  symlink("1234567890", paths[1].c_str());
  symlink("a234567890", paths[2].c_str());
  mkdir(paths[3].c_str(), 0755);
  symlink("b234567890", paths[4].c_str());

  std::string name;
  bool is_dir;
  // Show only files with ctime t + 60
  boost::scoped_ptr<IDirIterator> dir_iterator(
      MakeLeafViewDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  time_t now = std::time(0);
  dir_iterator->SetFilterCTime(now + 60);
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir));

  // Next
  dir_iterator.reset(MakeLeafViewDirIterator(data_path_));
  EXPECT_FALSE(dir_iterator->missing());
  dir_iterator->SetFilterCTime(0);
  std::vector<std::string> found;
  while (dir_iterator->GetNext(&name, &is_dir))
    found.push_back(name);
  std::sort(found.begin(), found.end());
  ASSERT_THAT(found, ElementsAre("000/123", "000/456", "001/123"));
}

TEST_F(DirIteratorTest, InodeRangesNormal) {
  std::vector<InodeNum> paths;
  mkdir(GetFilename("000").c_str(), 0777);
  symlink("a", GetFilename("000/0000000000000001").c_str());
  symlink("a", GetFilename("000/000000000000000b").c_str());
  symlink("a", GetFilename("000/000000000000000f").c_str());
  symlink("a", GetFilename("000/000000000000002b").c_str());
  mkdir(GetFilename("001").c_str(), 0777);
  symlink("a", GetFilename("001/001000000000000a").c_str());
  symlink("a", GetFilename("001/001000000000000b").c_str());
  mkdir(GetFilename("002").c_str(), 0777);
  mkdir(GetFilename("014").c_str(), 0777);
  symlink("a", GetFilename("014/0140000000000000").c_str());
  symlink("a", GetFilename("014/014000000000001b").c_str());
  symlink("a", GetFilename("014/014000000000001f").c_str());
  symlink("a", GetFilename("014/0140000000000020").c_str());

  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  ranges.push_back(0x20000000000000);
  ranges.push_back(0x30000000000000);
  ranges.push_back(0x140000000000000);
  boost::scoped_ptr<IDirIterator> dir_iterator(
      MakeInodeRangeDirIterator(data_path_, ranges));
  EXPECT_FALSE(dir_iterator->missing());
  std::string name;
  bool is_dir = true;
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "000/0000000000000001");
  EXPECT_FALSE(is_dir);
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "000/000000000000000b");
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "000/000000000000000f");
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "014/0140000000000000");
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "014/014000000000001b");
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "014/014000000000001f");
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir));
}

TEST_F(DirIteratorTest, InodeRangesFilter) {
  std::vector<InodeNum> paths;
  mkdir(GetFilename("000").c_str(), 0777);
  symlink("a", GetFilename("000/000000000000000a").c_str());
  sleep(1);
  time_t limit = time(0);
  symlink("a", GetFilename("000/000000000000000b").c_str());

  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  boost::scoped_ptr<IDirIterator> dir_iterator(
      MakeInodeRangeDirIterator(data_path_, ranges));
  EXPECT_FALSE(dir_iterator->missing());
  dir_iterator->SetFilterCTime(limit);
  struct stat stbuf;
  std::string name;
  bool is_dir;
  EXPECT_TRUE(dir_iterator->GetNext(&name, &is_dir, &stbuf));
  EXPECT_EQ(name, "000/000000000000000b");
  EXPECT_EQ(1U, stbuf.st_nlink);
  EXPECT_TRUE(S_ISLNK(stbuf.st_mode));
  EXPECT_FALSE(dir_iterator->GetNext(&name, &is_dir, &stbuf));
}

}  // namespace
}  // namespace cpfs
