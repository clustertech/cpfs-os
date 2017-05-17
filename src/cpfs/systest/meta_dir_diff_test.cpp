/* Copyright 2014 ClusterTech Ltd */
#include "meta_dir_diff.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <sys/xattr.h>

#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mock_actions.hpp"

using ::testing::ElementsAre;

namespace cpfs {
namespace systest {
namespace {

class MetaDataTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;

  MetaDataTest()
      : data_path_mgr_("/tmp/meta_data_test_XXXXXX"),
        data_path_(data_path_mgr_.GetPath()) {
    // TODO(Joseph): WIP for #13801
    mkdir(GetRoot().c_str(), 0777);
  }

  std::string GetRoot() {
    // TODO(Joseph): WIP for #13801
    return std::string(data_path_) + "/000";
  }
};

TEST_F(MetaDataTest, FileNotFound) {
  EXPECT_THROW(MetaData("/no_such_path"), std::runtime_error);
}

TEST_F(MetaDataTest, RegularFile) {
  std::string file_path0 = GetRoot() + "/00000a";
  std::string file_path1 = GetRoot() + "/00000b";
  std::string file_path2 = GetRoot() + "/00000c";
  open(file_path0.c_str(), O_CREAT, 0644);
  open(file_path1.c_str(), O_CREAT, 0644);
  open(file_path2.c_str(), O_CREAT, 0444);  // Permission differs
  MetaData meta_data0(file_path0);
  MetaData meta_data1(file_path1);
  MetaData meta_data2(file_path2);
  std::vector<std::string> diff;
  EXPECT_FALSE(meta_data0.Diff(meta_data1, &diff, true));
  EXPECT_FALSE(meta_data0.Diff(meta_data1, &diff, false));
  EXPECT_TRUE(meta_data1.Diff(meta_data2, &diff, false));
  EXPECT_THAT(diff, ElementsAre("mode"));
}

TEST_F(MetaDataTest, Link) {
  std::string file_path0 = GetRoot() + "/00000a";
  std::string file_path1 = GetRoot() + "/00000b";
  std::string file_path2 = GetRoot() + "/00000c";
  symlink("../test", file_path0.c_str());
  symlink("../test", file_path1.c_str());
  symlink("../", file_path2.c_str());  // Link target differs
  MetaData meta_data0(file_path0);
  MetaData meta_data1(file_path1);
  MetaData meta_data2(file_path2);
  std::vector<std::string> diff;
  EXPECT_FALSE(meta_data0.Diff(meta_data1, &diff, false));
  EXPECT_FALSE(meta_data0.Diff(meta_data1, &diff, false));
  EXPECT_TRUE(meta_data1.Diff(meta_data2, &diff, false));
  EXPECT_THAT(diff, ElementsAre("link target"));
}

TEST_F(MetaDataTest, ExtendedAttrs) {
  std::string file_path0 = GetRoot() + "/000a";
  std::string file_path1 = GetRoot() + "/000b";
  std::string file_path2 = GetRoot() + "/000c";
  open(file_path0.c_str(), O_CREAT, 0644);
  open(file_path1.c_str(), O_CREAT, 0644);
  open(file_path2.c_str(), O_CREAT, 0644);
  char buff[] = "test";
  lsetxattr(file_path0.c_str(), "user.ct", buff, strlen(buff), 0);
  lsetxattr(file_path1.c_str(), "user.ct", buff, strlen(buff), 0);
  lsetxattr(file_path2.c_str(), "user.ct", buff, strlen(buff), 0);
  lsetxattr(file_path2.c_str(), "user.extra", "ab", 2, 0);  // Extra xattr
  MetaData meta_data0(file_path0);
  MetaData meta_data1(file_path1);
  MetaData meta_data2(file_path2);
  std::vector<std::string> diff;
  EXPECT_FALSE(meta_data0.Diff(meta_data1, &diff, false));
  EXPECT_FALSE(meta_data0.Diff(meta_data1, &diff, false));
  EXPECT_TRUE(meta_data1.Diff(meta_data2, &diff, false));
  EXPECT_THAT(diff, ElementsAre("xattrs"));
}

class MetaDirDiffTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;

  MetaDirDiffTest()
      : data_path_mgr_("/tmp/meta_dir_diff_test_XXXXXX"),
        data_path_(data_path_mgr_.GetPath()) {
    // TODO(Joseph): WIP for #13801
    mkdir(GetRoot().c_str(), 0777);
  }

  std::string GetRoot() {
    // TODO(Joseph): WIP for #13801
    return std::string(data_path_) + "/000";
  }
};

TEST_F(MetaDirDiffTest, CheckNoDir) {
  std::vector<std::string> paths(2);
  paths[0] = GetRoot() + "/ms1";
  paths[1] = GetRoot() + "/ms2";

  uint64_t num_checked;
  EXPECT_THROW(MetaDirDiff(paths[0], paths[1], &num_checked),
               std::runtime_error);
}

TEST_F(MetaDirDiffTest, CheckSuccess) {
  std::vector<std::string> paths(2);
  paths[0] = GetRoot() + "/ms1";
  paths[1] = GetRoot() + "/ms2";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);
  paths[0] += "/000";
  paths[1] += "/000";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);
  mkdir((paths[0] + "/sub1").c_str(), 0755);
  mkdir((paths[1] + "/sub1").c_str(), 0755);

  close(open((paths[0] + "/000a").c_str(), O_CREAT, 0644));
  close(open((paths[1] + "/000a").c_str(), O_CREAT, 0644));
  close(open((paths[0] + "/sub1/000b").c_str(), O_CREAT, 0644));
  close(open((paths[1] + "/sub1/000b").c_str(), O_CREAT, 0644));

  // One folder and two files
  uint64_t num_checked;
  EXPECT_TRUE(MetaDirDiff(paths[0], paths[1], &num_checked));
  EXPECT_EQ(2U, num_checked);
}

TEST_F(MetaDirDiffTest, SkipDir) {
  std::vector<std::string> paths(2);
  paths[0] = GetRoot() + "/ms1";
  paths[1] = GetRoot() + "/ms2";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);
  paths[0] += "/001";
  paths[1] += "/001";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);
  // Create '/c' in both MS1 and MS2
  mkdir((paths[0] + "/c").c_str(), 0755);
  mkdir((paths[1] + "/c").c_str(), 0755);
  // Extra 'r' directory in MS2
  mkdir((paths[1] + "/r").c_str(), 0755);
  // Extra file in MS1 '/c'
  symlink("0000000000004966", (paths[0] + "/c/00005").c_str());

  // Directories are skipped
  uint64_t num_checked;
  EXPECT_TRUE(MetaDirDiff(paths[0], paths[1], &num_checked));
  EXPECT_EQ(0U, num_checked);
}

TEST_F(MetaDirDiffTest, CheckDiffFound) {
  std::vector<std::string> paths(2);
  paths[0] = GetRoot() + "/ms1";
  paths[1] = GetRoot() + "/ms2";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);
  paths[0] += "/002";
  paths[1] += "/002";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);

  close(open((paths[0] + "/000a").c_str(), O_CREAT, 0644));
  close(open((paths[1] + "/000a").c_str(), O_CREAT, 0674));  // mode differ
  close(open((paths[0] + "/000b").c_str(), O_CREAT, 0644));
  close(open((paths[1] + "/000b").c_str(), O_CREAT, 0644));
  truncate((paths[1] + "/000b").c_str(), 2);  // size differ

  // One folder and two files
  uint64_t num_checked;
  EXPECT_FALSE(MetaDirDiff(paths[0], paths[1], &num_checked));
  EXPECT_EQ(2U, num_checked);
}

TEST_F(MetaDirDiffTest, CheckDiffExtraFileFound) {
  std::vector<std::string> paths(2);
  paths[0] = GetRoot() + "/ms1";
  paths[1] = GetRoot() + "/ms2";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);
  paths[0] += "/003";
  paths[1] += "/003";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);

  close(open((paths[0] + "/000a").c_str(), O_CREAT, 0644));  // Extra file
  close(open((paths[0] + "/000b").c_str(), O_CREAT, 0644));
  close(open((paths[0] + "/000d").c_str(), O_CREAT, 0644));
  close(open((paths[1] + "/000b").c_str(), O_CREAT, 0644));
  close(open((paths[1] + "/000c").c_str(), O_CREAT, 0644));  // Extra file
  close(open((paths[1] + "/000d").c_str(), O_CREAT, 0644));
  close(open((paths[1] + "/000e").c_str(), O_CREAT, 0644));  // Extra file

  // One folder and two files
  uint64_t num_checked;
  EXPECT_FALSE(MetaDirDiff(paths[0], paths[1], &num_checked));
  EXPECT_EQ(2U, num_checked);
}

TEST_F(MetaDirDiffTest, CheckDiffExtraFileFound2) {
  std::vector<std::string> paths(2);
  paths[0] = GetRoot() + "/ms1";
  paths[1] = GetRoot() + "/ms2";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);
  paths[0] += "/004";
  paths[1] += "/004";
  mkdir(paths[0].c_str(), 0755);
  mkdir(paths[1].c_str(), 0755);

  close(open((paths[0] + "/000a").c_str(), O_CREAT, 0644));  // Extra file

  // One folder and two files
  uint64_t num_checked;
  EXPECT_FALSE(MetaDirDiff(paths[0], paths[1], &num_checked));
  EXPECT_EQ(0U, num_checked);
}

}  // namespace
}  // namespace systest
}  // namespace cpfs
