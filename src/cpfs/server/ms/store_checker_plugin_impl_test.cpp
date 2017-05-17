/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/store_checker_plugin_impl.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>

#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "console_mock.hpp"
#include "finfo.hpp"
#include "mock_actions.hpp"
#include "store_util.hpp"
#include "server/ms/store_checker_mock.hpp"

using ::testing::_;
using ::testing::Invoke;
using ::testing::MatchesRegex;
using ::testing::Return;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/store_test_XXXXXX";

class StoreCheckerPluginTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  MockIConsole console_;
  MockIStoreChecker store_checker_;

  StoreCheckerPluginTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()) {
    EXPECT_CALL(store_checker_, GetRoot())
        .WillRepeatedly(Return(data_path_));
    EXPECT_CALL(store_checker_, GetInodePath(_))
        .WillRepeatedly(Invoke(boost::bind(
            &StoreCheckerPluginTest::GetDirInodePath, this, _1)));
    EXPECT_CALL(store_checker_, GetDentryPath(_, _))
        .WillRepeatedly(Invoke(boost::bind(
            &StoreCheckerPluginTest::GetDirDentryPath, this, _1, _2)));
    EXPECT_CALL(store_checker_, GetConsole())
        .WillRepeatedly(Return(&console_));

    for (unsigned i = 0; i < kNumBaseDir; ++i)
      mkdir((boost::format("%s/%03x") % data_path_ % i).str().c_str(), 0777);
  }

  void SetParent(InodeNum inode, InodeNum parent) {
    std::string inode_path = GetDirInodePath(inode);
    close(open((inode_path + "x").c_str(),
               O_WRONLY | O_CREAT, 0755));
    SetDirParent(inode_path, GetInodeStr(parent));
  }

  std::string GetDirInodePath(InodeNum inode) {
    return (boost::format("%s/000/%05x") % data_path_ % inode).str();
  }

  std::string GetDirDentryPath(InodeNum inode, std::string name) {
    return (boost::format("%s/000/%05x/%s") % data_path_ % inode % name).str();
  }

  void PrepareInodeCtrl(InodeNum inode, bool set_dsg = false) {
    std::string inode_path = GetDirInodePath(inode);
    std::string ctrl_path = inode_path + "x";
    close(open(ctrl_path.c_str(), O_WRONLY | O_CREAT, 0644));
    FSTime ctime = {1234567890, 12345};
    SetCtrlCtime(inode_path, ctime);
    if (set_dsg) {
      std::vector<GroupId> groups;
      groups.push_back(1);
      SetFileGroupIds(inode_path, groups);
    }
  }
};

TEST_F(StoreCheckerPluginTest, BaseBasic) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeBaseStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  EXPECT_TRUE(plugin->InfoFound("foo", "bar"));
  EXPECT_TRUE(plugin->InfoScanCompleted());
  struct stat stbuf;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->UnknownNodeFound("c"));
  EXPECT_TRUE(plugin->InodeScanCompleted());
  EXPECT_TRUE(plugin->DentryFound(1, "foo", 3, &stbuf));
  EXPECT_TRUE(plugin->UnknownDentryFound(1, "c"));
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeBasicOkay) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2);
  SetParent(2, 1);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  PrepareInodeCtrl(4, true);
  stbuf.st_mode = S_IFREG | 0644;
  stbuf.st_nlink = 1;
  EXPECT_TRUE(plugin->InodeFound(4, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(4, &stbuf, -1));
  PrepareInodeCtrl(5, true);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(5, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(5, &stbuf, 1));
  EXPECT_TRUE(plugin->InodeFound(5, &stbuf, -1));
  EXPECT_TRUE(plugin->InodeScanCompleted());

  stbuf.st_mode = S_IFDIR | 0755;
  EXPECT_TRUE(plugin->DentryFound(1, "foo", 2, &stbuf));
  stbuf.st_mode = S_IFREG | 0644;
  EXPECT_TRUE(plugin->DentryFound(1, "bar", 4, &stbuf));
  EXPECT_TRUE(plugin->DentryFound(1, "bar2", 5, &stbuf));
  EXPECT_TRUE(plugin->DentryFound(2, "bar3", 5, &stbuf));
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeNoParentSet) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  EXPECT_TRUE(plugin->InodeScanCompleted());

  stbuf.st_mode = S_IFDIR | 0755;
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("No parent set for directory inode .*/00002")));
  EXPECT_FALSE(plugin->DentryFound(1, "foo", 2, &stbuf));
  stbuf.st_mode = S_IFREG | 0644;
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeNoControl) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Cannot find ctime for .*/000/00001")));
  EXPECT_FALSE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeScanCompleted());
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeMissingDSGAlloc) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  PrepareInodeCtrl(2);
  stbuf.st_mode = S_IFREG | 0644;
  stbuf.st_nlink = 1;

  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Cannot determine DSG allocation for .*/00002")));

  EXPECT_FALSE(plugin->InodeFound(2, &stbuf, 0));

  EXPECT_TRUE(plugin->InodeScanCompleted());
  EXPECT_TRUE(plugin->DentryFound(1, "foo", 2, &stbuf));
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeDanglingControl) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2);
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  EXPECT_TRUE(plugin->InodeScanCompleted());

  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Inode control file found for missing inode .*/00002")));

  EXPECT_FALSE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeParentMismatch) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  std::string inode2_path = std::string(data_path_) + "/00002";
  PrepareInodeCtrl(2);
  SetParent(2, 3);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  EXPECT_TRUE(plugin->InodeScanCompleted());

  stbuf.st_mode = S_IFDIR | 0755;
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Parent mismatched for .*/00001/foo vs .*/00002")));
  EXPECT_FALSE(plugin->DentryFound(1, "foo", 2, &stbuf));
  stbuf.st_mode = S_IFREG | 0644;
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeInodeDangling) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Dangling inode .*/00002")));
  EXPECT_FALSE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeMultiLinkDir) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2);
  SetParent(2, 1);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 1));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  EXPECT_TRUE(plugin->DentryFound(1, "bar", 2, &stbuf));
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Directory link inode .*/00002 found \\(max 1\\)")));
  EXPECT_FALSE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeDirTypeMismatch) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2);
  SetParent(2, 1);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  stbuf.st_mode = S_IFREG | 0644;
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex(
          "Dentry type does not match inode type for "
          ".*/00001/bar2 \\(file\\) vs "
          ".*/00002 \\(dir\\)")));
  try {
    plugin->DentryFound(1, "bar2", 2, &stbuf);
    FAIL();
  } catch(const SkipItemException& ex) {
    EXPECT_TRUE(ex.is_error);
  }
  stbuf.st_mode = S_IFDIR | 0755;
  plugin->DentryFound(1, "bar", 2, &stbuf);
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeMultiEntryDir) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2);
  SetParent(2, 1);
  stbuf.st_nlink = 2;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  EXPECT_TRUE(plugin->DentryFound(1, "bar", 2, &stbuf));
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Multiple dentries found for directory inode .*/00002: "
                   ".*/00001/bar2")));
  try {
    plugin->DentryFound(1, "bar2", 2, &stbuf);
    FAIL();
  } catch(const SkipItemException& ex) {
    EXPECT_TRUE(ex.is_error);
  }
  EXPECT_TRUE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, TreeLinkCountMismatch) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(MakeTreeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  PrepareInodeCtrl(1);
  struct stat stbuf;
  stbuf.st_mode = S_IFDIR | 0755;
  stbuf.st_nlink = 3;
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(1, &stbuf, -1));
  PrepareInodeCtrl(2, true);
  stbuf.st_mode = S_IFREG | 0644;
  stbuf.st_nlink = 1;
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, 1));
  EXPECT_TRUE(plugin->InodeFound(2, &stbuf, -1));
  EXPECT_TRUE(plugin->DentryFound(1, "bar", 2, &stbuf));
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("File link counts for .*/00002 mismatched: "
                   "dentry count 1, link count 1, link files 2, "
                   "max link file 1")));
  EXPECT_FALSE(plugin->DentryScanCompleted());
}

TEST_F(StoreCheckerPluginTest, SpecialInodeBasic) {
  boost::scoped_ptr<IStoreCheckerPlugin> plugin(
      MakeSpecialInodeStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  try {
    plugin->UnknownNodeFound("d");
    FAIL();
  } catch(const SkipItemException& ex) {
    EXPECT_FALSE(ex.is_error);
  }
  EXPECT_TRUE(plugin->UnknownNodeFound("e"));
}

TEST_F(StoreCheckerPluginTest, InodeCountBasic) {
  for (unsigned i = 0; i < kNumBaseDir; ++i)
    symlink((boost::format("%16x") % 0x100).str().c_str(),
            (boost::format("%s/%03x/c") % data_path_ % i).str().c_str());

  boost::scoped_ptr<IStoreCheckerPlugin> plugin(
      MakeInodeCountStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  EXPECT_TRUE(plugin->UnknownNodeFound("002/d"));
  try {
    plugin->UnknownNodeFound("002/c");
    FAIL();
  } catch(const SkipItemException& ex) {
    EXPECT_FALSE(ex.is_error);
  }
  EXPECT_TRUE(plugin->UnknownNodeFound("hello"));
  struct stat stbuf;
  EXPECT_TRUE(plugin->InodeFound(0x10, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(0x110, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeScanCompleted());
}

TEST_F(StoreCheckerPluginTest, InodeCountWideHole) {
  for (unsigned i = 0; i < kNumBaseDir; ++i)
    symlink((boost::format("%16x") % 0x100).str().c_str(),
            (boost::format("%s/%03x/c") % data_path_ % i).str().c_str());

  boost::scoped_ptr<IStoreCheckerPlugin> plugin(
      MakeInodeCountStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  EXPECT_TRUE(plugin->UnknownNodeFound("002/d"));
  struct stat stbuf;
  EXPECT_TRUE(plugin->InodeFound(0x10, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(0x130, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(0x140, &stbuf, 0));
  EXPECT_TRUE(plugin->InodeFound(0x180, &stbuf, 0));
  EXPECT_CALL(console_, PrintLine("Too wide inode hole starting from 0x100"));
  EXPECT_CALL(console_, PrintLine("Too wide inode hole starting from 0x141"));
  EXPECT_FALSE(plugin->InodeScanCompleted());
}

TEST_F(StoreCheckerPluginTest, InodeCountTooSmall) {
  for (unsigned i = 0; i < kNumBaseDir; ++i)
    symlink((boost::format("%16x") % 0x100).str().c_str(),
            (boost::format("%s/%03x/c") % data_path_ % i).str().c_str());

  boost::scoped_ptr<IStoreCheckerPlugin> plugin(
      MakeInodeCountStoreCheckerPlugin());
  EXPECT_TRUE(plugin->Init(&store_checker_, false));
  EXPECT_TRUE(plugin->UnknownNodeFound("002/d"));
  struct stat stbuf;
  for (InodeNum i = 0x100; i < 0x1000; ++i)
    EXPECT_TRUE(plugin->InodeFound(i, &stbuf, 0));
  EXPECT_CALL(console_, PrintLine("Inode count link value too small: is 0x100, "
                                  "need 0xfff"));
  EXPECT_FALSE(plugin->InodeScanCompleted());
}

TEST_F(StoreCheckerPluginTest, InodeCountFault) {
  symlink("hello", (std::string(data_path_) + "/000/c").c_str());

  boost::scoped_ptr<IStoreCheckerPlugin> plugin(
      MakeInodeCountStoreCheckerPlugin());
  EXPECT_CALL(console_, PrintLine(
      MatchesRegex("Cannot interpret inode count link .*/000/c")));
  EXPECT_FALSE(plugin->Init(&store_checker_, false));
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
