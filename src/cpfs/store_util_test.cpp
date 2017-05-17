/* Copyright 2013 ClusterTech Ltd */
#include "store_util.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>

#include <sys/xattr.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <ctime>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "finfo.hpp"
#include "mock_actions.hpp"

using ::testing::ElementsAre;

namespace cpfs {
namespace {

const char* kDataPath = "/tmp/store_util_test_XXXXXX";

class StoreUtilTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;

  StoreUtilTest() : data_path_mgr_(kDataPath) {}

  std::string GetPath(std::string filename) {
    std::string path = data_path_mgr_.GetPath();
    path += "/";
    path += filename;
    return path;
  }

  void CreateEmptyFile(std::string path) {
    int fd = open(path.c_str(), O_CREAT | O_WRONLY, 0666);
    if (fd == -1)
      throw std::runtime_error("Unexpected failure in creating inode file");
    close(fd);
  }
};

TEST_F(StoreUtilTest, LListUserXattrEmpty) {
  std::string path = GetPath("hello");
  mkdir(path.c_str(), 0777);
  EXPECT_THAT(LListUserXattr(path.c_str()), ElementsAre());
}

TEST_F(StoreUtilTest, LListUserXattrNormal) {
  std::string path = GetPath("hello");
  mkdir(path.c_str(), 0777);
  LSetUserXattr(path.c_str(), "foo", "bar");
  LSetUserXattr(path.c_str(), "foo2", "bar2");
  std::vector<std::string> keys = LListUserXattr(path.c_str());
  std::sort(keys.begin(), keys.end());
  EXPECT_THAT(keys, ElementsAre("foo", "foo2"));
}

TEST_F(StoreUtilTest, LListUserXattrError) {
  EXPECT_THROW(LListUserXattr(GetPath("hello").c_str()), std::runtime_error);
  EXPECT_THROW(LSetUserXattr(GetPath("hello").c_str(), "foo", "bar"),
               std::runtime_error);
}

TEST_F(StoreUtilTest, LGetUserXattrNormal) {
  std::string path = GetPath("hello");
  mkdir(path.c_str(), 0777);
  LSetUserXattr(path.c_str(), "foo", "bar");
  EXPECT_EQ("bar", LGetUserXattr(path.c_str(), "foo"));
}

TEST_F(StoreUtilTest, LGetUserXattrLong) {
  std::string path = GetPath("hello");
  mkdir(path.c_str(), 0777);
  LSetUserXattr(path.c_str(), "foo", std::string(3000, '\0'));
  EXPECT_EQ(3000U, LGetUserXattr(path.c_str(), "foo").size());
}

TEST_F(StoreUtilTest, LGetUserXattrMissing) {
  std::string path = GetPath("hello");
  mkdir(path.c_str(), 0777);
  EXPECT_THROW(LGetUserXattr(path.c_str(), "foo"), std::runtime_error);
}

TEST_F(StoreUtilTest, DumpXAttr) {
  std::string path = GetPath("hello");
  mkdir(path.c_str(), 0777);
  LSetUserXattr(path.c_str(), "abc", "1234");
  std::string str_with_null("hello world\0\n123", 16);
  LSetUserXattr(path.c_str(), "testing", str_with_null);
  // Ignored
  lsetxattr(path.c_str(), "system.posix_acl_access", "1", 2, 0);
  XAttrList ret = DumpXAttr(path);
  ret.sort();
  EXPECT_THAT(ret, ElementsAre(std::make_pair("abc", "1234"),
                               std::make_pair("testing", str_with_null)));
  // Error case: No extended attribute
  std::string path2 = GetPath("hello2");
  CreateEmptyFile(path2);
  EXPECT_EQ(0U, DumpXAttr(path2).size());
  // Error case: No such file
  EXPECT_EQ(0U, DumpXAttr(GetPath("not_exists")).size());
}

TEST_F(StoreUtilTest, XAttrListSerialize) {
  XAttrList origin;
  origin.push_back(std::make_pair("user.abc", std::string(1000, 'c')));
  origin.push_back(std::make_pair("user.x", std::string(10, 'x')));
  std::string buf;
  buf = XAttrListToBuffer(origin);
  XAttrList deserialized = BufferToXattrList(buf);
  deserialized.sort();
  origin.sort();
  EXPECT_TRUE(deserialized == origin);
}

TEST_F(StoreUtilTest, Mod2FT) {
  EXPECT_EQ(DT_REG, Mod2FT(S_IFREG | 0777));
  EXPECT_EQ(DT_DIR, Mod2FT(S_IFDIR | 0777));
  EXPECT_EQ(DT_LNK, Mod2FT(S_IFLNK | 0777));
  EXPECT_EQ(DT_CHR, Mod2FT(S_IFCHR | 0777));
  EXPECT_EQ(DT_BLK, Mod2FT(S_IFBLK | 0777));
  EXPECT_EQ(DT_SOCK, Mod2FT(S_IFSOCK | 0777));
  EXPECT_EQ(DT_FIFO, Mod2FT(S_IFIFO | 0777));
  EXPECT_EQ(DT_UNKNOWN, Mod2FT(0777));
}

TEST_F(StoreUtilTest, GetInodeStr) {
  EXPECT_EQ(std::string("000000000000000b"), GetInodeStr(11));
}

TEST_F(StoreUtilTest, GetInodeFTStr) {
  EXPECT_EQ(std::string("R000000000000000b"), GetInodeFTStr(11, DT_REG));
  EXPECT_EQ(std::string("000000000000000b"), GetInodeFTStr(11, DT_UNKNOWN));
}

TEST_F(StoreUtilTest, GetFileTimeStr) {
  char buf[kMinFiletimeLen];
  FSTime time = {1234567890ULL, 12345};
  GetFileTimeStr(buf, time);
  EXPECT_STREQ("1234567890-12345", buf);
}

TEST_F(StoreUtilTest, ParseInodeNumFT) {
  InodeNum inode_num;
  unsigned char ft;
  std::string str = "R00005";
  ParseInodeNumFT(str.c_str(), str.length(), &inode_num, &ft);
  EXPECT_EQ(5U, inode_num);
  EXPECT_EQ(DT_REG, ft);
  str = "00006";
  ParseInodeNumFT(str.c_str(), str.length(), &inode_num, &ft);
  EXPECT_EQ(6U, inode_num);
  EXPECT_EQ(DT_UNKNOWN, ft);
}

TEST_F(StoreUtilTest, ParseClientNum) {
  ClientNum client_num;
  std::string str = "00005";
  ParseClientNum(str.c_str(), str.length(), &client_num);
  EXPECT_EQ(ClientNum(5), client_num);
}

TEST_F(StoreUtilTest, ParseFileSize) {
  uint64_t file_size;
  std::string str = "00005";
  ParseFileSize(str.c_str(), str.length(), &file_size);
  EXPECT_EQ(5U, file_size);
}

TEST_F(StoreUtilTest, SymlinkToInode) {
  // Normal case
  std::string path = GetPath("hello");
  symlink("1234567890", path.c_str());
  InodeNum inode_num;
  EXPECT_EQ(0, SymlinkToInode(path, &inode_num));
  EXPECT_EQ(0x1234567890U, inode_num);

  // Error case: no such file
  EXPECT_EQ(-ENOENT, SymlinkToInode(path + "1", &inode_num));

  // Error case: name too long
  unlink(path.c_str());
  symlink("12345678901234567", path.c_str());
  EXPECT_EQ(-EIO, SymlinkToInode(path, &inode_num));

  // Error case: cannot convert name
  unlink(path.c_str());
  symlink("123x", path.c_str());
  EXPECT_EQ(-EIO, SymlinkToInode(path, &inode_num));
}

TEST_F(StoreUtilTest, SymlinkToInodeFT) {
  // Normal case
  std::string path = GetPath("hello");
  symlink("L1234567890", path.c_str());
  InodeNum inode_num;
  unsigned char ftype;
  EXPECT_EQ(0, SymlinkToInodeFT(path, &inode_num, &ftype));
  EXPECT_EQ(0x1234567890U, inode_num);
  EXPECT_EQ(DT_LNK, ftype);

  // Error case: no such file
  EXPECT_EQ(-ENOENT, SymlinkToInodeFT(path + "1", &inode_num, &ftype));

  // Error case: name too long
  unlink(path.c_str());
  symlink("12345678901234567", path.c_str());
  EXPECT_EQ(-EIO, SymlinkToInodeFT(path, &inode_num, &ftype));

  // Error case: cannot convert name
  unlink(path.c_str());
  symlink("123x", path.c_str());
  EXPECT_EQ(-EIO, SymlinkToInodeFT(path, &inode_num, &ftype));
}

TEST_F(StoreUtilTest, XattrToInode) {
  // Normal case
  std::string path = GetPath("hello");
  mkdir(path.c_str(), 0777);
  lsetxattr(path.c_str(), "user.ino", "1234567890", 11, 0);
  InodeNum inode_num;
  EXPECT_EQ(0, XattrToInode(path, &inode_num));
  EXPECT_EQ(0x1234567890U, inode_num);

  // Error case: no such file
  EXPECT_EQ(-ENOENT, XattrToInode(path + "1", &inode_num));

  // Error case: name too long
  lsetxattr(path.c_str(), "user.ino", "12345678901234567", 17, 0);
  EXPECT_EQ(-EIO, XattrToInode(path, &inode_num));
}

TEST_F(StoreUtilTest, DentryToInode) {
  // Normal cases
  std::string path = GetPath("hello");
  symlink("1234567890", path.c_str());
  struct stat stbuf;
  InodeNum inode_num;
  EXPECT_EQ(0, DentryToInode(path, &inode_num, &stbuf));
  EXPECT_EQ(0x1234567890U, inode_num);
  EXPECT_TRUE(S_ISLNK(stbuf.st_mode));

  std::string path2 = GetPath("world");
  mkdir(path2.c_str(), 0777);
  lsetxattr(path2.c_str(), "user.ino", "1234567890", 11, 0);
  EXPECT_EQ(0, DentryToInode(path2, &inode_num, &stbuf));
  EXPECT_EQ(0x1234567890U, inode_num);
  EXPECT_TRUE(S_ISDIR(stbuf.st_mode));

  // Error case: no such file
  EXPECT_EQ(-ENOENT, DentryToInode(path + "1", &inode_num, &stbuf));
}

TEST_F(StoreUtilTest, GetFileAttr) {
  std::string path = GetPath("0000000000000001");
  std::string control_path = path + "x";
  CreateEmptyFile(path);
  CreateEmptyFile(control_path);
  std::vector<GroupId> groups;
  groups.push_back(1);
  SetFileGroupIds(path, groups);
  FSTime ctime = {1234567800, 0};
  EXPECT_TRUE(SetCtrlCtime(path, ctime));

  struct stat stbuf;
  FileAttr fa;
  GetFileAttr(path, &fa);
  stat(path.c_str(), &stbuf);
  EXPECT_EQ(stbuf.st_mode, fa.mode);
  EXPECT_EQ(stbuf.st_nlink, fa.nlink);
  EXPECT_EQ(stbuf.st_uid, fa.uid);
  EXPECT_EQ(stbuf.st_gid, fa.gid);
  EXPECT_EQ(stbuf.st_rdev, fa.rdev);
  EXPECT_EQ(size_t(stbuf.st_size), size_t(fa.size));
  EXPECT_EQ(uint64_t(stbuf.st_atim.tv_sec), fa.atime.sec);
  EXPECT_EQ(uint64_t(stbuf.st_atim.tv_nsec), fa.atime.ns);
  EXPECT_EQ(uint64_t(stbuf.st_mtim.tv_sec), fa.mtime.sec);
  EXPECT_EQ(uint64_t(stbuf.st_mtim.tv_nsec), fa.mtime.ns);
  EXPECT_EQ(1234567800U, fa.ctime.sec);
  EXPECT_EQ(0U, fa.ctime.ns);
}

TEST_F(StoreUtilTest, GetFileAttrError) {
  // File not exists
  FileAttr fa;
  EXPECT_LT(GetFileAttr(GetPath("not_exists"), &fa), 0);

  // No ctime xattr in control file
  mkdir(GetPath("0000000000000001").c_str(), 0755);
  CreateEmptyFile(GetPath("0000000000000001x"));
  EXPECT_LT(GetFileAttr(GetPath("0000000000000001"), &fa), 0);

  // Corrupted ctime xattr
  setxattr(GetPath("0000000000000001x").c_str(), "user.ct", "#", 1, 0);
  EXPECT_LT(GetFileAttr(GetPath("0000000000000001"), &fa), 0);
}

TEST_F(StoreUtilTest, ReadLinkTarget) {
  std::string path = GetPath("test");
  symlink("../target", path.c_str());
  std::vector<char> ret;
  EXPECT_EQ(0, ReadLinkTarget(path, &ret));
  EXPECT_EQ(std::string("../target"), std::string(&ret[0]));
}

TEST_F(StoreUtilTest, ReadLinkTargetError) {
  // Link does not exists
  std::vector<char> ret;
  EXPECT_LT(ReadLinkTarget(GetPath("not_exists"), &ret), 0);
}

TEST_F(StoreUtilTest, GetFileGroupIds) {
  CreateEmptyFile(GetPath("0000000000000001x"));
  std::vector<GroupId> groups;
  groups.push_back(1);
  groups.push_back(3);
  SetFileGroupIds(GetPath("0000000000000001"), groups);
  groups.clear();
  GetFileGroupIds(GetPath("0000000000000001"), &groups);
  ASSERT_EQ(2U, groups.size());
  EXPECT_EQ(1U, groups[0]);
  EXPECT_EQ(3U, groups[1]);
}

TEST_F(StoreUtilTest, GetFileGroupIdsError) {
  std::vector<GroupId> groups;
  // Control file does not exists
  EXPECT_LT(GetFileGroupIds(GetPath("0000000000000001"), &groups), 0);

  // Corrupted xattr
  CreateEmptyFile(GetPath("0000000000000001x"));
  setxattr(GetPath("0000000000000001x").c_str(), "user.sg", "1=", 3, 0);
  EXPECT_LT(GetFileGroupIds(GetPath("0000000000000001"), &groups), 0);
}

TEST_F(StoreUtilTest, DirParent) {
  std::string path = GetPath("0000000000000001");
  std::string control_path = path + "x";
  CreateEmptyFile(path);
  EXPECT_THROW(SetDirParent(path, "0000000000000002"), std::runtime_error);
  CreateEmptyFile(control_path);
  EXPECT_THROW(GetDirParent(path), std::runtime_error);
  SetDirParent(path, "0000000000000002");
  EXPECT_EQ(2U, GetDirParent(path));
  SetDirParent(path, "000000000000000g");
  EXPECT_THROW(GetDirParent(path), std::runtime_error);
}

TEST_F(StoreUtilTest, RemoveDirInode) {
  // Non empty directory
  mkdir(GetPath("test").c_str(), 0755);
  CreateEmptyFile(GetPath("test/file1"));
  EXPECT_EQ(0, RemoveDirInode(GetPath("test")));
  struct stat stbuf;
  EXPECT_LT(stat(GetPath("test").c_str(), &stbuf), 0);

  // Empty directory
  mkdir(GetPath("test2").c_str(), 0755);
  EXPECT_EQ(0, RemoveDirInode(GetPath("test2")));

  // Error case: Directory not exists
  EXPECT_NE(0, RemoveDirInode(GetPath("notexist")));

  // Error case: No permission to remove file
  mkdir(GetPath("test3").c_str(), 0755);
  mkdir(GetPath("test3/a").c_str(), 0755);
  chmod(GetPath("test3").c_str(), 0555);
  EXPECT_LT(RemoveDirInode(GetPath("test3")), 0);

  // Error case: No permission to remove directory
  mkdir(GetPath("test4").c_str(), 0755);
  CreateEmptyFile(GetPath("test4/b"));
  chmod(GetPath("test4").c_str(), 0555);
  EXPECT_LT(RemoveDirInode(GetPath("test4")), 0);
}

TEST_F(StoreUtilTest, LoadOrCreateUUID) {
  std::string uuid = LoadOrCreateUUID(data_path_mgr_.GetPath());
  // Reload
  EXPECT_EQ(uuid, LoadOrCreateUUID(data_path_mgr_.GetPath()));
  EXPECT_EQ(36U, uuid.length());
  // New uuid is generated
  removexattr(data_path_mgr_.GetPath(), "user.uuid");
  std::string new_uuid = LoadOrCreateUUID(data_path_mgr_.GetPath());
  EXPECT_NE(uuid, new_uuid);
  EXPECT_EQ(36U, new_uuid.length());
  // Error case: No permission to persist new UUID
  removexattr(data_path_mgr_.GetPath(), "user.uuid");
  chmod(data_path_mgr_.GetPath(), 0555);
  EXPECT_THROW(LoadOrCreateUUID(data_path_mgr_.GetPath()),
               std::runtime_error);
}

TEST_F(StoreUtilTest, InodeStrConv) {
  EXPECT_EQ(
      std::string("fa0/fa01234567890101"),
      GetPrefixedInodeStr(0xfa01234567890101L));

  EXPECT_EQ(
      std::string("fa01234567890101"),
      StripPrefixInodeStr("fa0/fa01234567890101"));

  EXPECT_THROW(StripPrefixInodeStr("ab"), std::out_of_range);
}

}  // namespace
}  // namespace cpfs
