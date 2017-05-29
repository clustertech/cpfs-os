/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/store_impl.hpp"

#include <fcntl.h>
#include <fuse_lowlevel.h>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/xattr.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/format.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "finfo.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "store_util.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"
#include "server/ms/dsg_alloc_mock.hpp"
#include "server/ms/inode_mutex.hpp"
#include "server/ms/inode_src_mock.hpp"
#include "server/ms/inode_usage_mock.hpp"
#include "server/ms/store.hpp"
#include "server/ms/ugid_handler_mock.hpp"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Mock;
using ::testing::StartsWith;
using ::testing::Return;
using ::testing::ReturnNew;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/store_test_XXXXXX";
const InodeNum kIMissing = InodeNum(-1);

class MSStoreTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  boost::scoped_ptr<IStore> store_;
  boost::scoped_ptr<MockIDSGAllocator> dsg_allocator_;
  boost::scoped_ptr<MockIInodeSrc> inode_src_;
  boost::scoped_ptr<MockIUgidHandler> ugid_handler_;
  boost::scoped_ptr<MockIInodeUsage> inode_usage_;
  boost::scoped_ptr<MockIInodeRemovalTracker> inode_removal_tracker_;
  boost::scoped_ptr<MockIDurableRange> durable_range_;
  static const uint32_t kTestUid;
  static const uint32_t kTestGid;
  static const uint64_t kTestOptime;
  ReqContext req_context_;
  OpContext op_context_;

  MSStoreTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        store_(MakeStore(data_path_)),
        dsg_allocator_(new MockIDSGAllocator),
        inode_src_(new MockIInodeSrc),
        ugid_handler_(new MockIUgidHandler),
        inode_usage_(new MockIInodeUsage),
        inode_removal_tracker_(new MockIInodeRemovalTracker),
        durable_range_(new MockIDurableRange) {
    store_->SetDSGAllocator(dsg_allocator_.get());
    store_->SetInodeSrc(inode_src_.get());
    store_->SetUgidHandler(ugid_handler_.get());
    store_->SetInodeUsage(inode_usage_.get());
    store_->SetInodeRemovalTracker(inode_removal_tracker_.get());
    store_->SetDurableRange(durable_range_.get());
    req_context_.uid = kTestUid;
    req_context_.gid = kTestGid;
    req_context_.optime.sec = kTestOptime;
    req_context_.optime.ns = 0;
    op_context_.req_context = &req_context_;
  }

  void VerifyMocks() {
    Mock::VerifyAndClear(inode_src_.get());
    Mock::VerifyAndClear(ugid_handler_.get());
  }

  std::string EntryFilename(InodeNum parent, std::string filename) {
    return (boost::format("%s/000/%016x/%s") % data_path_ % parent % filename)
        .str();
  }

  std::string InodeFilename(InodeNum inode) {
    return (boost::format("%s/000/%016x") % data_path_ % inode).str();
  }

  void CreateRawFile(std::string filename) {
    int fd = open(filename.c_str(), O_CREAT | O_WRONLY, 0666);
    if (fd == -1)
      throw std::runtime_error("Unexpected failure in creating file"
                               + filename);
    close(fd);
  }

  void CreateControlFile(InodeNum inode, const char* ctime_str) {
    std::string ctrl_filename = InodeFilename(inode) + "x";
    CreateRawFile(ctrl_filename);
    lsetxattr(ctrl_filename.c_str(), "user.sg", "1", 1, 0);
    lsetxattr(ctrl_filename.c_str(), "user.ct",
              ctime_str, std::strlen(ctime_str), 0);
  }

  void CreateFile(InodeNum parent, InodeNum inode, std::string filename) {
    if (symlink((boost::format("%016x") % inode).str().c_str(),
                EntryFilename(parent, filename).c_str()) != 0)
      throw std::runtime_error("Unexpected failure in creating file entry");
    CreateRawFile(InodeFilename(inode));
    CreateControlFile(inode, "1234567800-0");
  }

  struct stat GetStat(InodeNum inode) {
    struct stat ret;
    if (lstat(InodeFilename(inode).c_str(), &ret) != 0)
      throw std::runtime_error("Unexpected failure in getting inode stat");
    return ret;
  }

  void CreateDirectory(InodeNum parent, InodeNum inode, std::string filename) {
    std::string dentry_path = EntryFilename(parent, filename);
    if (mkdir(dentry_path.c_str(), 0755) == -1)
      throw std::runtime_error("Unexpected failure in creating dentry");
    std::string inode_path = InodeFilename(inode).c_str();
    if (mkdir(inode_path.c_str(), 0755) == -1)
      throw std::runtime_error("Unexpected failure in creating inode dir");
    char inode_s[32] = {'\0'};
    int len = snprintf(inode_s, sizeof(inode_s), "%" PRIX64, inode);
    lsetxattr(dentry_path.c_str(), "user.ino", inode_s, len, 0);
    CreateControlFile(inode, "1234567800-0");
    len = snprintf(inode_s, sizeof(inode_s), "%" PRIX64, parent);
    lsetxattr((inode_path + "x").c_str(), "user.par", inode_s, len, 0);
  }
};

const uint32_t MSStoreTest::kTestUid = 100;
const uint32_t MSStoreTest::kTestGid = 100;
const uint64_t MSStoreTest::kTestOptime = 1234567890;

TEST_F(MSStoreTest, List) {
  store_->Initialize();
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  CreateDirReq create_req = {3, 0744};
  FileAttr fa_ret;
  EXPECT_EQ(0, store_->Mkdir(&op_context_, 1, "testd1", &create_req,
                             &fa_ret));

  VerifyMocks();

  boost::scoped_ptr<IDirIterator> dir1(store_->List(""));
  std::string name;
  bool is_dir;
  EXPECT_TRUE(dir1->GetNext(&name, &is_dir));
  boost::scoped_ptr<IDirIterator> dir2(
      store_->List((boost::format("000/%016x") % 1).str()));
  EXPECT_TRUE(dir2->GetNext(&name, &is_dir));

  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  dir1.reset(store_->InodeList(ranges));
  EXPECT_TRUE(dir1->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "000/0000000000000001");
  EXPECT_TRUE(is_dir);
  EXPECT_TRUE(dir1->GetNext(&name, &is_dir));
  EXPECT_EQ(name, "000/0000000000000003");
  EXPECT_TRUE(is_dir);
  EXPECT_FALSE(dir1->GetNext(&name, &is_dir));
}

TEST_F(MSStoreTest, Init) {
  EXPECT_FALSE(store_->IsInitialized());
  store_->Initialize();
  EXPECT_TRUE(store_->IsInitialized());
  store_->PrepareUUID();
  std::string uuid = store_->GetUUID();
  EXPECT_EQ(36U, uuid.length());

  // Can be initialized more than once
  store_->Initialize();
  EXPECT_TRUE(store_->IsInitialized());
}

TEST_F(MSStoreTest, Init2) {
  // Can be initialized even if data dir is missing
  rmdir(data_path_);
  EXPECT_FALSE(store_->IsInitialized());
  store_->Initialize();
  EXPECT_TRUE(store_->IsInitialized());
  std::string fpath;
  {
    // Cannot be initialized if even the parent is missing
    boost::scoped_ptr<IStore> store2;
    fpath = (boost::format("%s/a/b") % data_path_).str();
    store2.reset(MakeStore(fpath));
    EXPECT_THROW(store2->Initialize(), std::runtime_error);
    // Exist, but is another type of object
    fpath = (boost::format("%s/a") % data_path_).str();
    system(("touch " + fpath).c_str());
    store2.reset(MakeStore(fpath));
    EXPECT_THROW(store2->Initialize(), std::runtime_error);
  }
  // Similar, but for the case creating root directory
  fpath = (boost::format("%s/000/0000000000000001") % data_path_).str();
  system(("rm -rf " + fpath).c_str());
  system(("touch " + fpath).c_str());
  EXPECT_THROW(store_->Initialize(), std::runtime_error);
}

TEST_F(MSStoreTest, OperateInode) {
  store_->Initialize();
  store_->OperateInode(&op_context_, 3);
  EXPECT_TRUE(op_context_.i_mutex_guard);
}

TEST_F(MSStoreTest, GetUpdateInodeAttr) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  FSTime mtime = {123456789, 123456};
  EXPECT_EQ(0, store_->UpdateInodeAttr(3, mtime, 4096));

  FileAttr fa_ret;
  EXPECT_EQ(0, store_->GetInodeAttr(3, &fa_ret));
  EXPECT_EQ(mtime, fa_ret.mtime);
  EXPECT_EQ(4096U, fa_ret.size);

  EXPECT_EQ(-ENOENT, store_->UpdateInodeAttr(2, mtime, 4096));
  EXPECT_EQ(-ENOENT, store_->GetInodeAttr(2, &fa_ret));
}

TEST_F(MSStoreTest, AttrModeSize) {
  store_->Initialize();
  CreateFile(1, 3, "hello");
  FileAttr fa;
  fa.mode = 0705;
  fa.size = 4096;
  FileAttr fa_ret;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Attr(
      &op_context_,
      3, &fa, FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_SIZE, true, &fa_ret));
  VerifyMocks();
  EXPECT_EQ(0705U, fa_ret.mode & 07777);
  EXPECT_EQ(4096U, fa_ret.size);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));
}

TEST_F(MSStoreTest, AttrModeSizeUnlocked) {
  store_->Initialize();
  CreateFile(1, 3, "hello");
  FileAttr fa;
  fa.mode = 0705;
  fa.size = 4096;
  FileAttr fa_ret;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  fa.size = 4095;
  EXPECT_THROW(store_->Attr(&op_context_, 3, &fa,
                            FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_SIZE,
                            false, &fa_ret),
               MDNeedLock);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
  VerifyMocks();
}

TEST_F(MSStoreTest, AttrTimes) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  // Set both mtime and atime
  FileAttr fa;
  fa.mtime.sec = 123456789;
  fa.mtime.ns = 0;
  fa.atime.sec = 123456790;
  fa.atime.ns = 0;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  FileAttr fa_ret;
  EXPECT_EQ(0, store_->Attr(
      &op_context_, 3, &fa, FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_ATIME,
      true, &fa_ret));
  VerifyMocks();
  EXPECT_EQ(123456789U, fa_ret.mtime.sec);
  EXPECT_EQ(123456790U, fa_ret.atime.sec);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));

  // Set only mtime
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  fa.mtime.sec = 123456791;
  fa.atime.sec = 123456792;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Attr(&op_context_, 3, &fa, FUSE_SET_ATTR_MTIME,
                            true, &fa_ret));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));
  VerifyMocks();
  EXPECT_EQ(123456791U, fa_ret.mtime.sec);
  EXPECT_EQ(123456790U, fa_ret.atime.sec);

  // Set only atime
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  fa.mtime.sec = 123456793;
  fa.atime.sec = 123456794;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Attr(&op_context_, 3, &fa, FUSE_SET_ATTR_ATIME,
                            true, &fa_ret));
  VerifyMocks();
  EXPECT_EQ(123456791U, fa_ret.mtime.sec);
  EXPECT_EQ(123456794U, fa_ret.atime.sec);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));
}

TEST_F(MSStoreTest, AttrModeMtimeUnlocked) {
  store_->Initialize();
  CreateFile(1, 3, "hello");
  FileAttr fa;
  fa.mtime.sec = 123456789;
  fa.mtime.ns = 0;
  FileAttr fa_ret;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  fa.size = 4095;
  EXPECT_THROW(store_->Attr(&op_context_, 3, &fa, FUSE_SET_ATTR_MTIME,
                            false, &fa_ret),
               MDNeedLock);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
  VerifyMocks();
}

TEST_F(MSStoreTest, AttrOwnership) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  // Set uid
  FileAttr fa;
  FileAttr fa_ret;
  fa.uid = (getuid() == 0) ? 100 : getuid();
  fa.gid = 0;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Attr(&op_context_, 3, &fa, FUSE_SET_ATTR_UID,
                            true, &fa_ret));
  VerifyMocks();

  EXPECT_EQ(fa.uid, fa_ret.uid);

  // Set gid
  fa.uid = 0;
  fa.gid = (getuid() == 0) ? 100 : getgid();
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Attr(&op_context_, 3, &fa, FUSE_SET_ATTR_GID,
                            true, &fa_ret));
  VerifyMocks();

  EXPECT_EQ(fa.gid, fa_ret.gid);

  // Error case
  fa.uid = 65530;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EPERM, store_->Attr(
      &op_context_, 3, &fa, FUSE_SET_ATTR_UID, true, &fa_ret));
  VerifyMocks();
}

TEST_F(MSStoreTest, AttrErrors) {
  store_->Initialize();
  CreateFile(1, 3, "hello");
  FileAttr fa;
  FileAttr fa_ret;

  // No such file for read
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-ENOENT, store_->Attr(&op_context_, 4, 0, 0, true, &fa_ret));
  VerifyMocks();
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(4));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // No permission to access data directory
  chmod(data_path_, 0);

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  fa.mode = 0605;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EACCES, store_->Attr(
      &op_context_, 3, &fa, FUSE_SET_ATTR_MODE, true, &fa_ret));
  VerifyMocks();
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Invalid size
  chmod(data_path_, 0755);
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  fa.size = -1;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EINVAL, store_->Attr(
      &op_context_, 3, &fa, FUSE_SET_ATTR_SIZE, true, &fa_ret));
  VerifyMocks();
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
}

TEST_F(MSStoreTest, XAttrSet) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->SetXattr(&op_context_, 3, "user.CPFS.ndsg", "10", 2, 0));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));

  // No permission to access data directory
  chmod(data_path_, 0);
  op_context_.inodes_changed.clear();

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EPERM,
      store_->SetXattr(&op_context_, 3, "user.CPFS.ndsg", "10", 2, 0));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
}

TEST_F(MSStoreTest, XAttrGet) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  std::string fpath = InodeFilename(3);
  lsetxattr(fpath.c_str(), "user.CPFS.ndsg", "10", 2, 0);

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  char buf[2];
  EXPECT_EQ(2, store_->GetXattr(
      &op_context_, 3, "user.CPFS.ndsg", buf, 2));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_EQ("10", std::string(buf, 2));

  // No permission to access data directory
  chmod(data_path_, 0);
  op_context_.inodes_changed.clear();

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EACCES, store_->GetXattr(
      &op_context_, 3, "user.CPFS.ndsg", buf, 2));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
}

TEST_F(MSStoreTest, XAttrList) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  std::string fpath = InodeFilename(3);
  lsetxattr(fpath.c_str(), "user.CPFS.ndsg", "10", 2, 0);
  lsetxattr(fpath.c_str(), "user.abc", "hello world", 11, 0);

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  char buf[256];
  EXPECT_EQ(24, store_->ListXattr(&op_context_, 3, buf, 256));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));

  EXPECT_EQ(std::string("user.CPFS.ndsg\0user.abc", 24), std::string(buf, 24));

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(24, store_->ListXattr(&op_context_, 3, buf, 0));

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-ERANGE, store_->ListXattr(&op_context_, 3, buf, 23));

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-ENOENT, store_->ListXattr(&op_context_, 2, buf, 256));
}

TEST_F(MSStoreTest, XAttrRemove) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  std::string fpath = InodeFilename(3);
  lsetxattr(fpath.c_str(), "user.CPFS.ndsg", "10", 2, 0);
  lsetxattr(fpath.c_str(), "user.abc", "hello world", 11, 0);
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(0, store_->RemoveXattr(&op_context_, 3, "user.abc"));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));
  EXPECT_NE(-1, lgetxattr(fpath.c_str(), "user.CPFS.ndsg", 0, 0));
  EXPECT_EQ(-1, lgetxattr(fpath.c_str(), "user.abc", 0, 0));

  // No permission to access data directory
  chmod(data_path_, 0);
  op_context_.inodes_changed.clear();
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EACCES,
      store_->RemoveXattr(&op_context_, 3, "user.abc"));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
}

TEST_F(MSStoreTest, Open) {
  store_->Initialize();
  std::vector<GroupId> groups;

  // No such file
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  bool truncated;
  EXPECT_EQ(-EIO, store_->Open(&op_context_, 3, O_RDWR, &groups, &truncated));
  VerifyMocks();
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // File is there, but is not permitted to be opened
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  CreateFile(1, 3, "hello");
  std::string fpath = InodeFilename(3);
  chmod(fpath.c_str(), 0);
  EXPECT_EQ(-EACCES, store_->Open(&op_context_, 3, O_RDWR, &groups,
                                  &truncated));

  // Normal case: File with extended attributes
  std::string gstr = "4,10";
  lsetxattr((fpath + "x").c_str(), "user.sg", gstr.data(), gstr.size(), 0);
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  chmod(fpath.c_str(), 0664);
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Open(&op_context_, 3, O_RDWR | O_TRUNC, &groups,
                            &truncated));
  VerifyMocks();
  EXPECT_FALSE(truncated);
  ASSERT_EQ(2U, groups.size());
  EXPECT_EQ(4U, groups[0]);
  EXPECT_EQ(10U, groups[1]);
  struct stat stbuf = GetStat(3);
  EXPECT_EQ(1234567890U, stbuf.st_mtime);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));

  // Normal case: Open previous file with read only
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Open(&op_context_, 3, O_RDONLY, &groups, &truncated));
  VerifyMocks();
  EXPECT_FALSE(truncated);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3));

  // Normal case: Truncation
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  truncate(fpath.c_str(), 1);
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Open(&op_context_, 3, O_RDWR | O_TRUNC, &groups,
                            &truncated));
  VerifyMocks();
  EXPECT_TRUE(truncated);

  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());

  // Incorrect extended attributes
  lsetxattr((fpath + "x").c_str(), "user.sg", "-2", 1, 0);
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EIO, store_->Open(&op_context_, 3, O_RDWR, &groups, &truncated));
  VerifyMocks();

  // File error: unwritable control file
  lsetxattr((fpath + "x").c_str(), "user.sg", "2", 1, 0);
  chmod((fpath + "x").c_str(), 0444);
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(callback, Call(PLEVEL(warning, Store),
                             StartsWith("SetFileTimes: lsetxattr on "), _));
  EXPECT_CALL(*durable_range_, Add(3, kIMissing, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Open(&op_context_, 3, O_RDWR | O_TRUNC, &groups,
                            &truncated));
  VerifyMocks();

  // File error: no extended attributes
  chmod((fpath + "x").c_str(), 0644);
  lremovexattr((fpath + "x").c_str(), "user.sg");
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EIO, store_->Open(&op_context_, 3, O_RDWR, &groups, &truncated));
  VerifyMocks();
}

TEST_F(MSStoreTest, Access) {
  store_->Initialize();
  CreateFile(1, 4, "hello");
  chmod(InodeFilename(4).c_str(), 0640);

  // Other's access
  EXPECT_EQ(-ENOENT, store_->Access(&op_context_, 3, W_OK));
  EXPECT_EQ(0, store_->Access(&op_context_, 4, F_OK));

  EXPECT_CALL(*ugid_handler_, HasSupplementaryGroup(100, getgid()))
      .WillOnce(Return(false));

  EXPECT_EQ(-EACCES, store_->Access(&op_context_, 4, W_OK));

  EXPECT_CALL(*ugid_handler_, HasSupplementaryGroup(100, getgid()))
      .WillOnce(Return(false));

  EXPECT_EQ(-EACCES, store_->Access(&op_context_, 4, R_OK));

  // Group access
  req_context_.gid = getgid();
  EXPECT_EQ(-EACCES, store_->Access(&op_context_, 4, W_OK));
  EXPECT_EQ(0, store_->Access(&op_context_, 4, R_OK));

  // Owner access
  req_context_.uid = getuid();
  EXPECT_EQ(0, store_->Access(&op_context_, 4, W_OK));
  EXPECT_EQ(0, store_->Access(&op_context_, 4, R_OK));

  // Root access
  req_context_.uid = 0;
  EXPECT_EQ(0, store_->Access(&op_context_, 4, W_OK));
  EXPECT_EQ(0, store_->Access(&op_context_, 4, R_OK));
}

TEST_F(MSStoreTest, AdviseWrite) {
  store_->Initialize();
  CreateFile(1, 4, "hello");
  CreateFile(1, 5, "hello2");
  chmod(InodeFilename(5).c_str(), 0);
  mkdir(InodeFilename(6).c_str(), 0777);

  EXPECT_EQ(-ENOENT, store_->AdviseWrite(&op_context_, 3, 1024));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  EXPECT_CALL(*durable_range_, Add(4, kIMissing, kIMissing, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->AdviseWrite(&op_context_, 4, 1024));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(4));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(4));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-EACCES, store_->AdviseWrite(&op_context_, 5, 1024));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(5));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-EIO, store_->AdviseWrite(&op_context_, 6, 1024));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(6));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
  struct stat stbuf = GetStat(4);
  EXPECT_EQ(1234567890U, stbuf.st_mtime);
}

TEST_F(MSStoreTest, Readlink) {
  store_->Initialize();
  std::vector<char> buf;

  // No entry
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-ENOENT, store_->Readlink(&op_context_, 3, &buf));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Create a symlink
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  FileAttr fa_ret;
  EXPECT_EQ(0, store_->Symlink(&op_context_, 1, "test", 3, "lnk", &fa_ret));
  EXPECT_EQ(3U, fa_ret.size);

  // Try again
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Readlink(&op_context_, 3, &buf));
  EXPECT_EQ(3U, buf.size());
  EXPECT_EQ(0, std::memcmp(buf.data(), "lnk", 3));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
}

TEST_F(MSStoreTest, Lookup) {
  // Normal file lookup
  store_->Initialize();
  CreateFile(1, 3, "hello");
  InodeNum inode_found;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  FileAttr fi;
  EXPECT_EQ(0, store_->Lookup(&op_context_, 1, "hello", &inode_found, &fi));
  VerifyMocks();
  EXPECT_EQ(3U, inode_found);
  EXPECT_TRUE(S_ISREG(fi.mode));
  EXPECT_EQ(1234567800U, fi.ctime.sec);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Normal directory lookup
  CreateDirectory(1, 4, "testd");
  InodeNum inode_dir_found;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  FileAttr fi_dir;
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Lookup(&op_context_, 1, "testd",
                              &inode_dir_found, &fi_dir));
  VerifyMocks();
  EXPECT_EQ(4U, inode_dir_found);
  EXPECT_TRUE(S_ISDIR(fi_dir.mode));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Failure: non-existing
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-ENOENT,
            store_->Lookup(&op_context_, 1, "hello2", &inode_found, &fi));
  VerifyMocks();
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
}

TEST_F(MSStoreTest, LookupStrange) {
  store_->Initialize();
  CreateFile(1, 3, "hello");
  InodeNum inode_found;
  FileAttr fi;
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());

  // Failure: can't parse ctime
  setxattr((InodeFilename(3) + "x").c_str(), "user.ct", "a", 1, 0);
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(callback, Call(PLEVEL(error, Store),
                             StartsWith("Cannot parse ctime"), _));

  EXPECT_EQ(-EIO, store_->Lookup(&op_context_, 1, "hello",
                                 &inode_found, &fi));
  VerifyMocks();

  // Failure: no ctime
  removexattr((InodeFilename(3) + "x").c_str(), "user.ct");
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(callback, Call(PLEVEL(error, Store),
                             StartsWith("Cannot get ctime"), _));

  EXPECT_EQ(-EIO, store_->Lookup(&op_context_, 1, "hello",
                                 &inode_found, &fi));
  VerifyMocks();

  // Failure: non-existing inode file
  unlink(InodeFilename(3).c_str());
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-ENOENT,
            store_->Lookup(&op_context_, 1, "hello", &inode_found, &fi));
  VerifyMocks();
}

TEST_F(MSStoreTest, Opendir) {
  store_->Initialize();
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  CreateDirectory(1, 3, "test");
  EXPECT_EQ(0, store_->Opendir(&op_context_, 3));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Error case: no permission
  chmod(InodeFilename(3).c_str(), 0355);
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-EACCES, store_->Opendir(&op_context_, 3));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Error case: no such directory
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_EQ(-ENOENT, store_->Opendir(&op_context_, 100));
}

TEST_F(MSStoreTest, Readdir) {
  // Not a directory
  store_->Initialize();
  CreateFile(1, 3, "hello");
  CreateDirectory(1, 4, "hello2");
  std::vector<char> buf(0);
  EXPECT_EQ(-ENOTDIR, store_->Readdir(&op_context_, 3, 0, &buf));

  // Too little space
  EXPECT_EQ(-EINVAL, store_->Readdir(&op_context_, 1, 0, &buf));

  // Sufficient space for only one entry
  buf.resize(32);
  int ret1 = store_->Readdir(&op_context_, 1, 0, &buf);
  EXPECT_GT(ret1, 0);
  ReaddirRecord& rec1 = reinterpret_cast<ReaddirRecord&>(buf[0]);
  std::string rec1name(rec1.name);
  int inode1 = rec1.inode;
  EXPECT_EQ(true, inode1 == 1 || inode1 == 3 || inode1 == 4);

  // Can get another entry
  int ret2 = store_->Readdir(&op_context_, 1, rec1.cookie, &buf);
  EXPECT_GT(ret2, 0);
  ReaddirRecord& rec2 = reinterpret_cast<ReaddirRecord&>(buf[0]);
  int inode2 = rec2.inode;
  EXPECT_EQ(true, inode2 == 1 || inode2 == 3 || inode2 == 4);
  EXPECT_NE(rec1name, std::string(rec2.name));

  // Sufficient space for all entries
  buf.resize(256);
  int ret3 = store_->Readdir(&op_context_, 1, 0, &buf);
  ASSERT_EQ(ReaddirRecord::GetLenForName("hello")
            + ReaddirRecord::GetLenForName("hello2")
            + ReaddirRecord::GetLenForName(".")
            + ReaddirRecord::GetLenForName(".."), ret3);
  // Check all records
  bool matched[4] = { false, false, false, false };
  const char* curr = &buf[0];
  while (curr - &buf[0] < ret3) {
    const ReaddirRecord* rec = reinterpret_cast<const ReaddirRecord*>(curr);
    curr += rec->GetLen();
    if (std::strcmp(rec->name, ".") == 0 && rec->inode == 1)
      matched[0] = true;
    else if (std::strcmp(rec->name, "..") == 0 && rec->inode == 1)
      matched[1] = true;
    else if (std::strcmp(rec->name, "hello") == 0 && rec->inode == 3)
      matched[2] = true;
    else if (std::strcmp(rec->name, "hello2") == 0 && rec->inode == 4)
      matched[3] = true;
    else
      ADD_FAILURE() << std::string("Unmatched: ") << rec->name;
  }
  EXPECT_TRUE(matched[0] && matched[1] && matched[2] && matched[3]);
  // Exotic case: inode can't be read
  lremovexattr(EntryFilename(1, "hello2").c_str(), "user.ino");
  EXPECT_EQ(-EIO, store_->Readdir(&op_context_, 1, 0, &buf));
}

TEST_F(MSStoreTest, ReaddirUnknownDT) {
  store_->Initialize();
  std::vector<char> buf(256);
  // On XFS, DT_xxx attributes are not supported
  ASSERT_EQ(0, mknod(EntryFilename(1, "fifo").c_str(), S_IFIFO | 0666, 0));
  int ret1 = store_->Readdir(&op_context_, 1, 0, &buf);
  EXPECT_EQ(-EIO, ret1);
}

TEST_F(MSStoreTest, Create) {
  store_->Initialize();
  FileAttr fi;
  CreateReq create_req = {3, 0, 0644};
  std::vector<GroupId> groups;
  groups.push_back(3);
  boost::scoped_ptr<RemovedInodeInfo> removed_info;

  // Normal create
  std::memset(&fi, 0, sizeof(fi));
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillRepeatedly(Return(groups));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Create(&op_context_, 1, "hello", &create_req,
                              &groups, &fi, &removed_info));
  VerifyMocks();
  EXPECT_EQ(1U, fi.nlink);
  EXPECT_EQ(0U, fi.size);
  EXPECT_EQ(0U, fi.mode & ~0644 & 07777);
  EXPECT_EQ(1234567890U, fi.atime.sec);
  EXPECT_EQ(1234567890U, fi.mtime.sec);
  EXPECT_EQ(1234567890U, fi.ctime.sec);
  EXPECT_EQ(1U, groups.size());
  EXPECT_EQ(3U, groups[0]);
  EXPECT_TRUE(S_ISREG(fi.mode));
  struct stat stbuf = GetStat(3);
  EXPECT_EQ(1U, stbuf.st_nlink);
  EXPECT_EQ(0U, stbuf.st_size);
  EXPECT_EQ(0U, stbuf.st_mode & ~0644 & 07777);
  EXPECT_EQ(1234567890U, stbuf.st_atime);
  EXPECT_EQ(1234567890U, stbuf.st_mtime);
  EXPECT_TRUE(S_ISREG(stbuf.st_mode));
  stbuf = GetStat(1);
  EXPECT_EQ(1234567890U, stbuf.st_mtime);
  EXPECT_FALSE(removed_info);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 3));

  // Normal create, O_EXCL
  create_req.new_inode = 4;
  create_req.flags = O_EXCL;
  EXPECT_CALL(*inode_src_, NotifyUsed(4));
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillRepeatedly(Return(groups));
  EXPECT_CALL(*durable_range_, Add(1, 4, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Create(&op_context_, 1, "hello2", &create_req,
                              &groups, &fi, &removed_info));
  VerifyMocks();
  GetStat(4);
  EXPECT_FALSE(removed_info);

  // Failing create, existing with O_EXCL
  create_req.new_inode = 5;
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillRepeatedly(Return(groups));
  EXPECT_CALL(*durable_range_, Add(1, 5, kIMissing, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-EEXIST, store_->Create(&op_context_, 1, "hello", &create_req,
                                    &groups, &fi, &removed_info));
  VerifyMocks();
  EXPECT_THROW(GetStat(5), std::runtime_error);
  EXPECT_FALSE(removed_info);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Failing create, inode number used
  create_req.new_inode = 3;

  EXPECT_EQ(-EEXIST, store_->Create(&op_context_, 1, "hello3", &create_req,
                                    &groups, &fi, &removed_info));
  EXPECT_FALSE(removed_info);

  // Normal Unlinking create
  create_req.new_inode = 5;
  create_req.flags = 0;
  EXPECT_CALL(*inode_src_, NotifyUsed(5));
  EXPECT_CALL(*inode_src_, NotifyRemoved(4));
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(3).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_usage_, IsOpened(4, false)).WillOnce(Return(false));
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillRepeatedly(Return(groups));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(4));
  EXPECT_CALL(*durable_range_, Add(1, 5, kIMissing, kIMissing));
  EXPECT_CALL(*durable_range_, Add(1, 4, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Create(&op_context_, 1, "hello2", &create_req,
                              &groups, &fi, &removed_info));
  VerifyMocks();
  GetStat(5);
  EXPECT_EQ(4U, removed_info->inode);
  EXPECT_TRUE(removed_info->to_free);
  EXPECT_EQ(1U, removed_info->groups->size());
  EXPECT_EQ(3U, (*removed_info->groups)[0]);
}

TEST_F(MSStoreTest, CreateAllocateDSG) {
  store_->Initialize();
  FileAttr fi;
  CreateReq create_req = {3, 0, 0644};
  std::vector<GroupId> ret_groups, groups;
  for (int i = 0; i < 5; ++i)
    ret_groups.push_back(i);
  boost::scoped_ptr<RemovedInodeInfo> removed_info;

  // Allocate five DSG groups for the file
  // The # of groups is intentionally not null terminated
  lsetxattr(InodeFilename(1).c_str(), "user.ndsg", "5", 1, 0);

  // Normal create
  std::memset(&fi, 0, sizeof(fi));
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*dsg_allocator_, Allocate(5))
      .WillRepeatedly(Return(ret_groups));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Create(&op_context_, 1, "hello", &create_req,
                              &groups, &fi, &removed_info));
  VerifyMocks();
}

TEST_F(MSStoreTest, CreateStrange) {
  // Create unlink but fails
  store_->Initialize();
  FileAttr fi;
  CreateReq create_req = {3, 0, 0644};
  boost::scoped_ptr<RemovedInodeInfo> removed_info;

  // Test multi-group creation
  std::vector<GroupId> groups, ret_groups;
  ret_groups.push_back(2);
  ret_groups.push_back(0);
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillOnce(Return(ret_groups));
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Create(&op_context_, 1, "hello", &create_req,
                              &groups, &fi, &removed_info));
  VerifyMocks();
  char buf[1024];
  errno = 0;
  int len = getxattr((InodeFilename(3) + "x").c_str(), "user.sg", buf, 1024);
  EXPECT_EQ(0, errno);
  EXPECT_EQ(std::string("2,0"), std::string(buf, len));
  EXPECT_FALSE(removed_info);

  // Test creation fails because of DSG disk full
  std::vector<GroupId> empty_groups;
  create_req.new_inode = 4;
  EXPECT_CALL(*dsg_allocator_, Allocate(_))
      .WillOnce(Return(empty_groups));

  EXPECT_EQ(-ENOSPC, store_->Create(&op_context_, 1, "full", &create_req,
                                    &empty_groups, &fi, 0));
  VerifyMocks();
  EXPECT_THROW(GetStat(4), std::runtime_error);

  // Test unlink fails during create
  chmod(InodeFilename(1).c_str(), 0555);
  create_req.new_inode = 5;
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillOnce(Return(ret_groups));
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 5, kIMissing, kIMissing));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  groups.resize(0);
  EXPECT_EQ(-EACCES, store_->Create(&op_context_, 1, "hello", &create_req,
                                    &groups, &fi, 0));
  VerifyMocks();
}

TEST_F(MSStoreTest, Mkdir) {
  store_->Initialize();
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  CreateDirReq create_req = {3, 0744};
  FileAttr fa_ret;
  EXPECT_EQ(0, store_->Mkdir(&op_context_, 1, "testd1", &create_req,
                             &fa_ret));
  VerifyMocks();
  // Test inode
  struct stat stbuf = GetStat(3);
  EXPECT_EQ(2U, stbuf.st_nlink);
  EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
  // Test FileAttr
  struct stat fa_stbuf;
  fa_ret.ToStat(3, &fa_stbuf);
  EXPECT_EQ(2U, fa_stbuf.st_nlink);
  EXPECT_TRUE(S_ISDIR(fa_stbuf.st_mode));
  // Test dentry
  std::string dentry_path = EntryFilename(1, "testd1");
  struct stat dentry_stbuf;
  stat(dentry_path.c_str(), &dentry_stbuf);
  EXPECT_EQ(2U, dentry_stbuf.st_nlink);
  EXPECT_TRUE(S_ISDIR(dentry_stbuf.st_mode));
  // Test xattr
  char buf[32] = {'\0'};
  errno = 0;
  int len = getxattr(dentry_path.c_str(), "user.ino", buf, 32);
  EXPECT_EQ(0, errno);
  EXPECT_EQ(std::string("0000000000000003"), std::string(buf, len));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 3));

  // Failure: inode directory already exists
  EXPECT_EQ(-EEXIST,
            store_->Mkdir(&op_context_, 1, "testd1", &create_req, &fa_ret));

  // Failure: new inode but dentry already exists
  CreateDirReq create_req2 = {4, 0744};
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 4, kIMissing, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-EEXIST,
            store_->Mkdir(&op_context_, 1, "testd1", &create_req2, &fa_ret));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
  VerifyMocks();
  EXPECT_THROW(GetStat(4), std::runtime_error);

  // Failure: permission denied
  // Make the /1 directory non writable
  chmod(InodeFilename(1).c_str(), 0555);
  CreateDirReq create_req3 = {5, 0744};
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 5, kIMissing, kIMissing));

  EXPECT_EQ(-EACCES,
            store_->Mkdir(&op_context_, 1, "testd2", &create_req3, &fa_ret));
  VerifyMocks();
}

TEST_F(MSStoreTest, MkdirInheritDSG) {
  store_->Initialize();

  // The # of groups is intentionally not null terminated
  lsetxattr(InodeFilename(1).c_str(), "user.ndsg", "5", 1, 0);

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  CreateDirReq create_req = {3, 0744};
  FileAttr fa_ret;
  EXPECT_EQ(0, store_->Mkdir(&op_context_, 1, "testd1", &create_req,
                             &fa_ret));
  VerifyMocks();

  // Check that the value is inherited from root
  char buf[4] = {'\0'};
  lgetxattr(InodeFilename(3).c_str(), "user.ndsg", buf, 4);
  EXPECT_EQ("5", std::string(buf));
}

TEST_F(MSStoreTest, Symlink) {
  store_->Initialize();

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  FileAttr fa_ret;
  EXPECT_EQ(0, store_->Symlink(&op_context_, 1, "testd1", 3, "lnk",
                               &fa_ret));
  EXPECT_EQ(3U, fa_ret.size);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 3));

  // Error case: no parent
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(2, 4, kIMissing, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-ENOENT,
            store_->Symlink(&op_context_, 2, "testd1", 4, "lnk", &fa_ret));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(2));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());
  EXPECT_THROW(GetStat(4), std::runtime_error);

  // Unexpected error case: inode reused
  EXPECT_EQ(-EEXIST,
            store_->Symlink(&op_context_, 1, "testd1", 3, "lnk", &fa_ret));
}

TEST_F(MSStoreTest, Mknod) {
  store_->Initialize();
  std::vector<GroupId> groups, groups_ret;
  groups_ret.push_back(3);

  // Successful case
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  FileAttr fa_ret;
  EXPECT_EQ(0, store_->Mknod(&op_context_, 1, "tests", 3, S_IFSOCK | 0777, 0,
                             &groups, &fa_ret));
  EXPECT_TRUE(S_ISSOCK(fa_ret.mode));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 3));

  // Case for regular file creation
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(4));
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillRepeatedly(Return(groups_ret));
  EXPECT_CALL(*durable_range_, Add(1, 4, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Mknod(&op_context_, 1, "testf", 4, S_IFREG | 0777, 0,
                             &groups, &fa_ret));
  EXPECT_TRUE(S_ISREG(fa_ret.mode));

  // Error case: make device
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-EPERM, store_->Mknod(&op_context_, 1, "teste1", 5,
                                  S_IFCHR | 0777, 0, &groups, &fa_ret));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre());
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Error case: no parent
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(2, 5, kIMissing, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-ENOENT,
            store_->Mknod(&op_context_, 2, "teste2", 5, S_IFSOCK | 0777, 0,
                          &groups, &fa_ret));
  EXPECT_THROW(GetStat(5), std::runtime_error);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(2));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Error case: DS group disk full
  std::vector<GroupId> empty_group;
  EXPECT_CALL(*dsg_allocator_, Allocate(1))
      .WillRepeatedly(Return(empty_group));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(-ENOSPC,
            store_->Mknod(&op_context_, 1, "teste3", 5, S_IFREG | 0777, 0,
                          &empty_group, &fa_ret));

  // Unexpected error case: inode reused
  EXPECT_EQ(-EEXIST,
            store_->Mknod(&op_context_, 1, "teste4", 3, S_IFSOCK | 0777, 0,
                          &groups, &fa_ret));
}

TEST_F(MSStoreTest, Link) {
  store_->Initialize();
  FileAttr fi;

  // Error case: target doesn't exist
  EXPECT_EQ(-ENOENT, store_->Link(&op_context_, 3, 1, "hello", &fi));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3, 1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Error case: no parent
  CreateFile(1, 3, "foo");
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-ENOENT, store_->Link(&op_context_, 3, 2, "hello", &fi));

  // Error case: file already exists
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EEXIST, store_->Link(&op_context_, 3, 1, "foo", &fi));

  // Expected case: link successful
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Link(&op_context_, 3, 1, "hello", &fi));
  EXPECT_EQ(2U, fi.nlink);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(3, 1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(3, 1));
}

TEST_F(MSStoreTest, RenameFile) {
  store_->Initialize();
  InodeNum moved;
  boost::scoped_ptr<RemovedInodeInfo> removed_info;

  // Non-existent file
  EXPECT_EQ(-ENOENT, store_->Rename(&op_context_, 1, "hello", 2, "foo",
                                    &moved, &removed_info));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1, 2));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre());

  // Normal file rename
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 1, 1, kIMissing));

  CreateFile(1, 3, "hello");
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Rename(&op_context_, 1, "hello", 1, "foo",
                              &moved, &removed_info));
  EXPECT_EQ(3U, moved);
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1, 1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 1));

  EXPECT_EQ(-ENOENT, store_->Rename(&op_context_, 1, "hello", 1, "foo",
                                    &moved, &removed_info));

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 1, 1, kIMissing));

  EXPECT_EQ(0, store_->Rename(&op_context_, 1, "foo", 1, "hello",
                              &moved, &removed_info));
  EXPECT_EQ(3U, moved);
  EXPECT_EQ(-ENOENT, store_->Rename(&op_context_, 1, "foo", 1, "hello",
                                    &moved, &removed_info));

  // Unlinking file rename
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_usage_, IsOpened(4, false));
  EXPECT_CALL(*inode_src_, NotifyRemoved(4));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(4));
  EXPECT_CALL(*durable_range_, Add(1, 1, 4, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  CreateFile(1, 4, "foo");
  EXPECT_EQ(0, store_->Rename(&op_context_, 1, "hello", 1, "foo",
                              &moved, &removed_info));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1, 1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 1, 4));

  // No permission to read target directory
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 6, 1, kIMissing));

  mkdir(InodeFilename(6).c_str(), 0);
  EXPECT_EQ(-EACCES, store_->Rename(&op_context_, 1, "foo", 6, "foo",
                                    &moved, &removed_info));

  // No permission to write target directory
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 1, 1, kIMissing));

  chmod(InodeFilename(1).c_str(), 0555);
  EXPECT_EQ(-EACCES, store_->Rename(&op_context_, 1, "foo", 1, "hello",
                                    &moved, &removed_info));
}

TEST_F(MSStoreTest, RenameDir) {
  store_->Initialize();
  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  InodeNum moved;

  // Fixtures
  CreateDirectory(1, 3, "foo");
  CreateDirectory(1, 4, "bar");
  CreateFile(3, 5, "hello");

  // Directory to file rename
  EXPECT_CALL(*durable_range_, Add(1, 3, 5, kIMissing));

  EXPECT_EQ(-ENOTDIR, store_->Rename(&op_context_, 1, "bar", 3, "hello",
                                     &moved, &removed_info));

  // Non-empty directory rename
  EXPECT_CALL(*durable_range_, Add(1, 1, 3, kIMissing));

  EXPECT_EQ(-ENOTEMPTY, store_->Rename(&op_context_, 1, "bar", 1, "foo",
                                       &moved, &removed_info));

  // Successful directory rename
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyRemoved(4));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(4));
  EXPECT_CALL(*durable_range_, Add(1, 1, 4, kIMissing));

  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Rename(&op_context_, 1, "foo", 1, "bar",
                              &moved, &removed_info));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1, 1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 1, 4));
}

TEST_F(MSStoreTest, UnlinkSticky) {
  store_->Initialize();
  chmod(InodeFilename(1).c_str(), 01755);
  CreateFile(1, 3, "hello");

  // Successful unlink
  EXPECT_CALL(*inode_src_, NotifyRemoved(3));
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_usage_, IsOpened(3, false)).WillOnce(Return(false));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  EXPECT_EQ(0, store_->Unlink(&op_context_, 1, "hello", &removed_info));
  VerifyMocks();
  EXPECT_EQ(3U, removed_info->inode);
  EXPECT_TRUE(removed_info->to_free);
  EXPECT_EQ(1U, removed_info->groups->size());
  EXPECT_EQ(1U, (*removed_info->groups)[0]);
  EXPECT_THROW(GetStat(3), std::runtime_error);
  struct stat stbuf = GetStat(1);
  EXPECT_EQ(1234567890U, stbuf.st_mtime);

  // Failure: non-existing
  EXPECT_EQ(-ENOENT, store_->Unlink(&op_context_, 1, "hello", 0));
}

TEST_F(MSStoreTest, UnlinkHardLink) {
  // Fixture
  store_->Initialize();
  FileAttr fi;
  CreateFile(1, 3, "foo");
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Link(&op_context_, 3, 1, "hello", &fi));
  EXPECT_EQ(2U, fi.nlink);

  // First unlink
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  EXPECT_EQ(0, store_->Unlink(&op_context_, 1, "hello", &removed_info));
  EXPECT_TRUE(removed_info);
  EXPECT_EQ(3U, removed_info->inode);
  EXPECT_FALSE(removed_info->to_free);
  GetStat(3);

  // Second unlink
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_usage_, IsOpened(3, false)).WillOnce(Return(false));
  EXPECT_CALL(*inode_src_, NotifyRemoved(3));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  EXPECT_EQ(0, store_->Unlink(&op_context_, 1, "foo", &removed_info));
  EXPECT_TRUE(removed_info);
  EXPECT_TRUE(removed_info->to_free);
  EXPECT_THROW(GetStat(3), std::runtime_error);
}

TEST_F(MSStoreTest, UnlinkOpened) {
  store_->Initialize();
  CreateFile(1, 3, "hello");

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_usage_, IsOpened(3, false)).WillOnce(Return(true));
  EXPECT_CALL(*inode_usage_, AddPendingUnlink(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  EXPECT_EQ(0, store_->Unlink(&op_context_, 1, "hello", &removed_info));
  EXPECT_TRUE(removed_info);
  EXPECT_EQ(3U, removed_info->inode);
  EXPECT_FALSE(removed_info->to_free);
  VerifyMocks();
  GetStat(3);
  struct stat stbuf = GetStat(1);
  EXPECT_EQ(1234567890U, stbuf.st_mtime);
}

TEST_F(MSStoreTest, UnlinkStrange) {
  store_->Initialize();
  std::string entry_path = EntryFilename(1, "a");
  boost::scoped_ptr<RemovedInodeInfo> removed_info;

  // Inode doesn't exist
  {
    MockLogCallback callback;
    LogRoute route(callback.GetLogCallback());
    symlink("0000000000000010", entry_path.c_str());
    EXPECT_CALL(callback, Call(PLEVEL(warning, Store),
                               StartsWith("Cannot remove "), _));
    EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
        .WillOnce(ReturnNew<MockIUgidSetterGuard>());
    EXPECT_CALL(*inode_usage_, IsOpened(0x10, false)).WillOnce(Return(false));
    EXPECT_CALL(*durable_range_, Add(1, 16, kIMissing, kIMissing));

    EXPECT_EQ(0, store_->Unlink(&op_context_, 1, "a", &removed_info));
    VerifyMocks();
    EXPECT_EQ(0x10U, removed_info->inode);
    EXPECT_EQ(0U, removed_info->groups->size());
  }

  // Is directory
  CreateDirectory(1, 3, "a");
  EXPECT_EQ(-EISDIR, store_->Unlink(&op_context_, 1, "a", 0));
  VerifyMocks();

  // No permission
  CreateFile(1, 4, "b");
  chmod(InodeFilename(1).c_str(), 0555);
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 4, kIMissing, kIMissing));

  EXPECT_EQ(-EACCES, store_->Unlink(&op_context_, 1, "b", 0));
  VerifyMocks();
}

TEST_F(MSStoreTest, Rmdir) {
  store_->Initialize();

  // Non-existent file
  EXPECT_EQ(-ENOENT, store_->Rmdir(&op_context_, 1, "hello"));

  // Not a directory
  CreateFile(1, 2, "foo");
  EXPECT_EQ(-ENOTDIR, store_->Rmdir(&op_context_, 1, "foo"));

  // Normal
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyRemoved(3));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(3));
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  CreateDirectory(1, 3, "hello");
  op_context_.inodes_read.clear();
  op_context_.inodes_changed.clear();
  EXPECT_EQ(0, store_->Rmdir(&op_context_, 1, "hello"));
  EXPECT_THAT(op_context_.inodes_read, ElementsAre(1));
  EXPECT_THAT(op_context_.inodes_changed, ElementsAre(1, 3));

  // Non-empty
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 3, kIMissing, kIMissing));

  CreateDirectory(1, 3, "hello");
  CreateFile(3, 4, "foo");
  EXPECT_EQ(-ENOTEMPTY, store_->Rmdir(&op_context_, 1, "hello"));

  // No permission
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillOnce(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*durable_range_, Add(1, 5, kIMissing, kIMissing));

  CreateDirectory(1, 5, "world");
  chmod(InodeFilename(1).c_str(), 0555);
  EXPECT_EQ(-EACCES, store_->Rmdir(&op_context_, 1, "world"));
}

TEST_F(MSStoreTest, ResyncInode) {
  store_->Initialize();
  {
    // ResyncInode() should remove existing inode
    CreateFile(1, 3, "inode3");
    // Regular file
    FileAttr fa;
    fa.uid = kTestUid;
    fa.gid = kTestGid;
    fa.mode = S_IFREG | 0705;
    fa.size = 8192;
    fa.atime.sec = 10;
    fa.atime.ns = 100;
    fa.mtime.sec = 20;
    fa.mtime.ns = 200;
    fa.ctime.sec = 40;
    fa.ctime.ns = 400;
    fa.nlink = 2;
    GroupId groups[] = {0, 1};
    EXPECT_EQ(
        0,
        store_->ResyncInode(
            InodeNum(3),
            fa,
            reinterpret_cast<char*>(&groups[0]),
            sizeof(GroupId) * 2, sizeof(GroupId) * 2));

    struct stat stbuf;
    lstat(InodeFilename(3).c_str(), &stbuf);
    EXPECT_EQ(8192U, stbuf.st_size);
    EXPECT_EQ(10U, stbuf.st_atim.tv_sec);
    // EXPECT_EQ(100U, stbuf.st_atim.tv_nsec);
    EXPECT_EQ(20U, stbuf.st_mtim.tv_sec);
    // EXPECT_EQ(200U, stbuf.st_mtim.tv_nsec);

    // Check server group
    char sg_buf[1024];
    int sg_len = lgetxattr((InodeFilename(3) + "x").c_str(),
                           "user.sg", sg_buf, sizeof(sg_buf));
    EXPECT_EQ("0,1", std::string(sg_buf, sg_len));

    // Check ctime
    char ct_buf[1024];
    int ct_len = lgetxattr((InodeFilename(3) + "x").c_str(),
                           "user.ct", ct_buf, sizeof(ct_buf));
    EXPECT_EQ("40-400", std::string(ct_buf, ct_len));

    // Check hardlinks
    struct stat stbuf_lnk;
    EXPECT_EQ(0, lstat((InodeFilename(3) + ".1").c_str(), &stbuf_lnk));
  }

  {
    // ResyncInode() should remove existing inode
    CreateDirectory(1, 4, "inode4");
    // Directory 1
    FileAttr fa;
    fa.uid = kTestUid;
    fa.gid = kTestGid;
    fa.mode = S_IFDIR | 0777;
    fa.atime.sec = 10;
    fa.atime.ns = 100;
    fa.mtime.sec = 20;
    fa.mtime.ns = 200;
    fa.ctime.sec = 40;
    fa.ctime.ns = 400;
    InodeNum parent = 3;
    XAttrList xattr_list;
    xattr_list.push_back(std::make_pair("ndsg", "3"));
    std::string xattr_buf = XAttrListToBuffer(xattr_list);
    // Tail buffer stores parent inode and ndsg xattr
    std::vector<char> buff(sizeof(InodeNum) + xattr_buf.size());
    std::memcpy(&buff[0], &parent, sizeof(parent));
    std::memcpy(&buff[sizeof(InodeNum)], xattr_buf.data(), xattr_buf.size());
    EXPECT_EQ(0, store_->ResyncInode(
        InodeNum(4), fa, buff.data(), sizeof(InodeNum), buff.size()));
    struct stat stbuf;
    lstat(InodeFilename(4).c_str(), &stbuf);
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
    char ndsg_buf[16];
    int ndsg_len;
    ndsg_len = getxattr(InodeFilename(4).c_str(), "user.ndsg", ndsg_buf, 16);
    EXPECT_EQ("3", std::string(ndsg_buf, ndsg_len));

    // Parent xattr
    char buf[1024];
    int len;
    len = getxattr((InodeFilename(4) + "x").c_str(), "user.par", buf, 1024);
    EXPECT_EQ(std::string("0000000000000003"), std::string(buf, len));
  }

  {
    // Directory 2
    FileAttr fa;
    fa.uid = kTestUid;
    fa.gid = kTestGid;
    fa.mode = S_IFDIR | 0777;
    InodeNum parent = 1;
    EXPECT_EQ(0, store_->ResyncInode(
        InodeNum(40), fa, reinterpret_cast<char*>(&parent), 0, 0));
    struct stat stbuf;
    lstat(InodeFilename(40).c_str(), &stbuf);
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));

    char ct_buf[1024];
    // Set time for the last directory 1 should be trigerred
    int ct_len = lgetxattr((InodeFilename(4) + "x").c_str(),
                           "user.ct", ct_buf, sizeof(ct_buf));
    lstat(InodeFilename(4).c_str(), &stbuf);
    EXPECT_EQ(10U, stbuf.st_atim.tv_sec);
    // EXPECT_EQ(100U, stbuf.st_atim.tv_nsec);
    EXPECT_EQ(20U, stbuf.st_mtim.tv_sec);
    // EXPECT_EQ(200U, stbuf.st_mtim.tv_nsec);
    EXPECT_EQ("40-400", std::string(ct_buf, ct_len));
  }

  {
    // Soft link
    std::string link_target = "../test";
    FileAttr fa;
    fa.uid = kTestUid;
    fa.gid = kTestGid;
    fa.mode = S_IFLNK | 0555;
    fa.nlink = 1;
    fa.atime.sec = 10;
    fa.atime.ns = 100;
    fa.mtime.sec = 20;
    fa.mtime.ns = 200;
    fa.ctime.sec = 40;
    fa.ctime.ns = 400;
    EXPECT_EQ(
        0,
        store_->ResyncInode(
            InodeNum(5), fa, link_target.c_str(),
            link_target.length(), link_target.length()));
    char link_buf[32] = {'\0'};
    readlink(InodeFilename(5).c_str(), &link_buf[0], 32);
    EXPECT_EQ(link_target, std::string(link_buf));
  }

  {
    // Special file
    FileAttr fa;
    fa.uid = kTestUid;
    fa.gid = kTestGid;
    fa.mode = S_IFIFO | 0666;
    fa.nlink = 1;
    fa.rdev = 0;
    fa.atime.sec = 10;
    fa.atime.ns = 100;
    fa.mtime.sec = 20;
    fa.mtime.ns = 200;
    EXPECT_EQ(0, store_->ResyncInode(InodeNum(6), fa, 0, 0, 0));
    struct stat stbuf;
    lstat(InodeFilename(6).c_str(), &stbuf);
    EXPECT_TRUE(S_ISFIFO(stbuf.st_mode));
    EXPECT_EQ(10U, stbuf.st_atim.tv_sec);
    // EXPECT_EQ(100U, stbuf.st_atim.tv_nsec);
    EXPECT_EQ(20U, stbuf.st_mtim.tv_sec);
    // EXPECT_EQ(200U, stbuf.st_mtim.tv_nsec);
  }
}

TEST_F(MSStoreTest, ResyncInodeError) {
  store_->Initialize();
  // No permission to access data directory
  chmod(data_path_, 0);

  {
    // Regular file
    FileAttr fa;
    fa.mode = S_IFREG | 0705;
    fa.size = 1024;
    GroupId groups[] = {0};
    EXPECT_EQ(
        -EACCES,
        store_->ResyncInode(
            InodeNum(3),
            fa,
            reinterpret_cast<char*>(&groups[0]),
            sizeof(GroupId), sizeof(GroupId)));
  }

  {
    // Directory
    FileAttr fa;
    fa.mode = S_IFDIR | 0777;
    EXPECT_EQ(-EACCES, store_->ResyncInode(InodeNum(4), fa, 0, 0, 0));
  }

  {
    // Soft link
    FileAttr fa;
    fa.mode = S_IFLNK | 0555;
    EXPECT_EQ(-EACCES, store_->ResyncInode(InodeNum(5), fa, "../test", 7, 7));
  }

  {
    // Special file
    FileAttr fa;
    fa.mode = S_IFIFO | 0666;
    fa.rdev = 0;
    EXPECT_EQ(-EACCES, store_->ResyncInode(InodeNum(6), fa, 0, 0, 0));
  }
}

TEST_F(MSStoreTest, ResyncDentry) {
  store_->Initialize();

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
    .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());

  // Test dentry to directory
  EXPECT_EQ(0, store_->ResyncDentry(
      100, 100, InodeNum(1), InodeNum(5), 'D', "test"));

  char buf[1024];
  int len;
  len = getxattr(
      EntryFilename(InodeNum(1), "test").c_str(), "user.ino", buf, 1024);
  EXPECT_EQ(std::string("0000000000000005"), std::string(buf, len));

  // Test dentry to inode
  EXPECT_EQ(0, store_->ResyncDentry(
      100, 100, InodeNum(1), InodeNum(6), 'S', "hello"));
  char sym_buf[32];
  len = readlink(EntryFilename(InodeNum(1), "hello").c_str(), &sym_buf[0], 32);
  EXPECT_EQ(std::string("0000000000000006"), std::string(sym_buf, len));
}

TEST_F(MSStoreTest, ResyncDentryError) {
  store_->Initialize();

  // No permission to access data directory
  chmod(data_path_, 0);

  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
    .Times(2).WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());

  EXPECT_EQ(-EACCES, store_->ResyncDentry(
      100, 100, InodeNum(1), InodeNum(5), 'D', "test"));

  EXPECT_EQ(-EACCES, store_->ResyncDentry(
      100, 100, InodeNum(1), InodeNum(6), 'S', "hello"));
}

TEST_F(MSStoreTest, ResyncRemoval) {
  store_->Initialize();
  CreateFile(1, 3, "testf");
  CreateDirectory(1, 4, "testd1");
  CreateFile(4, 6, "subf1");
  CreateDirectory(1, 5, "testd2");

  EXPECT_EQ(0, store_->ResyncRemoval(3));
  EXPECT_EQ(0, store_->ResyncRemoval(4));

  struct stat stbuf;
  EXPECT_NE(0, stat(InodeFilename(3).c_str(), &stbuf));
  EXPECT_NE(0, stat(InodeFilename(4).c_str(), &stbuf));

  // Error case
  EXPECT_EQ(-ENOENT, store_->ResyncRemoval(200));

  // Error case
  // No permission to write the data directory
  chmod(data_path_, 0444);
  EXPECT_EQ(-EACCES, store_->ResyncRemoval(5));
}

TEST_F(MSStoreTest, RemoveInodeSince) {
  store_->Initialize();
  CreateFile(1, 3, "testf");
  CreateDirectory(1, 4, "testd1");

  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*durable_range_, SetConservative(true));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(1));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(3));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(4));

  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_EQ(0, store_->RemoveInodeSince(ranges, 1));
  struct stat stbuf;
  EXPECT_NE(0, stat(InodeFilename(3).c_str(), &stbuf));
  EXPECT_NE(0, stat(InodeFilename(4).c_str(), &stbuf));
}

TEST_F(MSStoreTest, RemoveInodeSinceError) {
  store_->Initialize();
  CreateFile(1, 3, "testf");
  CreateDirectory(1, 4, "testd1");
  // No permission to write the data directory
  chmod((std::string(data_path_) + "/000").c_str(), 0544);
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(1));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(3));
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(4));
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*durable_range_, SetConservative(true));

  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_EQ(-EPERM, store_->RemoveInodeSince(ranges, 1));
}

TEST_F(MSStoreTest, Stat) {
  // Normal case
  uint64_t total_inodes;
  uint64_t free_inodes;
  EXPECT_EQ(0, store_->Stat(&total_inodes, &free_inodes));
  EXPECT_GT(total_inodes, 0U);
  EXPECT_GT(free_inodes, 0U);

  // Test failure
  data_path_mgr_.Deinit();
  EXPECT_NE(0, store_->Stat(&total_inodes, &free_inodes));
}

TEST_F(MSStoreTest, LoadPersistAllUUID) {
  EXPECT_EQ(0U, store_->LoadAllUUID().size());
  // Persist
  UUIDInfoMap uuids;
  uuids["MS1"] = "1204d54-abd4-431b-a332-eb55999960140";
  uuids["DS 0-1"] = "13a3dff-bce4-2adb-b2f2-1235513945120";
  uuids["DS 10-1"] = "bba2abc-a134-665b-24bd-fccccccef9e20";
  uuids["MS2"] = "cea89bc-a8c4-225b-34ab-ce416f4624340";
  store_->PersistAllUUID(uuids);
  // Load
  UUIDInfoMap saved = store_->LoadAllUUID();
  EXPECT_EQ(uuids, saved);
  // Persist Error
  data_path_mgr_.Deinit();
  EXPECT_THROW(store_->PersistAllUUID(uuids), std::runtime_error);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
