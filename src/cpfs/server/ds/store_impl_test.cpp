/* Copyright 2013 ClusterTech Ltd */

#include "server/ds/store_impl.hpp"

#include <fcntl.h>  // IWYU pragma: keep
#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/xattr.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fstream>  // IWYU pragma: keep
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "finfo.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "posix_fs_impl.hpp"
#include "posix_fs_mock.hpp"
#include "store_util.hpp"
#include "server/ds/store.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::IgnoreResult;
using ::testing::Invoke;
using ::testing::StartsWith;
using ::testing::StrEq;
using ::testing::Return;

namespace cpfs {
namespace server {
namespace ds {
namespace {

const char kDataPath[] = "/tmp/store_test_XXXXXX";

class DSStoreTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  boost::scoped_ptr<IPosixFS> posix_fs_;
  boost::scoped_ptr<MockIInodeRemovalTracker> inode_removal_tracker_;
  boost::scoped_ptr<MockIDurableRange> durable_range_;
  boost::scoped_ptr<IFileDataIterator> iter_;
  boost::scoped_ptr<IChecksumGroupIterator> cg_iter_;
  boost::scoped_ptr<IFileDataWriter> writer_;
  boost::scoped_ptr<IChecksumGroupWriter> cg_writer_;
  boost::scoped_ptr<IStore> store_;

  DSStoreTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        posix_fs_(MakePosixFS()),
        inode_removal_tracker_(new MockIInodeRemovalTracker),
        durable_range_(new MockIDurableRange),
        store_(MakeStore(data_path_)) {
    store_->SetPosixFS(posix_fs_.get());
    store_->SetInodeRemovalTracker(inode_removal_tracker_.get());
    store_->SetDurableRange(durable_range_.get());
  }

  // Version for iterators and writers
  void WriteData(const char* fn, std::size_t off, const void* data,
                 std::size_t size) {
    std::string path = GetPath(fn);
    int fd = open(path.c_str(), O_RDWR | O_CREAT, 0666);
    EXPECT_EQ(int(size), pwrite(fd, data, size, off));
    close(fd);
    char buf[128];
    FSTime op_time = {0, 0};
    int len = GetFileTimeStr(buf, op_time);
    EXPECT_GE(0, posix_fs_->Lsetxattr(path.c_str(), "user.mt", buf, len, 0));
    std::string str = GetInodeStr(size + off);
    EXPECT_GE(0, posix_fs_->Lsetxattr(path.c_str(), "user.fs", str.data(),
                                      str.size(), 0));
  }

  // Version for store
  void WriteData(InodeNum inode, std::size_t off, const void* data,
                 std::size_t size) {
    EXPECT_CALL(*durable_range_, Add(inode, _, _, _));

    FSTime op_time = {0, 0};
    char dummy[kSegmentSize];
    EXPECT_EQ(int64_t(size),
              store_->Write(inode, op_time, off + size, off, data, size,
                            dummy));
  }

  std::string GetRoot(const std::string& parent = "000") {
    return std::string(data_path_) + "/" + parent;
  }

  // Version for iterators and writers
  std::string GetPath(const char* fn) {
    return GetRoot() + "/" + fn;
  }

  // Version for store
  std::string GetPath(InodeNum inode, bool cs = false) {
    return GetRoot() + "/" + GetInodeStr(inode)
        + (cs ? ".c" : ".d");
  }

  void SetDataIterator(const char* fn) {
    iter_.reset(MakeFileDataIterator(GetPath(fn)));
  }

  void SetDataWriter(const char* fn) {
    writer_.reset(MakeFileDataWriter(GetPath(fn)));
  }

  void SetChecksumIterator(const char* fn, InodeNum inode, GroupRole role) {
    cg_iter_.reset(MakeChecksumGroupIterator(GetPath(fn), inode, role));
  }

  void SetChecksumWriter(const char* fn, InodeNum inode, GroupRole role) {
    cg_writer_.reset(MakeChecksumGroupWriter(GetPath(fn), inode, role));
  }
};

TEST_F(DSStoreTest, FileDataIterator) {
  // File not found case
  SetDataIterator("2");
  std::size_t off;
  char buf[kSegmentSize];
  EXPECT_EQ(0U, iter_->GetNext(&off, buf));

  // Test with small file
  WriteData("2", 64, "hello", 5);
  SetDataIterator("2");
  EXPECT_EQ(69U, iter_->GetNext(&off, buf));
  EXPECT_EQ(0U, off);
  EXPECT_EQ(0, std::memcmp(buf + 64, "hello", 5));
  EXPECT_EQ(0U, iter_->GetNext(&off, buf));

  // Test getting info
  FSTime mtime;
  uint64_t size;
  iter_->GetInfo(&mtime, &size);
  EXPECT_EQ(0U, mtime.sec);
  EXPECT_EQ(0U, mtime.ns);
  EXPECT_EQ(69U, size);

  // Test with large file
  WriteData("2", kSegmentSize, "world", 5);
  SetDataIterator("2");
  EXPECT_EQ(kSegmentSize, iter_->GetNext(&off, buf));
  EXPECT_EQ(0U, off);
  EXPECT_EQ(0, std::memcmp(buf + 64, "hello", 5));
  EXPECT_EQ(5U, iter_->GetNext(&off, buf));
  EXPECT_EQ(kSegmentSize, off);
  EXPECT_EQ(0, std::memcmp(buf, "world", 5));
  EXPECT_EQ(0U, iter_->GetNext(&off, buf));

  // Test with file hole
  unlink(GetPath("2").c_str());
  WriteData("2", kSegmentSize, "world", 6);
  WriteData("2", kSegmentSize * 1024, "\0\0", 3);
  WriteData("2", kSegmentSize * 2048, "foo", 4);
  SetDataIterator("2");
  EXPECT_EQ(kSegmentSize, iter_->GetNext(&off, buf));
  EXPECT_EQ(kSegmentSize, off);
  EXPECT_EQ(0, std::memcmp(buf, "world", 6));
  EXPECT_EQ(4U, iter_->GetNext(&off, buf));
  EXPECT_EQ(kSegmentSize * 2048, off);
  EXPECT_EQ(0, std::memcmp(buf, "foo", 4));
  EXPECT_EQ(0U, iter_->GetNext(&off, buf));

  // Test file with a full block of zero bytes
  unlink(GetPath("2").c_str());
  char kEmptyBlock[kSegmentSize] = {'\0'};
  WriteData("2", 0, kEmptyBlock, kSegmentSize);
  SetDataIterator("2");
  EXPECT_EQ(0U, iter_->GetNext(&off, buf));

  // Test failure
  mkdir(GetPath("3").c_str(), 0777);
  SetDataIterator("3");
  EXPECT_THROW(iter_->GetNext(&off, buf), std::runtime_error);

  // FIE not supported
  symlink("/dev/urandom", GetPath("4").c_str());
  SetDataIterator("4");
  EXPECT_EQ(kSegmentSize, iter_->GetNext(&off, buf));
  EXPECT_EQ(kSegmentSize, iter_->GetNext(&off, buf));
}

TEST_F(DSStoreTest, FileDataWriter) {
  // Normal case
  SetDataWriter("2");
  char buf[kSegmentSize];
  std::memset(buf, 42, kSegmentSize);
  writer_->Write(buf, kSegmentSize, 0);

  // Rewrite is okay
  SetDataWriter("2");
  writer_->Write(buf, kSegmentSize, kSegmentSize);

  // Argument error
  EXPECT_THROW(writer_->Write(buf, -2, 2 * kSegmentSize), std::runtime_error);

  // Test close error
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  EXPECT_CALL(callback, Call(
      PLEVEL(error, Store),
      StartsWith("Error closing file after writing "), _));

  close(writer_->fd());
  writer_.reset();

  // Test failure
  mkdir(GetPath("3").c_str(), 0777);
  EXPECT_THROW(SetDataWriter("3"), std::runtime_error);
}

TEST_F(DSStoreTest, ChecksumGroupIterator) {
  FSTime mtime;
  uint64_t size;
  std::size_t cg_off;
  char buf[kSegmentSize];

  // No file
  SetChecksumIterator("a", kNumDSPerGroup, 0);
  cg_iter_->GetInfo(&mtime, &size);
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));

  // Data file only
  WriteData("b.d", kSegmentSize, "world", 6);
  SetChecksumIterator("b", kNumDSPerGroup, 0);
  cg_iter_->GetInfo(&mtime, &size);
  EXPECT_EQ(0U, mtime.sec);
  EXPECT_EQ(0U, mtime.ns);
  EXPECT_EQ(kSegmentSize + 6U, size);
  EXPECT_EQ(6U, cg_iter_->GetNext(&cg_off, buf));
  EXPECT_EQ(0, std::memcmp("world", buf, 6));
  EXPECT_EQ(kChecksumGroupSize, cg_off);
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));

  // Checksum file only
  WriteData("c.c", kSegmentSize, "world", 6);
  SetChecksumIterator("c", kNumDSPerGroup, 0);
  EXPECT_EQ(6U, cg_iter_->GetNext(&cg_off, buf));
  EXPECT_EQ(0, std::memcmp("world", buf, 6));
  EXPECT_EQ(kChecksumGroupSize * (2 * kNumDSPerGroup - 1), cg_off);
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));

  // Data and checksum
  WriteData("d.d", kSegmentSize, "hello", 6);
  WriteData("d.c", 0, "world", 6);
  SetChecksumIterator("d", kNumDSPerGroup, 0);
  EXPECT_EQ(6U, cg_iter_->GetNext(&cg_off, buf));
  EXPECT_EQ(0, std::memcmp("hello", buf, 6));
  EXPECT_EQ(kChecksumGroupSize, cg_off);
  EXPECT_EQ(6U, cg_iter_->GetNext(&cg_off, buf));
  EXPECT_EQ(0, std::memcmp("world", buf, 6));
  EXPECT_EQ(kChecksumGroupSize * (kNumDSPerGroup - 1), cg_off);
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));
}

TEST_F(DSStoreTest, ChecksumGroupWriter) {
  // Empty case
  std::size_t cg_off;
  char buf[kSegmentSize];
  SetChecksumWriter("a", kNumDSPerGroup, 0);
  cg_writer_.reset();

  SetChecksumIterator("a", kNumDSPerGroup, 0);
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));

  // Data file write
  SetChecksumWriter("b", kNumDSPerGroup, 0);
  memcpy(buf, "world", 6);
  cg_writer_->Write(buf, 6, CgStart(kSegmentSize * kNumDSPerGroup));
  cg_writer_.reset();

  SetChecksumIterator("b", kNumDSPerGroup, 0);
  EXPECT_EQ(6U, cg_iter_->GetNext(&cg_off, buf));
  EXPECT_EQ(0, std::memcmp("world", buf, 6));
  EXPECT_EQ(kChecksumGroupSize, cg_off);
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));

  // Checksum file write
  SetChecksumWriter("c", kNumDSPerGroup, 0);
  memcpy(buf, "world", 6);
  cg_writer_->Write(buf, 6,
                    CgStart(kChecksumGroupSize * (2 * kNumDSPerGroup - 1)));
  cg_writer_.reset();

  SetChecksumIterator("c", kNumDSPerGroup, 0);
  EXPECT_EQ(6U, cg_iter_->GetNext(&cg_off, buf));
  EXPECT_EQ(0, std::memcmp("world", buf, 6));
  EXPECT_EQ(kChecksumGroupSize * (2 * kNumDSPerGroup - 1), cg_off);
  EXPECT_EQ(0U, cg_iter_->GetNext(&cg_off, buf));
}

TEST_F(DSStoreTest, Init) {
  // Initialize() cause directory to be created
  data_path_mgr_.Deinit();
  store_.reset(MakeStore(data_path_));
  // Initialize() multiple times do nothing
  store_.reset(MakeStore(data_path_));
  // Try retrieve UUID
  EXPECT_EQ(36U, store_->GetUUID().length());
  // Fail if we are not permitted to write to it
  chmod(GetRoot().c_str(), 0500);
  EXPECT_THROW(MakeStore(data_path_), std::runtime_error);
  // Fail if we are asked to create the directory somewhere we cannot write to
  std::string dp2 = GetRoot() + "/a";
  EXPECT_THROW(MakeStore(dp2), std::runtime_error);
  // Fail if a non-directory is already there
  data_path_mgr_.Deinit();
  system((boost::format("touch %s") % data_path_).str().c_str());
  EXPECT_THROW(MakeStore(data_path_), std::runtime_error);
}

TEST_F(DSStoreTest, Role) {
  // SetRole can set
  EXPECT_FALSE(store_->is_role_set());
  store_->SetRole(1, 4);
  EXPECT_TRUE(store_->is_role_set());
  EXPECT_EQ(1U, store_->ds_group());
  EXPECT_EQ(4U, store_->ds_role());

  // Set again do nothing
  store_->SetRole(1, 4);

  // If directory already has role set, can read during initialization
  boost::scoped_ptr<IStore> dd2(MakeStore(data_path_));
  dd2->SetRole(1, 4);
  EXPECT_TRUE(dd2->is_role_set());
  EXPECT_EQ(1U, dd2->ds_group());
  EXPECT_EQ(4U, dd2->ds_role());

  // But cannot change
  EXPECT_THROW(store_->SetRole(1, 3), std::runtime_error);
  EXPECT_TRUE(dd2->is_role_set());
  EXPECT_EQ(1U, dd2->ds_group());
  EXPECT_EQ(4U, dd2->ds_role());

  // Strange case 1: attribute does not start with a number
  lsetxattr(data_path_, "user.role", "a", 1, 0);
  EXPECT_THROW(MakeStore(data_path_), std::runtime_error);

  // Strange case 2: attribute does not end correctly
  lsetxattr(data_path_, "user.role", "1-2a", 4, 0);
  EXPECT_THROW(MakeStore(data_path_), std::runtime_error);
}

TEST_F(DSStoreTest, List) {
  WriteData(2, 64, "hello", 5);
  WriteData(12, 64, "hello", 5);
  boost::scoped_ptr<IDirIterator> iter(store_->List());
  std::string name;
  bool is_dir;
  std::vector<InodeNum> inodes;

  while (iter->GetNext(&name, &is_dir)) {
    name = StripPrefixInodeStr(name);
    ASSERT_EQ(".d", name.substr(name.size() - 2));
    name = name.substr(0, name.size() - 2);
    InodeNum inode;
    ASSERT_TRUE(ParseInodeNum(name.data(), name.size(), &inode));
    inodes.push_back(inode);
  }
  std::sort(inodes.begin(), inodes.end());
  ASSERT_THAT(inodes, ElementsAre(2U, 12U));
}

TEST_F(DSStoreTest, InodeList) {
  WriteData(2, 64, "hello", 5);
  WriteData(12, 64, "hello", 5);
  WriteData(12345, 64, "hello", 5);
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  ranges.push_back(256);
  boost::scoped_ptr<IDirIterator> iter(store_->InodeList(ranges));
  std::string name;
  bool is_dir;
  std::vector<InodeNum> inodes;

  while (iter->GetNext(&name, &is_dir)) {
    name = StripPrefixInodeStr(name);
    ASSERT_EQ(".d", name.substr(name.size() - 2));
    name = name.substr(0, name.size() - 2);
    InodeNum inode;
    ASSERT_TRUE(ParseInodeNum(name.data(), name.size(), &inode));
    inodes.push_back(inode);
  }
  std::sort(inodes.begin(), inodes.end());
  ASSERT_THAT(inodes, ElementsAre(2U, 12U));
}

TEST_F(DSStoreTest, RemoveAll) {
  // Normal case, with 1 extra directory
  WriteData(2, 64, "hello", 5);
  WriteData(12, 64, "hello", 5);
  mkdir((GetRoot() + "/a").c_str(), 0777);
  store_->RemoveAll();
  boost::scoped_ptr<IDirIterator> iter(store_->List());
  std::string name;
  bool is_dir;
  ASSERT_TRUE(iter->GetNext(&name, &is_dir));
  EXPECT_TRUE(is_dir);
  ASSERT_FALSE(iter->GetNext(&name, &is_dir));

  // Error case
  WriteData(2, 64, "hello", 5);
  chmod(GetRoot().c_str(), 0555);
  EXPECT_THROW(store_->RemoveAll(), std::runtime_error);
}

TEST_F(DSStoreTest, Write) {
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  char buf[5];
  FSTime optime = {1234567890ULL, 987654321};
  EXPECT_EQ(5, store_->Write(2, optime, 69, 64, "hello", 5, buf));

  std::string path = GetPath(2);
  struct stat stbuf;
  ASSERT_EQ(0, stat(path.c_str(), &stbuf));
  ASSERT_EQ(69, stbuf.st_size);
  EXPECT_EQ(0, std::memcmp("hello", buf, 5));
  FSTime optime2;
  uint64_t size;
  EXPECT_EQ(0, store_->GetAttr(2, &optime2, &size));
  EXPECT_EQ(optime, optime2);
  EXPECT_EQ(69U, size);
  EXPECT_EQ(-ENOENT, store_->GetAttr(5, &optime2, &size));

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  EXPECT_EQ(5, store_->Write(2, optime, 69, 64, "hello", 5, buf));
  EXPECT_EQ(0, std::memcmp("\0\0\0\0\0", buf, 5));

  // If nothing to write, no need to pass valid checksum buffer
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  EXPECT_EQ(0, store_->Write(2, optime, 69, 0, 0, 0, 0));

  // Size check error
  EXPECT_EQ(-EINVAL, store_->Write(2, optime, 0, 0, "hello", -1, buf));

  // Trigger open failure
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  chmod(path.c_str(), 0400);
  EXPECT_EQ(-EACCES, store_->Write(2, optime, 69, 64, "hello", 5, buf));

  // Creation failed due to no permission
  EXPECT_CALL(*durable_range_, Add(3, _, _, _));

  chmod(GetRoot().c_str(), 0500);
  EXPECT_EQ(-EACCES, store_->Write(3, optime, 69, 64, "hello", 5, buf));
}

int SetErr(int err_no) {
  errno = err_no;
  return -1;
}

TEST_F(DSStoreTest, WriteError) {
  MockIPosixFS mock_posix_fs;
  store_->SetPosixFS(&mock_posix_fs);

  // Read error case
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  EXPECT_CALL(mock_posix_fs, Lsetxattr(_, StrEq("user.mt"), _, 20, 0));
  EXPECT_CALL(mock_posix_fs, Lsetxattr(_, StrEq("user.fs"), _, 16, 0));
  char buf[5] = "orig";
  EXPECT_CALL(mock_posix_fs, Pread(_, buf, 5, 64))
      .WillOnce(Invoke(boost::bind(&SetErr, EIO)));

  FSTime optime = {1234567890ULL, 987654321};
  EXPECT_EQ(-EIO, store_->Write(2, optime, 69, 64, "hello", 5, buf));

  // Trigger write error
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  char written[5];
  EXPECT_CALL(mock_posix_fs, Pread(_, buf, 5, 64));
  EXPECT_CALL(mock_posix_fs, Pwrite(_, _, 5, 64))
      .WillOnce(DoAll(
          IgnoreResult(Invoke(boost::bind(&std::memcpy, written, _2, 5))),
          Invoke(boost::bind(&SetErr, EIO))));

  EXPECT_EQ(-EIO, store_->Write(2, optime, 69, 64, "hello", 5, buf));
  EXPECT_EQ(0, memcmp(written, "hello", 5));

  // Short write: try rewrite with original content
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  EXPECT_CALL(mock_posix_fs, Pread(_, buf, 5, 64))
      .WillOnce(Return(5));
  EXPECT_CALL(mock_posix_fs, Pwrite(_, _, 5, 64))
      .WillOnce(Return(4))
      .WillOnce(DoAll(
          IgnoreResult(Invoke(boost::bind(&std::memcpy, written, _2, 5))),
          Return(5)));

  std::memcpy(buf, "orig", 5);
  EXPECT_EQ(-ENOSPC, store_->Write(2, optime, 69, 64, "hello", 5, buf));
  EXPECT_EQ(0, memcmp(written, "orig", 5));

  // Failed creating attributes
  EXPECT_CALL(*durable_range_, Add(3, _, _, _));
  EXPECT_CALL(mock_posix_fs, Lsetxattr(_, StrEq("user.mt"), _, 20, 0))
      .WillOnce(Invoke(boost::bind(&SetErr, ENOSPC)));

  EXPECT_EQ(-ENOSPC, store_->Write(3, optime, 69, 64, "hello", 5, buf));
  struct stat stbuf;
  EXPECT_EQ(-1, stat(GetPath(3).c_str(), &stbuf));
  EXPECT_EQ(ENOENT, errno);  // Creation reverted
}

TEST_F(DSStoreTest, Read) {
  char buf[5];
  std::string path = GetPath(2);
  EXPECT_EQ(0, store_->Read(2, false, 64, buf, 5));
  std::ofstream of(path.c_str());
  of << "Hello";
  of.close();
  EXPECT_EQ(5, store_->Read(2, false, 0, buf, 5));
  EXPECT_EQ(0, std::memcmp("Hello", buf, 5));
  EXPECT_EQ(0, store_->Read(2, false, 5, buf, 5));

  // Trigger open error
  unlink(path.c_str());
  symlink(path.c_str(), path.c_str());
  EXPECT_EQ(-ELOOP, store_->Read(2, false, 5, buf, 5));

  // Trigger read error
  unlink(path.c_str());
  mkdir(path.c_str(), 0777);
  EXPECT_EQ(-EISDIR, store_->Read(2, false, 5, buf, 5));
}

TEST_F(DSStoreTest, ApplyDelta) {
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  FSTime optime = {1234567890ULL, 987654321};
  char buf[5];
  EXPECT_EQ(5, store_->ApplyDelta(2, optime, 69, 64, "Hello", 5));
  EXPECT_EQ(5, store_->Read(2, true, 64, buf, 5));
  EXPECT_EQ(0, std::memcmp("Hello", buf, 5));
  std::string path = GetPath(2, true);
  FSTime optime2;
  uint64_t size;
  EXPECT_EQ(0, store_->GetAttr(2, &optime2, &size));
  EXPECT_EQ(optime, optime2);
  EXPECT_EQ(69U, size);

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  EXPECT_EQ(5, store_->ApplyDelta(2, optime, 69, 64, "Hello", 5));
  EXPECT_EQ(5, store_->Read(2, true, 64, buf, 5));
  EXPECT_EQ(0, std::memcmp("\0\0\0\0\0", buf, 5));

  // Trigger open error
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  chmod(path.c_str(), 0400);
  EXPECT_EQ(-EACCES, store_->ApplyDelta(2, optime, 69, 64, "hello", 5));
  chmod(path.c_str(), 0700);

  // Trigger read error
  EXPECT_EQ(-EINVAL, store_->ApplyDelta(2, optime, 0, -1, 0, 0));
}

TEST_F(DSStoreTest, ApplyDeltaError) {
  MockIPosixFS mock_posix_fs;
  store_->SetPosixFS(&mock_posix_fs);

  // Write error
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  EXPECT_CALL(mock_posix_fs, Pread(_, _, 5, 64));
  EXPECT_CALL(mock_posix_fs, Pwrite(_, _, 5, 64))
      .WillOnce(Invoke(boost::bind(&SetErr, EIO)));

  FSTime optime = {1234567890ULL, 987654321};
  EXPECT_EQ(-EIO, store_->ApplyDelta(2, optime, 69, 64, "Hello", 5));

  // Write error: no space
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  EXPECT_CALL(mock_posix_fs, Pread(_, _, 5, 64))
      .WillOnce(DoAll(
          IgnoreResult(Invoke(boost::bind(std::memcpy, _2, "orig", 5))),
          Return(5)));
  char written[5];
  EXPECT_CALL(mock_posix_fs, Pwrite(_, _, 5, 64))
      .WillOnce(Invoke(boost::bind(&SetErr, ENOSPC)))
      .WillOnce(DoAll(
          IgnoreResult(Invoke(boost::bind(std::memcpy, written, _2, 5))),
          Return(5)));

  EXPECT_EQ(-ENOSPC, store_->ApplyDelta(2, optime, 69, 64, "Hello", 5));
  EXPECT_EQ(0, std::memcmp(written, "orig", 5));
}

TEST_F(DSStoreTest, ApplyDeltaDataError) {
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  FSTime optime = {1234567890ULL, 987654321};
  EXPECT_EQ(5, store_->ApplyDelta(2, optime, 69, 64, "Hello", 5));

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  EXPECT_CALL(callback, Call(
      PLEVEL(warning, Store),
      StartsWith("Unexpected failure when updating file time of "
                 "0000000000000002 upon checksum update, errno ="),
      _));

  unlink(GetPath(2).c_str());
  chmod((std::string(data_path_) + "/000").c_str(), 0500);
  EXPECT_EQ(5, store_->ApplyDelta(2, optime, 69, 64, "Hello", 5));
}

TEST_F(DSStoreTest, TruncateData) {
  WriteData(2, 0, "hello, world", 12);

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  FSTime optime = {1234567890ULL, 987654321};
  store_->ApplyDelta(2, optime, 5, 0, "Hello", 5);

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  char buf[5];
  EXPECT_EQ(0, store_->TruncateData(2, optime, 7, 3, 5, buf));
  EXPECT_EQ(0, std::memcmp("world", buf, 5));
  std::string path = GetPath(2);
  FSTime optime2;
  uint64_t size;
  EXPECT_EQ(0, store_->GetAttr(2, &optime2, &size));
  EXPECT_EQ(optime, optime2);
  EXPECT_EQ(0U, size);
  struct stat stbuf;
  ASSERT_EQ(0, stat(GetPath(2, true).c_str(), &stbuf));
  ASSERT_EQ(3, stbuf.st_size);

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  EXPECT_EQ(0, store_->TruncateData(2, optime, 5, 0, 0, 0));
  ASSERT_EQ(0, stat(path.c_str(), &stbuf));
  ASSERT_EQ(5, stbuf.st_size);

  // Truncate non-existent file does nothing
  EXPECT_EQ(0, store_->TruncateData(3, optime, 7, 0, 5, buf));
  EXPECT_EQ(0, std::memcmp("\0\0\0\0\0", buf, 5));

  // Trigger open error
  chmod(path.c_str(), 0400);
  EXPECT_EQ(-EACCES, store_->TruncateData(2, optime, 64, 0, 0, 0));
  chmod(path.c_str(), 0700);

  // Trigger truncate error
  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  EXPECT_EQ(-EINVAL, store_->TruncateData(2, optime, -1, 0, 0, 0));
}

TEST_F(DSStoreTest, TruncateDataError) {
  WriteData(2, 0, "hello, world", 12);
  MockIPosixFS mock_posix_fs;
  store_->SetPosixFS(&mock_posix_fs);

  EXPECT_CALL(mock_posix_fs, Pread(_, _, 5, 7))
      .WillOnce(Invoke(boost::bind(&SetErr, EIO)));

  FSTime optime = {1234567890ULL, 987654321};
  char buf[5];
  EXPECT_EQ(-EIO, store_->TruncateData(2, optime, 7, 0, 5, buf));
}

TEST_F(DSStoreTest, TruncateDataChecksumError) {
  WriteData(2, 0, "hello, world", 12);

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  FSTime optime = {1234567890ULL, 987654321};
  store_->ApplyDelta(2, optime, 5, 0, "Hello", 5);

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  EXPECT_CALL(callback, Call(
      PLEVEL(warning, Store),
      StartsWith("Unexpected failure when truncating checksum for inode "
                 "0000000000000002: errno ="),
      _));

  chmod(GetPath(2, true).c_str(), 0400);
  char buf[5];
  EXPECT_EQ(0, store_->TruncateData(2, optime, 7, 3, 5, buf));
}

TEST_F(DSStoreTest, UpdateMtime) {
  WriteData(2, 64, "hello", 5);

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  FSTime mtime1 = {1234567890ULL, 12345};
  uint64_t size;
  EXPECT_EQ(0, store_->UpdateMtime(2, mtime1, &size));
  EXPECT_EQ(69U, size);
  FSTime mtime2;
  size = 0;
  EXPECT_EQ(0, store_->GetAttr(2, &mtime2, &size));
  EXPECT_EQ(mtime1, mtime2);
  EXPECT_EQ(69U, size);
}

TEST_F(DSStoreTest, FreeData) {
  WriteData(2, 64, "hello", 5);

  // Normal case
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(2));

  EXPECT_EQ(0, store_->FreeData(2, false));
  struct stat stbuf;
  std::string path = GetPath(2);
  ASSERT_EQ(-1, stat(path.c_str(), &stbuf));
  ASSERT_EQ(ENOENT, errno);

  // File non-existing case
  EXPECT_CALL(*inode_removal_tracker_, RecordRemoved(2));

  EXPECT_EQ(0, store_->FreeData(2, false));

  // Trigger unlink error
  mkdir(path.c_str(), 0777);
  EXPECT_EQ(-EISDIR, store_->FreeData(2, true));
}

TEST_F(DSStoreTest, FileData) {
  boost::scoped_ptr<IChecksumGroupIterator> iter(
      store_->GetChecksumGroupIterator(2));
  EXPECT_TRUE(iter);
  boost::scoped_ptr<IChecksumGroupWriter> writer(
      store_->GetChecksumGroupWriter(2));
  EXPECT_TRUE(writer);
}

TEST_F(DSStoreTest, FileXattr) {
  std::ofstream(GetPath(1).c_str()).close();
  FSTime mod_time1, mod_time2 = {1234, 5678};
  uint64_t file_size;

  // Normal errors
  EXPECT_EQ(-ENODATA, store_->GetAttr(1, &mod_time1, &file_size));

  EXPECT_CALL(*durable_range_, Add(2, _, _, _));

  EXPECT_EQ(-ENOENT, store_->SetAttr(2, mod_time2, 12345));

  EXPECT_CALL(*durable_range_, Add(3, _, _, _));

  mknod(GetPath(3).c_str(), 0666 | S_IFIFO, 0);
  EXPECT_EQ(-EPERM, store_->SetAttr(3, mod_time2, 12345));

  EXPECT_CALL(*durable_range_, Add(3, _, _, _));

  EXPECT_EQ(-EPERM, store_->SetAttr(3, mod_time2, 12345, kSkipMtime));

  // Successful cases
  EXPECT_CALL(*durable_range_, Add(1, _, _, _));

  EXPECT_EQ(0, store_->SetAttr(1, mod_time2, 12345));
  EXPECT_EQ(0, store_->GetAttr(1, &mod_time1, &file_size));
  EXPECT_EQ(mod_time2, mod_time1);
  EXPECT_EQ(12345U, file_size);

  EXPECT_CALL(*durable_range_, Add(1, _, _, _));

  store_->UpdateAttr(1, mod_time1, 12344);
  EXPECT_EQ(0, store_->GetAttr(1, &mod_time1, &file_size));
  EXPECT_EQ(mod_time2, mod_time1);
  EXPECT_EQ(12345U, file_size);

  // Only modify size if kSkipMtime specified
  EXPECT_CALL(*durable_range_, Add(1, _, _, _));

  ++mod_time1.ns;
  EXPECT_EQ(0, store_->SetAttr(1, mod_time1, 12346, kSkipMtime));
  EXPECT_EQ(0, store_->GetAttr(1, &mod_time1, &file_size));
  EXPECT_EQ(mod_time2, mod_time1);
  EXPECT_EQ(12346U, file_size);

  // Only modify mtime if kSkipFileSize specified
  EXPECT_CALL(*durable_range_, Add(1, _, _, _));

  ++mod_time2.ns;
  EXPECT_EQ(0, store_->SetAttr(1, mod_time2, 12347, kSkipFileSize));
  EXPECT_EQ(0, store_->GetAttr(1, &mod_time1, &file_size));
  EXPECT_EQ(mod_time2, mod_time1);
  EXPECT_EQ(12346U, file_size);

  // Format error
  lsetxattr(GetPath(1).c_str(), "user.fs", "xxx", 3, 0);
  EXPECT_EQ(-EIO, store_->GetAttr(1, &mod_time1, &file_size));
  lsetxattr(GetPath(1).c_str(), "user.mt", "xxx", 3, 0);
  EXPECT_EQ(-EIO, store_->GetAttr(1, &mod_time1, &file_size));
}

TEST_F(DSStoreTest, Stat) {
  // Normal case
  uint64_t total_space;
  uint64_t free_space;
  EXPECT_EQ(0, store_->Stat(&total_space, &free_space));
  EXPECT_GT(total_space, 0U);
  EXPECT_GT(free_space, 0U);

  // Test failure
  data_path_mgr_.Deinit();
  EXPECT_NE(0, store_->Stat(&total_space, &free_space));
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
