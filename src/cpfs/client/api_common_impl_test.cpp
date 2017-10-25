/* 0Copyright 2015 ClusterTech Ltd */
#include "client/api_common_impl.hpp"

#include <fcntl.h>
#include <stdint.h>  // IWYU pragma: keep

#include <sys/stat.h>

#include <cstring>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cpfs_api.h"  // NOLINT(build/include)
#include "mock_actions.hpp"
#include "client/api_common.hpp"
#include "client/fs_common_lowlevel_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::SaveArg;
using ::testing::SaveArgPointee;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::SetArgPointee;

namespace cpfs {
namespace client {
namespace {

class APICommonTest : public ::testing::Test {
 protected:
  MockIFSCommonLL fs_ll_;
  boost::scoped_ptr<IAPICommon> fs_;
  APICommonTest() : fs_(MakeAPICommon(&fs_ll_)) {}
};

// Open a file
// The path resolution will resolve the target to an inode
TEST_F(APICommonTest, Open) {
  // Mocks for ll->Open()
  FSLookupReply lookup_rets[2];
  lookup_rets[0].inode = 500;
  lookup_rets[0].attr.st_mode = S_IFDIR;
  lookup_rets[1].inode = 600;
  lookup_rets[1].attr.st_mode = S_IFREG;
  EXPECT_CALL(fs_ll_, Lookup(_, 1, StrEq("path_of_dir"), _))
      .WillOnce(DoAll(SetArgPointee<3>(lookup_rets[0]), Return(true)));
  EXPECT_CALL(fs_ll_, Lookup(_, 500, StrEq("filename"), _))
      .WillOnce(DoAll(SetArgPointee<3>(lookup_rets[1]), Return(true)));
  FSOpenReply open_ret;
  open_ret.fh = 9999U;
  EXPECT_CALL(fs_ll_, Open(_, 600, _, _))
      .WillOnce(DoAll(SetArgPointee<3>(open_ret), Return(true)));

  // Actual call
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  EXPECT_EQ(0, fs_->Open("path_of_dir/filename", O_RDONLY, 0640, &handle));
  EXPECT_EQ(O_RDONLY, handle.flags);
  EXPECT_EQ(600U, handle.inode);
  EXPECT_EQ(9999U, handle.fh);
  EXPECT_EQ(1, handle.opened);
  // Open again on the same handle
  EXPECT_EQ(0, fs_->Open("path_of_dir/filename", O_RDONLY, 0640, &handle));
  EXPECT_EQ(O_RDONLY, handle.flags);
  EXPECT_EQ(600U, handle.inode);
  EXPECT_EQ(9999U, handle.fh);
  EXPECT_EQ(1, handle.opened);
}

TEST_F(APICommonTest, OpenLookupError) {
 // Mocks for ll->Lookup()
  EXPECT_CALL(fs_ll_, Lookup(_, 1, StrEq("not_exist"), _))
      .WillOnce(Return(false));
  // Actual call
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  EXPECT_EQ(-1, fs_->Open("not_exist", O_RDONLY, 0640, &handle));
  EXPECT_EQ(0, handle.opened);
}

TEST_F(APICommonTest, OpenFileError) {
  // Mocks for ll->Open()
  FSLookupReply lookup_rets[2];
  lookup_rets[0].inode = 500;
  lookup_rets[0].attr.st_mode = S_IFDIR;
  lookup_rets[1].inode = 600;
  lookup_rets[1].attr.st_mode = S_IFREG;
  EXPECT_CALL(fs_ll_, Lookup(_, 1, StrEq("path_of_dir"), _))
      .WillOnce(DoAll(SetArgPointee<3>(lookup_rets[0]), Return(true)));
  EXPECT_CALL(fs_ll_, Lookup(_, 500, StrEq("filename"), _))
      .WillOnce(DoAll(SetArgPointee<3>(lookup_rets[1]), Return(true)));
  EXPECT_CALL(fs_ll_, Open(_, 600, _, _))
      .WillOnce(Return(false));
  // Actual call
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  EXPECT_EQ(-1, fs_->Open("path_of_dir/filename", O_RDONLY, 0640, &handle));
  EXPECT_EQ(0, handle.opened);
}

TEST_F(APICommonTest, Close) {
  // Actual call
  EXPECT_CALL(fs_ll_, Release(500, _, _));
  CpfsFileHandle handle;
  handle.opened = 1;
  handle.inode = 500;
  fs_->Close(&handle);
  EXPECT_EQ(0, handle.opened);
  // Close again
  fs_->Close(&handle);
  EXPECT_EQ(0, handle.opened);
}

// Create a new file
TEST_F(APICommonTest, OpenForCreate) {
  // Mocks for ll->Lookup()
  FSLookupReply lookup_ret;
  lookup_ret.inode = 1234;
  lookup_ret.attr.st_mode = S_IFDIR;
  EXPECT_CALL(fs_ll_, Lookup(_, 1, StrEq("dir1"), _))
      .WillOnce(DoAll(SetArgPointee<3>(lookup_ret), Return(true)));
  // Mocks for ll->Create() for "new_File" dentry
  FSCreateReply create_ret;
  create_ret.inode = 555;
  create_ret.fh = 9999;
  EXPECT_CALL(
      fs_ll_,
      Create(_, 1234, StrEq("new_file"), 0640, O_CREAT | O_WRONLY, _))
      .WillOnce(DoAll(SetArgPointee<5>(create_ret), Return(true)));
  // Actual call
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  EXPECT_EQ(0,
      fs_->Open("dir1/new_file", O_CREAT | O_WRONLY, 0640, &handle));
  EXPECT_EQ(O_CREAT | O_WRONLY, handle.flags);
  EXPECT_EQ(555U, handle.inode);
  EXPECT_EQ(9999U, handle.fh);
  EXPECT_EQ(1, handle.opened);
}

// Parent for the file not found
TEST_F(APICommonTest, OpenForCreateError) {
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  // Case 1: Lookup error
  // Mocks for ll->Lookup()
  EXPECT_CALL(fs_ll_, Lookup(_, 1, StrEq("dir1"), _))
      .WillOnce(Return(false));
  // Actual call
  EXPECT_EQ(-1,
      fs_->Open("dir1/new_file", O_CREAT | O_WRONLY, 0640, &handle));
  EXPECT_EQ(0, handle.opened);

  // Case 2: Parent path is not a directory
  FSLookupReply lookup_fret;
  lookup_fret.inode = 555;
  lookup_fret.attr.st_mode = S_IFREG;
  // Return dir1 as a file
  EXPECT_CALL(fs_ll_, Lookup(_, 1, StrEq("dir1"), _))
      .WillOnce(DoAll(SetArgPointee<3>(lookup_fret), Return(true)));
  // Actual call
  EXPECT_EQ(-1,
      fs_->Open("dir1/new_file", O_CREAT | O_WRONLY, 0640, &handle));
  EXPECT_EQ(0, handle.opened);

  // Case 3: Create error
  // Mocks for ll->Lookup()
  FSLookupReply lookup_dret;
  lookup_dret.inode = 1234;
  lookup_dret.attr.st_mode = S_IFDIR;
  EXPECT_CALL(fs_ll_, Lookup(_, 1, StrEq("dir1"), _))
      .WillOnce(DoAll(SetArgPointee<3>(lookup_dret), Return(true)));
  // Mocks for ll->Create() for "new_File" dentry
  EXPECT_CALL(
      fs_ll_,
      Create(_, 1234, StrEq("new_file"), 0640, O_CREAT | O_WRONLY, _))
      .WillOnce(Return(false));
  // Actual call
  EXPECT_EQ(-1,
      fs_->Open("dir1/new_file", O_CREAT | O_WRONLY, 0640, &handle));
  EXPECT_EQ(0, handle.opened);
}

// Close a file not yet opened
TEST_F(APICommonTest, CloseNotOpened) {
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  fs_->Close(&handle);
}

TEST_F(APICommonTest, Read) {
  // Mocks for ll->Read()
  EXPECT_CALL(fs_ll_, Read(9999, 500, 1024, 2, _))
      .WillOnce(Return(true));
  // Actual call
  char buf[1024] = {'\0'};
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.fh = 9999;
  handle.inode = 500;
  handle.opened = 1;
  // Actual call
  EXPECT_EQ(0, fs_->Read(buf, 1024, 2, &handle));
}

TEST_F(APICommonTest, ReadNotOpened) {
  // Actual call
  char buf[1024] = {'\0'};
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  EXPECT_EQ(-1, fs_->Read(buf, 1024, 2, &handle));
}

TEST_F(APICommonTest, ReadError) {
  // Mocks for ll->Read()
  EXPECT_CALL(fs_ll_, Read(9999, 500, 1024, 2, _))
      .WillOnce(Return(false));
  // Actual call
  char buf[1024] = {'\0'};
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.fh = 9999;
  handle.inode = 500;
  handle.opened = 1;
  // Actual call
  EXPECT_EQ(-1, fs_->Read(buf, 1024, 2, &handle));
}

TEST_F(APICommonTest, Write) {
  char buf[1024] = {'\0'};
  // Mocks for ll->Write()
  EXPECT_CALL(fs_ll_, Write(9999, 500, buf, 1024, 2, _))
      .WillOnce(Return(true));
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.fh = 9999;
  handle.inode = 500;
  handle.opened = 1;
  // Actual call
  EXPECT_EQ(0, fs_->Write(buf, 1024, 2, &handle));
}

TEST_F(APICommonTest, WriteNotOpened) {
  // Actual call
  char buf[1024] = {'\0'};
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  EXPECT_EQ(-1, fs_->Write(buf, 1024, 2, &handle));
}

TEST_F(APICommonTest, WriteError) {
  char buf[1024] = {'\0'};
  // Mocks for ll->Write()
  EXPECT_CALL(fs_ll_, Write(9999, 500, buf, 1024, 2, _))
      .WillOnce(Return(false));
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.fh = 9999;
  handle.inode = 500;
  handle.opened = 1;
  // Actual call
  EXPECT_EQ(-1, fs_->Write(buf, 1024, 2, &handle));
}

TEST_F(APICommonTest, Getattr) {
  // Mocks for ll->Getattr()
  FSGetattrReply attr_ret;
  EXPECT_CALL(fs_ll_, Getattr(_, 500, _))
      .WillOnce(DoAll(SaveArgPointee<2>(&attr_ret),
                      Return(true)));
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.fh = 9999;
  handle.inode = 500;
  handle.opened = 1;
  // Actual call
  struct stat stbuf_ret;
  EXPECT_EQ(0, fs_->Getattr(&stbuf_ret, &handle));
  EXPECT_EQ(&stbuf_ret, attr_ret.stbuf);
}

TEST_F(APICommonTest, GetattrNotOpened) {
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  struct stat stbuf;
  EXPECT_EQ(-1, fs_->Getattr(&stbuf, &handle));
}

TEST_F(APICommonTest, GetattrError) {
  // Mocks for ll->Getattr()
  EXPECT_CALL(fs_ll_, Getattr(_, 500, _))
      .WillOnce(Return(false));
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.inode = 500;
  handle.opened = 1;
  // Actual call
  struct stat stbuf;
  EXPECT_EQ(-1, fs_->Getattr(&stbuf, &handle));
}

TEST_F(APICommonTest, Setattr) {
  // Mocks for ll->Setattr()
  EXPECT_CALL(fs_ll_, Setattr(_, 500, _, SET_ATTR_SIZE, _))
      .WillOnce(Return(true));
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.fh = 9999;
  handle.inode = 500;
  handle.opened = 1;
  // Actual call
  struct stat stbuf;
  EXPECT_EQ(0, fs_->Setattr(&stbuf, SET_ATTR_SIZE, &handle));
}

TEST_F(APICommonTest, SetattrNotOpened) {
  CpfsFileHandle handle;
  memset(&handle, 0, sizeof(handle));
  struct stat stbuf;
  EXPECT_EQ(-1, fs_->Setattr(&stbuf, SET_ATTR_SIZE, &handle));
}

TEST_F(APICommonTest, SetattrError) {
  // Mocks for ll->Setattr()
  EXPECT_CALL(fs_ll_, Setattr(_, 500, _, SET_ATTR_SIZE, _))
      .WillOnce(Return(false));
  // Manually set open here to skip steps for open
  CpfsFileHandle handle;
  handle.opened = 1;
  handle.inode = 500;
  // Actual call
  struct stat stbuf;
  EXPECT_EQ(-1, fs_->Setattr(&stbuf, SET_ATTR_SIZE, &handle));
}

TEST_F(APICommonTest, FSAsyncRWExecutorBasic) {
  // Stop early
  {
    boost::scoped_ptr<IFSAsyncRWExecutor> executor(
          MakeAsyncRWExecutor(fs_.get()));
  }
  boost::scoped_ptr<IFSAsyncRWExecutor> executor(
        MakeAsyncRWExecutor(fs_.get()));
  executor->Start(2);
  Sleep(0.3)();
  executor->Stop();
}

struct MockVars {
  int ret;
};

void MockReadCallback(int ret, void* data) {
  MockVars* read_vars = static_cast<MockVars*>(data);
  read_vars->ret = ret;
}

TEST_F(APICommonTest, FSAsyncRWExecutorReadWrite) {
  boost::scoped_ptr<IFSAsyncRWExecutor> executor(
        MakeAsyncRWExecutor(fs_.get()));
  executor->Start(2);
  Sleep(0.3)();

  char buf[] = "dummy";
  CpfsFileHandle handle;
  handle.opened = 1;
  MockVars read_vars, write_vars;
  read_vars.ret = -1000;
  write_vars.ret = -1000;
  EXPECT_CALL(fs_ll_, Read(_, _, 5, 0, _))
    .WillOnce(Return(true));
  executor->Add(new APIReadRequest(
      buf, 5, 0, &handle, &MockReadCallback, &read_vars));
  EXPECT_CALL(fs_ll_, Write(_, _, _, 10, 0, _))
    .WillOnce(Return(false));
  executor->Add(new APIWriteRequest(
      buf, 10, 0, &handle, &MockReadCallback, &write_vars));
  Sleep(0.3)();
  executor->Stop();
  // At this points all previous requests are processed
  EXPECT_EQ(0, read_vars.ret);
  EXPECT_EQ(-1, write_vars.ret);
  // Cannot further add request after being stopped
  executor->Add(new APIReadRequest(
      buf, 5, 0, &handle, &MockReadCallback, &read_vars));
}

}  // namespace
}  // namespace client
}  // namespace cpfs
