/* Copyright 2013 ClusterTech Ltd */
#include "client/fs_common_lowlevel.hpp"

#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "req_entry_mock.hpp"

namespace cpfs {
namespace client {
namespace {

using testing::Return;
using testing::ReturnRef;

TEST(FSCommonLLUtilTest, CreateReplyCallback) {
  // Setup a req_entry to always say the reply is a valid one
  boost::shared_ptr<MockIReqEntry> req_entry(new MockIReqEntry);
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->inode = 42;
  reply_fim->tail_buf_resize(sizeof(GroupId));
  *reinterpret_cast<GroupId*>(reply_fim->tail_buf()) = 3;
  FIM_PTR<IFim> reply_ifim = reply_fim;
  EXPECT_CALL(*req_entry, reply())
      .WillRepeatedly(ReturnRef(reply_ifim));

  // Try mkdir
  FIM_PTR<MkdirFim> mkdir_req_fim = MkdirFim::MakePtr();
  EXPECT_CALL(*req_entry, request())
      .WillOnce(Return(mkdir_req_fim));

  CreateReplyCallback(req_entry);
  EXPECT_EQ(42U, (*mkdir_req_fim)->req.new_inode);
  EXPECT_EQ(0U, mkdir_req_fim->tail_buf_size());

  // Try symlink
  FIM_PTR<SymlinkFim> symlink_req_fim = SymlinkFim::MakePtr();
  EXPECT_CALL(*req_entry, request())
      .WillOnce(Return(symlink_req_fim));

  CreateReplyCallback(req_entry);
  EXPECT_EQ(42U, (*symlink_req_fim)->new_inode);
  EXPECT_EQ(0U, symlink_req_fim->tail_buf_size());

  // Try mknod
  FIM_PTR<MknodFim> mknod_req_fim = MknodFim::MakePtr();
  EXPECT_CALL(*req_entry, request())
      .WillOnce(Return(mknod_req_fim));

  CreateReplyCallback(req_entry);
  EXPECT_EQ(42U, (*mknod_req_fim)->new_inode);
  EXPECT_EQ(1U, (*mknod_req_fim)->num_ds_groups);
  EXPECT_EQ(sizeof(GroupId), mknod_req_fim->tail_buf_size());
  EXPECT_EQ(3U, *reinterpret_cast<GroupId*>(mknod_req_fim->tail_buf()));
}

}  // namespace
}  // namespace client
}  // namespace cpfs
