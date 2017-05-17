/* Copyright 2013 ClusterTech Ltd */
#include "client/file_handle.hpp"

#include <stdint.h>

#include <cerrno>
#include <cstring>

#include <gtest/gtest.h>

#include "common.hpp"
#include "fims.hpp"

namespace cpfs {
namespace client {
namespace {

TEST(FileHandleTest, API) {
  GroupId gid = 2;
  FIM_PTR<IFim> fim = DataReplyFim::MakePtr(sizeof(gid));
  std::memcpy(fim->tail_buf(), &gid, sizeof(gid));
  int64_t fh = MakeFH(3, fim);
  EXPECT_EQ(3U, FHFileCoordManager(fh)->inode());
  EXPECT_EQ(0, FHGetErrno(fh, true));
  FHSetErrno(fh, EIO);
  EXPECT_EQ(EIO, FHGetErrno(fh, false));
  EXPECT_EQ(EIO, FHGetErrno(fh, true));
  EXPECT_EQ(0, FHGetErrno(fh, true));
  DeleteFH(fh);
}

}  // namespace
}  // namespace client
}  // namespace cpfs
