/* Copyright 2013 ClusterTech Ltd */
#include "finfo.hpp"

#include <fuse/fuse_lowlevel.h>

#include <sys/stat.h>

#include <gtest/gtest.h>

namespace cpfs {
namespace {

TEST(FInfoTest, FsTime) {
  FSTime fst;
  struct timespec ts;
  ts.tv_sec = 1234567890;
  ts.tv_nsec = 987654321;
  fst.FromTimeSpec(ts);
  EXPECT_EQ(uint64_t(ts.tv_sec), fst.sec);
  EXPECT_EQ(uint64_t(ts.tv_nsec), fst.ns);
  ++fst.sec;
  --fst.ns;
  fst.ToTimeSpec(&ts);
  EXPECT_EQ(uint64_t(ts.tv_sec), fst.sec);
  EXPECT_EQ(uint64_t(ts.tv_nsec), fst.ns);
  FSTime fst2 = fst;
  EXPECT_EQ(fst, fst2);
  EXPECT_FALSE(fst < fst2);
  ++fst2.ns;
  EXPECT_FALSE(fst == fst2);
  EXPECT_LT(fst, fst2);
}

TEST(FInfoTest, ReqContext) {
  fuse_ctx ctx;
  ctx.uid = 100;
  ctx.gid = 101;
  ReqContext context;
  context.FromFuseCtx(&ctx);
  EXPECT_EQ(100U, context.uid);
  EXPECT_EQ(101U, context.gid);
}

TEST(FInfoTest, FileAttr) {
  FileAttr fa;
  fa.mode = S_IFREG | 2;
  fa.nlink = 3;
  fa.uid = 4;
  fa.gid = 4;
  fa.rdev = 5;
  fa.size = 12345;
  fa.atime.sec = 7;
  fa.atime.ns = 70;
  fa.mtime.sec = 8;
  fa.mtime.ns = 80;
  fa.ctime.sec = 9;
  fa.ctime.ns = 90;
  struct stat s;
  fa.ToStat(1, &s);
  EXPECT_EQ(1U, s.st_ino);
  EXPECT_EQ(7U, s.st_atime);
  EXPECT_EQ(9U, s.st_ctime);
  EXPECT_EQ(131072U, s.st_blksize);
  EXPECT_EQ(25U, s.st_blocks);
}

TEST(FInfoTest, ReaddirRecord) {
  ReaddirRecord rec;
  rec.name_len = 0;
  EXPECT_FALSE(rec.IsValid());
  rec.name_len = 100;
  strncpy(rec.name, "abc", 4);
  EXPECT_FALSE(rec.IsValid());
  rec.name_len = 3;
  EXPECT_TRUE(rec.IsValid());
  EXPECT_EQ(24, rec.GetLen());
  EXPECT_EQ(24, rec.GetLenForName("abcde"));
  EXPECT_EQ(32, rec.GetLenForName("abcdef"));
}

}  // namespace
}  // namespace cpfs
