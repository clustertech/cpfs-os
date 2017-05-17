/* Copyright 2013 ClusterTech Ltd */
#include "util.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <string>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <gtest/gtest.h>

#include "mock_actions.hpp"

namespace cpfs {
namespace {

struct CopyTester {
  SkipCopy<std::string> x;
  std::string y;
};

TEST(UtilTest, SkipCopy) {
  CopyTester t1;
  EXPECT_EQ("", *(t1.x));
  *(t1.x) = "abc";
  t1.y = "def";
  const CopyTester t2(t1);
  EXPECT_EQ("", *(t2.x));
  EXPECT_EQ("def", t2.y);
  // Can be modified even if const
  *(t2.x) = "abcd";
  EXPECT_EQ("abcd", *(t2.x));
  EXPECT_EQ(4U, t2.x->size());
  CopyTester t3;
  t3 = t1;
  EXPECT_EQ("", *(t3.x));
  EXPECT_EQ("def", t3.y);
}

TEST(UtilTest, CompareTime) {
  struct timespec ts1 = {0, 1};
  struct timespec ts2 = {0, 2};
  EXPECT_TRUE(CompareTime()(ts1, ts2));
  EXPECT_FALSE(CompareTime()(ts1, ts1));
  EXPECT_FALSE(CompareTime()(ts2, ts1));
  struct timespec ts3 = {1, 0};
  struct timespec ts4 = {2, 0};
  EXPECT_TRUE(CompareTime()(ts3, ts4));
  EXPECT_FALSE(CompareTime()(ts3, ts3));
  EXPECT_FALSE(CompareTime()(ts4, ts3));
}

TEST(UtilTest, XorBytesTest) {
  char buf1[10];
  char buf2[10];
  for (unsigned i = 0; i < 10; ++i) {
    buf1[i] = i;
    buf2[i] = 2 * i;
  }
  XorBytes(buf2, buf1, 10);
  for (unsigned i = 0; i < 10; ++i)
    EXPECT_EQ(char(i ^ (2 * i)), buf2[i]);
}

TEST(UtilTest, IpIntConversion) {
  std::string ip_str("192.168.133.1");
  uint32_t ip_int = IPToInt(ip_str);
  EXPECT_EQ(IntToIP(ip_int), ip_str);
}

TEST(UtilTest, ToTimeDuration) {
  EXPECT_EQ(ToTimeDuration(3.5).total_milliseconds(), 3500);
}

TEST(UtilTest, IsWriteFlag) {
  EXPECT_FALSE(IsWriteFlag(O_RDONLY));
  EXPECT_FALSE(IsWriteFlag(O_RDONLY | O_TRUNC));
  EXPECT_TRUE(IsWriteFlag(O_WRONLY));
  EXPECT_TRUE(IsWriteFlag(O_WRONLY | O_TRUNC));
  EXPECT_TRUE(IsWriteFlag(O_RDWR));
  EXPECT_TRUE(IsWriteFlag(O_RDWR | O_TRUNC));
}

TEST(UtilTest, FormatDiskSize) {
  EXPECT_EQ("4 Bytes", FormatDiskSize(4));
  EXPECT_EQ("16383 Bytes", FormatDiskSize(16383));
  EXPECT_EQ("16.2 KB", FormatDiskSize(16584));
  EXPECT_EQ("101.0 MB", FormatDiskSize(1024ULL * 1024ULL * 101ULL));
  EXPECT_EQ("123.0 TB",
            FormatDiskSize(1024ULL * 1024ULL * 1024ULL * 1024ULL * 123ULL));
  // Extreme case
  EXPECT_EQ("0", FormatDiskSize(0));
}

TEST(UtilTest, PIDFile) {
  MockDataPathMgr data_path("/tmp/server_main-XXXXXX");
  std::string pidfile = data_path.GetPath() + std::string("/pidfile");
  WritePID(pidfile);
  FILE* fp = fopen(pidfile.c_str(), "r");
  pid_t pid;
  fscanf(fp, "%d", &pid);
  fclose(fp);
  EXPECT_EQ(getpid(), pid);

  // Cannot open file for write
  WritePID("");
}

TEST(UtilTest, CreateUUID) {
  std::string generated = CreateUUID();
  // 32 characters and four hyphens
  EXPECT_EQ(36U, generated.length());
  // Different per call
  EXPECT_NE(generated, CreateUUID());
}

}  // namespace
}  // namespace cpfs
