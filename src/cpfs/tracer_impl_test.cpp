/* Copyright 2015 ClusterTech Ltd */
#include "tracer.hpp"

#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "tracer_impl.hpp"

using ::testing::Mock;
using ::testing::Return;

namespace cpfs {
namespace {

TEST(Tracer, Basic) {
  boost::scoped_ptr<ITracer> tracer(MakeTracer());
  tracer.reset(MakeSmallTracer());
  std::vector<std::string> ops;
  // Empty
  ops = tracer->DumpAll();
  EXPECT_EQ(0U, ops.size());

  tracer->Log("TestMethodA", 100, 1);
  tracer->Log("TestMethodB", 200, 1);
  ops = tracer->DumpAll();
  EXPECT_EQ(2U, ops.size());
  EXPECT_NE(std::string::npos,
      ops[0].find("TestMethodA() inode: 100, client: 1"));
  EXPECT_NE(std::string::npos,
      ops[1].find("TestMethodB() inode: 200, client: 1"));

  for (unsigned i = 0; i < 5000; ++i)
    tracer->Log("TestMethodC", 300, 1);
  ops = tracer->DumpAll();
  EXPECT_EQ(5U, ops.size());
  EXPECT_NE(std::string::npos,
      ops[0].find("TestMethodC() inode: 300, client: 1"));
  EXPECT_NE(std::string::npos,
      ops[1].find("TestMethodC() inode: 300, client: 1"));
  EXPECT_NE(std::string::npos,
      ops[2].find("TestMethodC() inode: 300, client: 1"));
  EXPECT_NE(std::string::npos,
      ops[3].find("TestMethodC() inode: 300, client: 1"));
  EXPECT_NE(std::string::npos,
      ops[4].find("TestMethodC() inode: 300, client: 1"));
}

}  // namespace
}  // namespace cpfs
