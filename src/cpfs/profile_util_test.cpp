/* Copyright 2013 ClusterTech Ltd */
#include "profile_util.hpp"

#include <gtest/gtest.h>

#include "mock_actions.hpp"

namespace cpfs {
namespace {

TEST(ProfileUtil, StopWatch) {
  StopWatch watch;
  Sleep(0.1)();
  uint64_t ms = watch.ReadTimerMicro();
  EXPECT_GE(ms, 100000U);
  EXPECT_LE(ms, 1000000U);
}

}  // namespace
}  // namespace cpfs
