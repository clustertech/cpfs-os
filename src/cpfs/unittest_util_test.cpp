/* Copyright 2013 ClusterTech Ltd */
#include "unittest_util.hpp"

#include <gtest/gtest.h>

namespace cpfs {
namespace {

TEST(UnittestUtilTest, SignalBlock) {
  // Just exercise the API
  UnittestBlockSignals(true);
  UnittestBlockSignals(false);
}

}  // namespace
}  // namespace cpfs
