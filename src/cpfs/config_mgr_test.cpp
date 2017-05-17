/* Copyright 2013 ClusterTech Ltd */
#include "config_mgr.hpp"

#include <gtest/gtest.h>

namespace cpfs {
namespace {

class ConfigMgrTest : public ::testing::Test {
 protected:
  cpfs::ConfigMgr cfg_;
};

TEST_F(ConfigMgrTest, GetSetConfig) {
  EXPECT_TRUE(cfg_.log_severity().empty());
  cfg_.set_log_severity("3");
  EXPECT_EQ("3", cfg_.log_severity());
  cfg_.set_log_severity("5");
  EXPECT_EQ("5", cfg_.log_severity());
  cfg_.set_log_path("/dev/null");
  EXPECT_EQ("/dev/null", cfg_.log_path());
}

}  // namespace
}  // namespace cpfs
