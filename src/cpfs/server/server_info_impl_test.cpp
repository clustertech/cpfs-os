/* Copyright 2014 ClusterTech Ltd */
#include "server/server_info_impl.hpp"

#include <sys/stat.h>

#include <stdexcept>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "mock_actions.hpp"
#include "server/server_info.hpp"

namespace cpfs {
namespace server {
namespace {

const char* kDataPath = "/tmp/server_info_impl_test_XXXXXX";

class ServerInfoTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  boost::scoped_ptr<IServerInfo> server_info_;

  ServerInfoTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        server_info_(MakeServerInfo(data_path_)) {}
};

TEST_F(ServerInfoTest, API) {
  // Initial list
  EXPECT_EQ(0U, server_info_->List().size());

  // New key
  server_info_->Set("key1", "value1");
  EXPECT_EQ(std::string("value1"), server_info_->Get("key1"));
  std::vector<std::string> keys = server_info_->List();
  EXPECT_EQ(1U, keys.size());
  EXPECT_EQ("key1", keys[0]);

  // Overwrite
  server_info_->Set("key1", "value1b");
  EXPECT_EQ(std::string("value1b"), server_info_->Get("key1"));

  // New key
  server_info_->Set("key2", "value2");
  EXPECT_EQ(std::string("value2"), server_info_->Get("key2"));
  EXPECT_EQ(2U, server_info_->List().size());

  // Remove the key and get again
  server_info_->Set("key3", "value3");
  EXPECT_EQ(std::string("value3"), server_info_->Get("key3", "def_val3"));
  server_info_->Remove("key3");
  // Key not found
  EXPECT_EQ(std::string("def_val3"), server_info_->Get("key3", "def_val3"));

  // Check that it is persisted
  server_info_.reset(MakeServerInfo(data_path_));
  EXPECT_EQ(std::string("value2"), server_info_->Get("key2"));

  // No permission to set and remove value
  chmod(data_path_, 0);
  EXPECT_THROW(server_info_->Set("key2", "value2"), std::runtime_error);
  EXPECT_THROW(server_info_->Remove("key2"), std::runtime_error);
}

TEST_F(ServerInfoTest, Error) {
  server_info_.reset(MakeServerInfo(std::string(data_path_) + "/hello"));
  EXPECT_THROW(server_info_->List().size(), std::runtime_error);
}

}  // namespace
}  // namespace server
}  // namespace cpfs
