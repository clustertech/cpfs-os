/* Copyright 2013 ClusterTech Ltd */

#include "client/admin_client_impl.hpp"

#include <stdlib.h>

#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "common.hpp"
#include "client/base_client.hpp"

namespace cpfs {
namespace client {
namespace {

TEST(AdminClientTest, Construct) {
  setenv("CPFS_KEY_PATH", ".", 1);
  AdminConfigItems configs;
  configs.meta_servers.push_back("127.0.0.1:4000");
  configs.log_severity = "5";
  configs.heartbeat_interval = 5;
  configs.socket_read_timeout = 5;
  boost::scoped_ptr<BaseAdminClient>(MakeAdminClient(configs));
}

}  // namespace
}  // namespace client
}  // namespace cpfs
