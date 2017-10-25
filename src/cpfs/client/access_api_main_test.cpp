/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS API client.
 */

#include "client/access_api_main.hpp"

#include <cstdlib>

#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "client/base_client.hpp"

namespace cpfs {
namespace client {
namespace {

TEST(APIMainTest, Construct) {
  setenv("CPFS_KEY_PATH", ".", 1);
  boost::scoped_ptr<BaseFSClient> client(MakeAPIClient(4));
  ASSERT_TRUE(client);
  client->Init();
  client->Shutdown();
}

}  // namespace
}  // namespace client
}  // namespace cpfs
