/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS.
 */

#include "client/client_main.hpp"

#include <cstdlib>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "mock_actions.hpp"
#include "service.hpp"

namespace cpfs {
namespace client {
namespace {

TEST(ClientMainTest, Construct) {
  // Load the key file
  setenv("CPFS_KEY_PATH", ".", 1);
  const char* arg_list[] = {
    "cpfs",
    0
  };
  std::vector<std::string> arg_vector = MakeArgVector(arg_list);
  std::vector<char*> args = MakeArgs(&arg_vector);
  boost::scoped_ptr<IService> client(MakeFSClient(1, args.data()));
  ASSERT_TRUE(client);
  client->Init();
  client->Shutdown();
}

}  // namespace
}  // namespace client
}  // namespace cpfs
