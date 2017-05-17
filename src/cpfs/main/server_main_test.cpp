/* Copyright 2013 ClusterTech Ltd */
#include "main/server_main.hpp"

#include <unistd.h>

#include <cstdlib>
#include <exception>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "config_mgr.hpp"
#include "daemonizer_mock.hpp"
#include "mock_actions.hpp"
#include "service.hpp"
#include "store_util.hpp"

using ::testing::_;

// Assert the exception message and is derived from std::exception
#define ASSERT_THROW_MSG(statement, msg) \
  {                                      \
    std::string error_msg;               \
    try {                                \
      statement;                         \
    } catch (const std::exception& ex) { \
      error_msg = ex.what();             \
    }                                    \
    ASSERT_EQ(msg, error_msg);           \
  }

namespace cpfs {
namespace main {
namespace {

const char* kDefaultArgs[] = {
  "cpfs-server",
  "--meta-server=192.168.1.3:3000,192.168.1.4:4000",
  "--role=MS1",
  "--ds-host=192.168.1.2",
  "--ds-port=3333",
  "--log-level=5",
  0
};

class ServerMainTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;

  ServerMainTest() : data_path_mgr_("/tmp/server_main-XXXXXX") {}

  std::string GetPath() {
    return data_path_mgr_.GetPath();
  }
};

TEST_F(ServerMainTest, ParseOpts) {
  ConfigMgr configs;
  std::vector<std::string> arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back(std::string("--data=") + GetPath());
  std::vector<char*> args = MakeArgs(&arg_vector);
  // Only pass program name, throw options missing
  ASSERT_THROW(ParseOpts(1, args.data(), &configs), std::exception);

  // All arguments valid
  ASSERT_TRUE(ParseOpts(args.size(), args.data(), &configs));
  ASSERT_EQ(std::string("MS1"), configs.role());
  ASSERT_EQ(3333, configs.ds_port());
  ASSERT_EQ(GetPath(), configs.data_dir());
  ASSERT_EQ(std::string("192.168.1.3"), configs.ms1_host());
  ASSERT_EQ(3000, configs.ms1_port());
  ASSERT_EQ("192.168.1.4", configs.ms2_host());
  ASSERT_EQ(4000, configs.ms2_port());

  // Invalid role
  arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back("--data=.");
  arg_vector[2] = "--role=xyz";
  args = MakeArgs(&arg_vector);
  ASSERT_THROW_MSG(
      ParseOpts(args.size(), args.data(), &configs),
      "Unrecognized role");
  // Invalid port
  arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back(std::string("--data=") + GetPath());
  arg_vector[4] = "--ds-port=0";
  args = MakeArgs(&arg_vector);
  ASSERT_THROW_MSG(
      ParseOpts(args.size(), args.data(), &configs),
      "Invalid port number");
  // Invalid data directory
  arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back("--data=no_dir");
  args = MakeArgs(&arg_vector);
  ASSERT_THROW_MSG(
      ParseOpts(args.size(), args.data(), &configs),
      "Data directory does not exist");
  // Role of DS without ds-host
  arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back(std::string("--data=") + GetPath());
  arg_vector[2] = "--role=DS";
  arg_vector.erase(arg_vector.begin() + 3);
  args = MakeArgs(&arg_vector);
  ASSERT_THROW_MSG(
      ParseOpts(args.size(), args.data(), &configs),
      "Missing ds-host");
  // Role of DS without ds-port
  arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back(std::string("--data=") + GetPath());
  arg_vector[2] = "--role=DS";
  arg_vector.erase(arg_vector.begin() + 4);
  args = MakeArgs(&arg_vector);
  ASSERT_THROW_MSG(
      ParseOpts(args.size(), args.data(), &configs),
      "Missing ds-port");
}

TEST_F(ServerMainTest, MS) {
  std::vector<std::string> arg_vector = MakeArgVector(kDefaultArgs);
  LSetUserXattr(GetPath().c_str(), "max-ndsg", "1");
  arg_vector.push_back(std::string("--data=") + GetPath());
  std::vector<char*> args = MakeArgs(&arg_vector);
  // Noraml run
  ASSERT_TRUE(boost::shared_ptr<IService>(
      MakeServer(args.size(), args.data(), 0)));
}

TEST_F(ServerMainTest, MS2) {
  std::vector<std::string> arg_vector = MakeArgVector(kDefaultArgs);
  LSetUserXattr(GetPath().c_str(), "max-ndsg", "1");
  arg_vector.push_back(std::string("--data=") + GetPath());
  arg_vector[2] = "--role=MS2";
  std::vector<char*> args = MakeArgs(&arg_vector);
  // Noraml run
  ASSERT_TRUE(boost::shared_ptr<IService>(
      MakeServer(args.size(), args.data(), 0)));
}

TEST_F(ServerMainTest, DS) {
  std::vector<std::string> arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back(std::string("--data=") + GetPath());
  arg_vector[2] = "--role=DS";
  std::vector<char*> args = MakeArgs(&arg_vector);
  // Noraml run
  ASSERT_TRUE(boost::shared_ptr<IService>(
      MakeServer(args.size(), args.data(), 0)));
}

TEST_F(ServerMainTest, Misc) {
  std::vector<std::string> arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back(std::string("--data=") + GetPath());
  std::vector<char*> args = MakeArgs(&arg_vector);

  // No option given
  ASSERT_FALSE(MakeServer(1, args.data(), 0));

  // Show help
  int status;
  char buffer[1024];
  if (ForkCaptureResult res = ForkCapture(buffer, 1024, &status)) {
    execl("tbuild/cpfs/main/server_main_test_parse", "cpfs-server", "--help",
          NULL);
    exit(2);
  }
  EXPECT_EQ(1, WEXITSTATUS(status));
  EXPECT_NE(std::string(buffer).find("Allowed options"), std::string::npos);
}

TEST_F(ServerMainTest, Daemonizer) {
  MockIDaemonizer daemonizer;
  std::string pidfile = GetPath() + std::string("/pidfile");
  std::vector<std::string> arg_vector = MakeArgVector(kDefaultArgs);
  arg_vector.push_back(std::string("--data=") + GetPath());
  arg_vector.push_back("--daemonize=true");
  arg_vector.push_back("--pidfile=" + pidfile);
  std::vector<char*> args = MakeArgs(&arg_vector);
  EXPECT_CALL(daemonizer, Daemonize());
  // Noraml run
  ASSERT_TRUE(boost::shared_ptr<IService>(
      MakeServer(args.size(), args.data(), &daemonizer)));
}

}  // namespace
}  // namespace main
}  // namespace cpfs
