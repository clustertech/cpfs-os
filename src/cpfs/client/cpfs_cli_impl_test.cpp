/* Copyright 2014 ClusterTech Ltd */
#include "cpfs_cli_impl.hpp"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "admin_info.hpp"
#include "console_mock.hpp"
#include "client/cpfs_admin_mock.hpp"
#include "client/cpfs_cli.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StartsWith;

namespace cpfs {
namespace client {
namespace {

class CpfsCLITest : public ::testing::Test {
 protected:
  MockICpfsAdmin cpfs_admin_;
  MockIConsole console_;
  boost::scoped_ptr<ICpfsCLI> cpfs_cli_;

  CpfsCLITest()
      : cpfs_cli_(MakeCpfsCLI(&cpfs_admin_, &console_)) {}
};

TEST_F(CpfsCLITest, Init) {
  // Connected
  EXPECT_CALL(console_, PrintLine("Connecting to CPFS"));
  EXPECT_CALL(console_, PrintLine("Connected to CPFS"));
  EXPECT_CALL(cpfs_admin_, Init(_, _))
      .WillOnce(Return(true));
  cpfs_cli_->Init(true);

  // Connect timeout and force start
  EXPECT_CALL(console_, PrintLine("Connecting to CPFS"));
  EXPECT_CALL(cpfs_admin_, ForceStart(1));
  EXPECT_CALL(console_, PrintLine("Connection to MS2 rejected"));
  EXPECT_CALL(console_, PrintLine("System is started forcibly"));
  std::vector<bool> reject_info;
  reject_info.push_back(false);
  reject_info.push_back(true);
  EXPECT_CALL(cpfs_admin_, Init(_, _))
      .WillOnce(DoAll(SetArgPointee<1>(reject_info),
                      Return(false)));
  cpfs_cli_->Init(true);

  // Connect timeout and unable to force start
  EXPECT_CALL(console_, PrintLine("Connecting to CPFS"));
  EXPECT_CALL(console_, PrintLine("Connection to CPFS failed"));

  std::vector<bool> all_reject_info;
  all_reject_info.push_back(false);
  all_reject_info.push_back(false);
  EXPECT_CALL(cpfs_admin_, Init(_, _))
      .WillOnce(DoAll(SetArgPointee<1>(all_reject_info),
                      Return(false)));
  cpfs_cli_->Init(true);

  // Connect timeout without force start
  EXPECT_CALL(console_, PrintLine("Connecting to CPFS"));
  EXPECT_CALL(console_, PrintLine("Connection to CPFS failed"));
  EXPECT_CALL(cpfs_admin_, Init(_, _))
      .WillOnce(Return(false));
  cpfs_cli_->Init(false);
}

TEST_F(CpfsCLITest, PromptHelp) {
  EXPECT_CALL(console_, ReadLine())
      .WillOnce(Return(std::string("help")))
      .WillOnce(Return(std::string("exit")));

  EXPECT_CALL(console_, PrintLine(_));

  cpfs_cli_->Run("");
}

TEST_F(CpfsCLITest, RunQueryStatus) {
  cpfs::client::ClusterInfo cluster_info;
  cluster_info.ms_role = "MS1";
  cluster_info.ms_state = "Active";
  EXPECT_CALL(cpfs_admin_, QueryStatus())
      .WillOnce(Return(cluster_info));

  EXPECT_CALL(console_, PrintLine("MS1: Active"));
  EXPECT_CALL(console_, PrintTable(_));

  cpfs_cli_->Run("status");
}

TEST_F(CpfsCLITest, RunQueryInfo) {
  cpfs::client::DiskInfoList disk_info;
  EXPECT_CALL(cpfs_admin_, QueryDiskInfo())
      .WillOnce(Return(disk_info));
  EXPECT_CALL(console_, PrintTable(_));

  cpfs_cli_->Run("info");
}

TEST_F(CpfsCLITest, RunConfigList) {
  cpfs::client::ConfigList config_list;
  EXPECT_CALL(cpfs_admin_, ListConfig())
      .WillOnce(Return(config_list));

  cpfs_cli_->Run("config list");
}

TEST_F(CpfsCLITest, RunConfigSet) {
  // Test DS with extra spaces
  EXPECT_CALL(cpfs_admin_, ChangeConfig("DS0-1", "log_severity", "3"))
      .WillOnce(Return(true));

  cpfs_cli_->Run("config set DS   0-1   log_severity 3");

  // Test MS
  EXPECT_CALL(cpfs_admin_, ChangeConfig("MS1", "log_severity", "3"))
      .WillOnce(Return(true));

  cpfs_cli_->Run("config set MS1 log_severity 3");

  // Test lowercase
  EXPECT_CALL(cpfs_admin_, ChangeConfig("MS1", "log_severity", "3"))
      .WillOnce(Return(true));

  cpfs_cli_->Run("config set ms1 log_severity 3");

  // Test unknown role
  EXPECT_CALL(console_,
              PrintLine("Usage: config set [target] [config] [value]"));

  cpfs_cli_->Run("config set alien log_severity 3");
}

TEST_F(CpfsCLITest, RunConfigHelp) {
  EXPECT_CALL(console_, PrintLine(std::string("Usage: config [list|set]")));

  cpfs_cli_->Run("config");
}

TEST_F(CpfsCLITest, RunConfigSetHelp) {
  EXPECT_CALL(console_,
              PrintLine("Usage: config set [target] [config] [value]"));

  cpfs_cli_->Run("config set");
}

TEST_F(CpfsCLITest, RunUnknownCommandHelp) {
  EXPECT_CALL(console_,
              PrintLine(std::string("Unknown command 'wrong'")));

  cpfs_cli_->Run("wrong commands");
}

TEST_F(CpfsCLITest, RunSystem) {
  EXPECT_CALL(console_, PrintLine("Usage: system [shutdown|restart]"));

  cpfs_cli_->Run("system");
}

TEST_F(CpfsCLITest, PromptSystemStop) {
  EXPECT_CALL(console_, ReadLine())
      .WillOnce(Return(std::string("system shutdown")));
  EXPECT_CALL(cpfs_admin_, SystemShutdown());

  cpfs_cli_->Run("");
}

TEST_F(CpfsCLITest, QueryStatus) {
  cpfs::client::ClusterInfo cluster_info;
  cluster_info.ms_role = "MS1";
  cluster_info.ms_state = "Active";
  cluster_info.dsg_states.push_back("Pending");
  std::map<std::string, std::string> node_info;
  node_info["Alias"] = "MS1";
  node_info["IP"] = "192.168.10.7";
  node_info["Port"] = "3388";
  node_info["PID"] = "1234";
  cluster_info.node_infos.push_back(node_info);
  EXPECT_CALL(cpfs_admin_, QueryStatus())
      .WillOnce(Return(cluster_info));

  EXPECT_CALL(console_, PrintLine(std::string("MS1: Active")));
  EXPECT_CALL(console_, PrintLine(std::string("DSG0: Pending")));
  EXPECT_CALL(console_, PrintTable(_));
  // Real call
  std::vector<std::string> nodes;
  cpfs_cli_->HandleQueryStatus(nodes);
}

TEST_F(CpfsCLITest, QueryInfo) {
  cpfs::client::DiskInfoList disk_info_list;
  std::map<std::string, std::string> disk_info;
  disk_info["Name"] = "DS 0-1";
  disk_info["Total"] = "100";
  disk_info["Free"] = "50";
  disk_info_list.push_back(disk_info);
  EXPECT_CALL(cpfs_admin_, QueryDiskInfo())
      .WillOnce(Return(disk_info_list));
  EXPECT_CALL(console_, PrintTable(_));
  // Real call
  std::vector<std::string> nodes;
  cpfs_cli_->HandleQueryInfo(nodes);
}

TEST_F(CpfsCLITest, ConfigList) {
  cpfs::client::ConfigList config_list;
  config_list.push_back(std::make_pair(kNodeCfg, "MS1"));
  config_list.push_back(std::make_pair("log_path", "/"));
  config_list.push_back(std::make_pair(kNodeCfg, "MS2"));
  config_list.push_back(std::make_pair("log_path", "/"));
  EXPECT_CALL(cpfs_admin_, ListConfig())
      .WillOnce(Return(config_list));
  EXPECT_CALL(console_, PrintTable(_)).Times(2);
  EXPECT_CALL(console_, PrintLine(_)).Times(2);
  // Real call
  std::vector<std::string> configs;
  cpfs_cli_->HandleConfigList(configs);
}

TEST_F(CpfsCLITest, ConfigChange) {
  EXPECT_CALL(cpfs_admin_, ChangeConfig("MS1", "key", "value"))
      .WillOnce(Return(true));
  EXPECT_TRUE(cpfs_cli_->HandleConfigChange("MS1", "key", "value"));
}

TEST_F(CpfsCLITest, SystemStop) {
  EXPECT_CALL(cpfs_admin_, SystemShutdown());

  // Real call
  cpfs_cli_->HandleSystemShutdown();
}

}  // namespace
}  // namespace client
}  // namespace cpfs
