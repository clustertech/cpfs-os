/* Copyright 2014 ClusterTech Ltd */
#include "console_impl.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "console.hpp"

using ::testing::_;
using ::testing::Mock;

namespace cpfs {
namespace {

class ConsoleTest : public ::testing::Test {
 protected:
  char stdin_[8192];
  char stdout_[8192];
  char stderr_[8192];
  boost::scoped_ptr<IConsole> console_;
  ConsoleTest() {
    memset(stdin_, '\0', 8192);
    memset(stdout_, '\0', 8192);
    memset(stderr_, '\0', 8192);
    console_.reset(
      MakeConsole(fmemopen(stdin_, 8192, "r"),
                  fmemopen(stdout_, 8192, "w"),
                  fmemopen(stderr_, 8192, "w")));
  }
};

TEST_F(ConsoleTest, ReadLine) {
  strncpy(stdin_, "test\n", 5);
  EXPECT_EQ(std::string("test"), console_->ReadLine());
}

TEST_F(ConsoleTest, PrintLine) {
  console_->PrintLine("test");
  EXPECT_EQ(std::string("test\n"), std::string(stdout_));
}

TEST_F(ConsoleTest, PrintTable) {
  std::vector<std::vector<std::string> > rows;
  std::vector<std::string> headers;
  headers.push_back("Name");
  headers.push_back("PID");
  headers.push_back("IP");
  headers.push_back("State");
  rows.push_back(headers);
  {
    std::vector<std::string> row;
    row.push_back("cpe1.clustertech.com");
    row.push_back("1234");
    row.push_back("192.168.1.137");
    row.push_back("Connected");
    rows.push_back(row);
  }
  {
    std::vector<std::string> row;
    row.push_back("testing.clustertech.com");
    row.push_back("3212");
    row.push_back("192.168.125.210");
    row.push_back("Disconnected");
    rows.push_back(row);
  }
  {
    std::vector<std::string> row;
    row.push_back("test.com");
    row.push_back("0");
    row.push_back("192.168.125.121");
    row.push_back("Connected");
    rows.push_back(row);
  }
  console_->PrintTable(rows);
  std::string output =
      " Name                    | PID  | IP              | State        \n"
      "------------------------------------------------------------------\n"
      " cpe1.clustertech.com    | 1234 | 192.168.1.137   | Connected    \n"
      " testing.clustertech.com | 3212 | 192.168.125.210 | Disconnected \n"
      " test.com                | 0    | 192.168.125.121 | Connected    \n";
  EXPECT_EQ(output, std::string(stdout_));
}

}  // namespace
}  // namespace cpfs
