/* Copyright 2013 ClusterTech Ltd */
#include "logger.hpp"

#include <cstring>
#include <stdexcept>
#include <string>

#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim.hpp"
#include "fims.hpp"

namespace cpfs {
namespace {

using ::testing::StrEq;

TEST(Logger, GetSet) {
  EXPECT_EQ(pantheios::notice, GetSeverityCeiling(kServerCategory));
  EXPECT_EQ(pantheios::notice, GetSeverityCeiling(kFimCategory));
  SetSeverityCeiling(kFimCategory, pantheios::debug);
  EXPECT_EQ(pantheios::notice, GetSeverityCeiling(kServerCategory));
  EXPECT_EQ(pantheios::debug, GetSeverityCeiling(kFimCategory));
  SetSeverityCeiling(0, pantheios::informational);
  EXPECT_EQ(pantheios::informational, GetSeverityCeiling(kServerCategory));
  EXPECT_EQ(pantheios::debug, GetSeverityCeiling(kFimCategory));
  EXPECT_TRUE(pantheios_fe_isSeverityLogged(
      0, PLEVEL(informational, Server), 0));
  EXPECT_FALSE(pantheios_fe_isSeverityLogged(
      0, PLEVEL(debug, Server), 0));
  SetSeverityCeiling(0, -1);
  EXPECT_EQ(pantheios::notice, GetSeverityCeiling(kServerCategory));
  EXPECT_EQ(pantheios::debug, GetSeverityCeiling(kFimCategory));
  SetSeverityCeiling(kFimCategory, -1);
}

TEST(Logger, Spec) {
  SetSeverityCeiling("");  // Do nothing
  EXPECT_EQ(pantheios::notice, GetSeverityCeiling(kServerCategory));
  EXPECT_EQ(pantheios::notice, GetSeverityCeiling(kFimCategory));
  SetSeverityCeiling("Fim=7");
  EXPECT_EQ(pantheios::notice, GetSeverityCeiling(kServerCategory));
  EXPECT_EQ(pantheios::debug, GetSeverityCeiling(kFimCategory));
  SetSeverityCeiling("6:Fim=7");
  EXPECT_EQ(pantheios::informational, GetSeverityCeiling(kServerCategory));
  EXPECT_EQ(pantheios::debug, GetSeverityCeiling(kFimCategory));
  EXPECT_TRUE(pantheios_fe_isSeverityLogged(
      0, PLEVEL(informational, Server), 0));
  EXPECT_FALSE(pantheios_fe_isSeverityLogged(
      0, PLEVEL(debug, Server), 0));
  EXPECT_THROW(SetSeverityCeiling("Fim"), std::runtime_error);
  EXPECT_THROW(SetSeverityCeiling("Fim=-1"), std::runtime_error);
  EXPECT_THROW(SetSeverityCeiling("Fim=a=-1"), std::runtime_error);
  EXPECT_THROW(SetSeverityCeiling("Fima=1"), std::runtime_error);
  SetSeverityCeiling(0, -1);
  SetSeverityCeiling(kFimCategory, -1);
}

TEST(Logger, SimpleLog) {
  boost::shared_ptr<int> si;
  PanValue<boost::shared_ptr<int> > pv = PVal(si);
  EXPECT_EQ(0, std::memcmp(pv.Data(), "(null)", 6));
  EXPECT_EQ(6U, pv.Length());

  boost::shared_ptr<int> si2(new int(2));
  PanValue<boost::shared_ptr<int> > pv2 = PVal(si2);
  EXPECT_EQ(0, std::memcmp(pv2.Data(), "2", 1));
  EXPECT_EQ(1U, pv2.Length());

  FIM_PTR<IFim> fp;
  PanValue<FIM_PTR<IFim> > pv3 = PVal(fp);
  EXPECT_EQ(0, std::memcmp(pv3.Data(), "(null)", 6));
  fp = HeartbeatFim::MakePtr();
  PanValue<FIM_PTR<IFim> > pv4 = PVal(fp);
  EXPECT_EQ(0, std::memcmp(pv4.Data(), "#Q00000-000000(Heartbeat)", 25));
}

TEST(Logger, Misc) {
  EXPECT_EQ(std::string("1-0"), GroupRoleName(1, 0));
  SetLogPath("/");
  SetLogPath("/");
  SetLogPath("");
}

}  // namespace
}  // namespace cpfs
