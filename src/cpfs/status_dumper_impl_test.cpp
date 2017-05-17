/* Copyright 2013 ClusterTech Ltd */
#include "status_dumper_impl.hpp"

#include <csignal>

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "status_dumper_mock.hpp"

using ::testing::_;
using ::testing::Pointee;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace {

TEST(StatusDumperTest, API) {
  // Construction
  MockIStatusDumpable dumpable;
  MockIAsioPolicy asio_policy;
  IOService io_service;
  SignalSet* signal_set = new SignalSet(io_service);
  EXPECT_CALL(asio_policy, MakeSignalSet())
      .WillOnce(Return(signal_set));
  boost::scoped_ptr<IStatusDumper> status_dumper(MakeStatusDumper(&dumpable));

  // Setting asio policy
  SignalHandler handler;
  EXPECT_CALL(asio_policy, SetSignalHandler(signal_set, Pointee(SIGHUP), 2, _))
      .WillOnce(SaveArg<3>(&handler));

  status_dumper->SetAsioPolicy(&asio_policy);

  // Running handler
  EXPECT_CALL(dumpable, Dump());
  EXPECT_CALL(asio_policy, SetSignalHandler(signal_set, Pointee(SIGHUP), 2, _))
      .WillOnce(SaveArg<3>(&handler));

  handler(boost::system::error_code(), SIGUSR2);

  // Also try SIGHUP
  EXPECT_CALL(asio_policy, SetSignalHandler(signal_set, Pointee(SIGHUP), 2, _))
      .WillOnce(SaveArg<3>(&handler));

  handler(boost::system::error_code(), SIGHUP);

  // Nothing will happen if cancelled
  handler(boost::system::error_code(
      boost::system::errc::operation_canceled,
      boost::system::system_category()), 0);

  // Destruction
  EXPECT_CALL(asio_policy, SetSignalHandler(signal_set, 0, 0, _));
}

TEST(StatusDumperTest, Unset) {
  MockIStatusDumpable dumpable;

  boost::scoped_ptr<IStatusDumper> status_dumper(MakeStatusDumper(&dumpable));
  (void) status_dumper;  // dtor will not fail
}

}  // namespace
}  // namespace cpfs
