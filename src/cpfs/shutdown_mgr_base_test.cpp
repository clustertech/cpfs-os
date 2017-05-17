/* Copyright 2014 ClusterTech Ltd */
#include "shutdown_mgr_base.hpp"

#include <csignal>

#include <boost/function.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"

using ::testing::_;
using ::testing::Pointee;
using ::testing::SaveArg;
using ::testing::Return;

namespace cpfs {
namespace {

class TestShutdownMgr : public BaseShutdownMgr {
 public:
  MOCK_METHOD0(DoInit, bool());
  MOCK_METHOD0(DoShutdown, bool());
};

TEST(ShutdownMgrTest, InitShutdown) {
  // On construct, not inited
  IOService io_service;
  MockIAsioPolicy asio_policy;
  EXPECT_CALL(asio_policy, io_service())
      .WillRepeatedly(Return(&io_service));
  DeadlineTimer* timer = new DeadlineTimer(io_service);
  EXPECT_CALL(asio_policy, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  TestShutdownMgr mgr;
  mgr.SetAsioPolicy(&asio_policy);
  EXPECT_FALSE(mgr.inited());

  // Init
  IOHandler timeout_handler;
  EXPECT_CALL(asio_policy, SetDeadlineTimer(timer, 10, _))
      .WillOnce(SaveArg<2>(&timeout_handler));  // before destroy
  EXPECT_CALL(mgr, DoInit())
      .WillOnce(Return(true));

  EXPECT_TRUE(mgr.Init(10));
  EXPECT_TRUE(mgr.inited());

  // Can call again, no effect
  EXPECT_FALSE(mgr.Init(10));

  // handler has no effect if it is due to an error
  timeout_handler(boost::system::error_code(
      boost::system::errc::operation_canceled,
      boost::system::system_category()));

  // On timeout, call shutdown, and shuting_down start returning true
  EXPECT_CALL(mgr, DoShutdown());

  EXPECT_FALSE(mgr.shutting_down());
  timeout_handler(boost::system::error_code());
  EXPECT_TRUE(mgr.shutting_down());

  // Calling Shutdown again has no effect
  EXPECT_FALSE(mgr.Shutdown());
}

TEST(ShutdownMgrTest, CancelOnShutdown) {
  // On construct, not inited
  IOService io_service;
  MockIAsioPolicy asio_policy;
  EXPECT_CALL(asio_policy, io_service())
      .WillRepeatedly(Return(&io_service));
  DeadlineTimer* timer = new DeadlineTimer(io_service);
  EXPECT_CALL(asio_policy, MakeDeadlineTimer())
      .WillOnce(Return(timer));
  TestShutdownMgr mgr;
  mgr.SetAsioPolicy(&asio_policy);

  // Init
  EXPECT_CALL(asio_policy, SetDeadlineTimer(timer, 10, _));
  EXPECT_CALL(mgr, DoInit())
      .WillOnce(Return(true));

  EXPECT_TRUE(mgr.Init(10));

  EXPECT_CALL(asio_policy, SetDeadlineTimer(timer, 0, _));  // before destroy
}

// Also override Init()
class TestShutdownMgr2 : public BaseShutdownMgr {
 public:
  MOCK_METHOD1(Init, bool(double timeout));
  MOCK_METHOD0(DoInit, bool());
  MOCK_METHOD0(DoShutdown, bool());
};

TEST(ShutdownMgrTest, SignalTrigger) {
  // On construct, not inited
  IOService io_service;
  MockIAsioPolicy asio_policy;
  EXPECT_CALL(asio_policy, io_service())
      .WillRepeatedly(Return(&io_service));
  SignalSet* signal_set = new SignalSet(io_service);
  EXPECT_CALL(asio_policy, MakeSignalSet())
      .WillOnce(Return(signal_set));

  TestShutdownMgr2 mgr;
  mgr.SetAsioPolicy(&asio_policy);

  // On SetupSignals call, register callback
  SignalHandler handler;
  EXPECT_CALL(asio_policy, SetSignalHandler(signal_set, Pointee(SIGUSR1), 1, _))
      .WillOnce(SaveArg<3>(&handler));

  int signal = SIGUSR1;
  mgr.SetupSignals(&signal, 1);

  // Nothing happen if cancelled
  handler(boost::asio::error::operation_aborted, SIGUSR1);

  // Trigger Init() call if handler called normally
  EXPECT_CALL(mgr, Init(120));

  handler(boost::system::error_code(), SIGUSR1);

  // Cancel before destroy
  EXPECT_CALL(asio_policy, SetSignalHandler(signal_set, _, 0, _));
}

}  // namespace
}  // namespace cpfs
