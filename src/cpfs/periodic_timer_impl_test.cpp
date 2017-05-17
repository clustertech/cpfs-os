/* Copyright 2013 ClusterTech Ltd */
#include "periodic_timer_impl.hpp"

#include <cstddef>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "mock_actions.hpp"
#include "periodic_timer.hpp"

using ::testing::Mock;
using ::testing::Return;

namespace cpfs {
namespace {

class TimerCallback {
 public:
  MOCK_METHOD0(OnTimeout, bool());
};

class PeriodicTimerTest : public ::testing::Test {
 protected:
  IOService io_service_;
  TimerCallback callback_;
  boost::shared_ptr<IPeriodicTimer> timer_;

  PeriodicTimerTest() : timer_(kPeriodicTimerMaker(&io_service_, 0.2)) {
    timer_->OnTimeout(boost::bind(&TimerCallback::OnTimeout, &callback_));
  };
};

TEST_F(PeriodicTimerTest, ResetAndRepeat) {
  boost::thread th(boost::bind(&IOService::run, &io_service_));
  Sleep(0.1)();
  timer_->Reset();
  // Try resetting with tolerance when already resetted: no effect
  timer_->Reset(0.9);
  Sleep(0.15)();
  // Timeout occurs and callback invoked twice
  for (int i = 0; i < 2; ++i) {
    EXPECT_CALL(callback_, OnTimeout())
        .WillOnce(Return(true));
    Sleep(0.2)();
    Mock::VerifyAndClear(&callback_);
  }
  timer_->Stop();
  th.join();
}

TEST_F(PeriodicTimerTest, Destruct) {
  timer_.reset();
  // Timer destructed and stopped, io service has no pending operation
  io_service_.run();
}

TEST_F(PeriodicTimerTest, CallbackTimerCancel) {
  boost::thread th(boost::bind(&IOService::run, &io_service_));
  EXPECT_CALL(callback_, OnTimeout())
      .WillOnce(Return(false));
  Sleep(0.5)();
  th.join();
}

TEST_F(PeriodicTimerTest, Duplicate) {
  IOService io_service2;
  boost::shared_ptr<IPeriodicTimer> timer2 = timer_->Duplicate(&io_service2);
  boost::thread th(boost::bind(&IOService::run, &io_service2));
  EXPECT_CALL(callback_, OnTimeout())
      .WillOnce(Return(false));
  Sleep(0.5)();
  th.join();
}

}  // namespace
}  // namespace cpfs
