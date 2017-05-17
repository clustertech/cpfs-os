/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for unit tests.
 */
#include <unistd.h>

#include <csignal>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "unittest_util.hpp"

namespace cpfs {
namespace main {
namespace {

/**
 * Reset signal handlers.
 */
void ResetSigHandlers() {
  for (unsigned i = 0; i < kNumUnittestDeferredSigs; ++i)
    std::signal(kUnittestDeferredSigNums[i], SIG_DFL);
}

/**
 * Signal caught by handler.
 */
volatile sig_atomic_t signal_caught = 0;

/**
 * Signal handler to just mark that a signal has been caught, while
 * unsetting all handlers so that further attempts will actually kill
 * the program.
 *
 * @param signum The signal number of the signal caught
 */
extern "C" void DeferringSignalHandler(int signum) {
  signal_caught = signum;
  ResetSigHandlers();
}

/**
 * Set up signal handlers to defer signal delivery.
 */
void SetSigHandlers() {
  for (unsigned i = 0; i < kNumUnittestDeferredSigs; ++i)
    std::signal(kUnittestDeferredSigNums[i], DeferringSignalHandler);
}

/**
 * A gtest test event listener to manage the marking signal handler.
 */
class SignalDeferringListener : public ::testing::EmptyTestEventListener {
 public:
  /**
   * Install the signal handler.
   *
   * @param test_info Unused
   */
  void OnTestStart(const ::testing::TestInfo& test_info) {
    (void) test_info;
    signal_caught = 0;
    SetSigHandlers();
  }

  /**
   * Uninstall the signal handler.
   *
   * @param test_info Unused
   */
  void OnTestEnd(const ::testing::TestInfo& test_info) {
    (void) test_info;
    ResetSigHandlers();
    if (signal_caught)
      kill(getpid(), signal_caught);
  }
};

}  // namespace
}  // namespace main
}  // namespace cpfs

/**
 * Main entry point.
 */
int main(int argc, char **argv) {
  ::testing::InitGoogleMock(&argc, argv);
  ::testing::UnitTest::GetInstance()->
        listeners().Append(new cpfs::main::SignalDeferringListener());
  return RUN_ALL_TESTS();
}
