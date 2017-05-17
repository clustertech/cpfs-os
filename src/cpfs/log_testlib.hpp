#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Logging facilities interface for unit tests.
 */

#include <cstddef>

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction

#include <pantheios/pantheios.hpp>

namespace cpfs {

/**
 * A logging callback, to be called by the unittest logging backend
 * when LogRoute is active.
 */
typedef boost::function<void(int,
                             const PAN_CHAR_T*,
                             std::size_t cchEntry)> LogCallback;

/**
 * A mocking class implementing LogCallback.
 */
class MockLogCallback
    : public ::testing::MockFunction<void(int, const PAN_CHAR_T*,
                                          std::size_t)> {
 public:
  /**
   * Get the callback to be passed to LogRoute.
   */
  LogCallback GetLogCallback() {
    return boost::bind(&MockLogCallback::Call, this, _1, _2, _3);
  }
};

/**
 * Route logging to a LogFunc.  This is a RAII class, when the object
 * is destroyed the original routing is restored.
 */
class LogRoute {
 public:
  /**
   * @param callback The logging function to use.
   */
  explicit LogRoute(LogCallback callback);
  ~LogRoute();
 private:
  LogCallback old_callback;
};

}  // namespace cpfs
