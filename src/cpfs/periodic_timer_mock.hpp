#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/bind.hpp>

#include "mock_helper.hpp"
#include "periodic_timer.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Reset, void, (double)))                                             \
  ((Stop, void,))                                                       \
  ((OnTimeout, void, (PeriodicTimerCallback)))                          \
  ((Duplicate, boost::shared_ptr<IPeriodicTimer>, (IOService*)))

class MockIPeriodicTimer : public IPeriodicTimer {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Make, boost::shared_ptr<IPeriodicTimer>, (IOService*)(double)))

class MockPeriodicTimerMaker {
  MAKE_MOCK_METHODS(OBJ_METHODS);

  PeriodicTimerMaker GetMaker() {
    return boost::bind(&MockPeriodicTimerMaker::Make, this, _1, _2);
  }
};

#undef OBJ_METHODS

}  // namespace cpfs
