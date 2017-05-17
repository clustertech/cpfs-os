/* Copyright 2013 ClusterTech Ltd */
#include "event.hpp"

#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>

#include <gtest/gtest.h>

#include "mock_actions.hpp"

namespace cpfs {
namespace {

void InvokeEvent(Event* ev, bool* completed) {
  ev->Wait();
  *completed = true;
}

TEST(EventTest, API) {
  Event ev1;
  ev1.Invoke();
  ev1.Invoke();
  ev1.Wait();
  ev1.Wait();

  bool completed = false;
  Event ev2;
  boost::thread th(boost::bind(InvokeEvent, &ev2, &completed));
  Sleep(0.05)();
  EXPECT_FALSE(completed);
  ev2.Invoke();
  Sleep(0.05)();
  EXPECT_TRUE(completed);
}

}  // namespace
}  // namespace cpfs
