/* Copyright 2013 ClusterTech Ltd */
#include "concurrent_queue.hpp"

#include <gtest/gtest.h>

namespace cpfs {
namespace {

TEST(ConcurrentQueueTest, Base) {
  ConcurrentQueue<int> int_queue;
  int_queue.enqueue(new int(42));
  boost::scoped_ptr<int> obtained(int_queue.dequeue());
  ASSERT_TRUE(obtained);
  EXPECT_EQ(42, *obtained);
  // Dequeue from empty
  EXPECT_FALSE(int_queue.try_dequeue());
  int_queue.enqueue(new int(43));
  boost::scoped_ptr<int> try_obtained(int_queue.try_dequeue());
  ASSERT_TRUE(try_obtained);
  EXPECT_EQ(43, *try_obtained);
}

}  // namespace
}  // namespace cpfs
