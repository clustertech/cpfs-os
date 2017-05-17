/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/inode_mutex.hpp"

#include <gtest/gtest.h>

namespace cpfs {
namespace server {
namespace ms {
namespace {

TEST(InodeMutexTest, Basic) {
  InodeMutexArray mutex_array(10);
  mutex_array.Lock(1024);
  mutex_array.Unlock(1024);
}

TEST(InodeMutexTest, GuardOne) {
  InodeMutexArray mutex_array(10);
  InodeMutexGuard g(&mutex_array, 5);
}

TEST(InodeMutexTest, GuardOrder1) {
  InodeMutexArray mutex_array(10);
  InodeMutexGuard g(&mutex_array, 2, 5);
}

TEST(InodeMutexTest, GuardOrder2) {
  InodeMutexArray mutex_array(10);
  InodeMutexGuard g(&mutex_array, 2, 12);
}

TEST(InodeMutexTest, GuardOrder3) {
  InodeMutexArray mutex_array(10);
  InodeMutexGuard g(&mutex_array, 5, 2);
}

TEST(InodeMutexTest, GuardThree) {
  InodeMutexArray mutex_array(10);
  InodeMutexGuard g(&mutex_array, 5, 2, 12);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
