/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define and implement a simple checker for mutex lock ordering.
 */

// This library does not use the logger.hpp facility for locking,
// because we are defining how logger.hpp should do locking.

#include "mutex_checker.hpp"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "util.hpp"

namespace cpfs {

namespace {

using testing::_;
using testing::Return;
using testing::StrEq;

class MutexChecker : public ::testing::Test {
 protected:
  boost::scoped_ptr<IMutexChecker> checker_;
  MockLogCallback callback_;
  LogRoute route_;

  MutexChecker()
      : checker_(MakeMutexChecker()),
        route_(callback_.GetLogCallback()) {}
};

TEST_F(MutexChecker, Default) {
  EXPECT_CALL(callback_,
              Call(PLEVEL(notice, Lock),
                   StrEq("Mutex lock ordering checker enabled"),
                   _))
      .WillRepeatedly(Return());
  EXPECT_TRUE(GetDefaultMutexChecker());
  EXPECT_EQ(GetDefaultMutexChecker(), GetDefaultMutexChecker());
}

TEST_F(MutexChecker, Good) {
  CheckingMutex mutex1("Mutex1", checker_.get());
  CheckingMutex mutex2("Mutex2", checker_.get());
  mutex1.lock();
  mutex2.lock();
  mutex2.unlock();
  mutex1.unlock();

  mutex1.lock();
  mutex2.lock();
  mutex1.unlock();
  mutex2.unlock();
}

TEST_F(MutexChecker, Good3) {
  CheckingMutex mutex1("Mutex1", checker_.get());
  CheckingMutex mutex2("Mutex2", checker_.get());
  CheckingMutex mutex3("Mutex3", checker_.get());
  mutex1.lock();
  mutex2.lock();
  mutex2.unlock();
  mutex1.unlock();

  mutex2.lock();
  mutex3.lock();
  mutex2.unlock();
  mutex3.unlock();

  mutex1.lock();
  mutex3.lock();
  mutex1.unlock();
  mutex3.unlock();
}

TEST_F(MutexChecker, Cycle2) {
  CheckingMutex mutex1("Mutex1", checker_.get());
  CheckingMutex mutex2("Mutex2", checker_.get());
  mutex1.SetNextLockInfo("Lock1-1");
  mutex1.lock();
  mutex2.SetNextLockInfo("Lock2-1");
  mutex2.lock();
  mutex2.unlock();
  mutex1.unlock();

  mutex2.SetNextLockInfo("Lock2-2");
  mutex2.lock();
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("Lock problem found: locking Mutex1 (Lock1-2) "
                         "when holding Mutex2 (Lock2-2)"),
                   _));
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("  Mutex1 (Lock1-1) -> Mutex2 (Lock2-1)"),
                   _));
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("  Adding new dependency leads to a cycle."),
                   _));
  mutex1.SetNextLockInfo("Lock1-2");
  mutex1.lock();
  mutex2.unlock();
  mutex1.unlock();
}

TEST_F(MutexChecker, Cycle3) {
  CheckingMutex mutex1("Mutex1", checker_.get());
  CheckingMutex mutex2("Mutex2", checker_.get());
  CheckingMutex mutex3("Mutex3", checker_.get());
  mutex1.SetNextLockInfo("Lock1-1");
  mutex1.lock();
  mutex2.SetNextLockInfo("Lock2-1");
  mutex2.lock();
  mutex2.unlock();
  mutex1.unlock();

  mutex2.SetNextLockInfo("Lock2-2");
  mutex2.lock();
  mutex3.SetNextLockInfo("Lock3-2");
  mutex3.lock();
  mutex3.unlock();
  mutex2.unlock();

  mutex3.SetNextLockInfo("Lock3-3");
  mutex3.lock();
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("Lock problem found: locking Mutex1 (Lock1-3) "
                         "when holding Mutex3 (Lock3-3)"),
                   _));
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("  Mutex1 (Lock1-1) -> Mutex2 (Lock2-1)"),
                   _));
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("  Mutex2 (Lock2-2) -> Mutex3 (Lock3-2)"),
                   _));
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("  Adding new dependency leads to a cycle."),
                   _));
  mutex1.SetNextLockInfo("Lock1-3");
  mutex1.lock();
  mutex3.unlock();
  mutex1.unlock();
}

TEST_F(MutexChecker, RecursiveGood) {
  CheckingMutex mutex1a("Mutex1", checker_.get());
  CheckingMutex mutex1b("Mutex1", checker_.get());
  mutex1a.lock();
  mutex1b.lock();
  mutex1b.unlock();
  mutex1a.unlock();
}

TEST_F(MutexChecker, RecursiveBad) {
  CheckingMutex mutex1a("Mutex1", checker_.get());
  CheckingMutex mutex1b("Mutex1", checker_.get());
  CheckingMutex mutex2("Mutex2", checker_.get());
  mutex1a.SetNextLockInfo("Lock1-1");
  mutex1a.lock();
  mutex2.SetNextLockInfo("Lock2-1");
  mutex2.lock();
  EXPECT_CALL(callback_,
              Call(PLEVEL(warning, Lock),
                   StrEq("Non-homogenious recursive lock: "
                         "to lock Mutex1 (Lock1-2), "
                         "last locked Mutex2 (Lock2-1)"),
                   _));
  mutex1b.SetNextLockInfo("Lock1-2");
  mutex1b.lock();
  mutex1b.unlock();
  mutex2.unlock();
  mutex1a.unlock();
}

TEST_F(MutexChecker, TryLock) {
  CheckingMutex mutex("Mutex", checker_.get());
  EXPECT_TRUE(mutex.try_lock());
  EXPECT_FALSE(mutex.try_lock());
  mutex.unlock();
}

void SleepAndWait(CheckingMutex* mutex, boost::condition_variable* cond) {
  Sleep(0.5)();
  mutex->lock();
  cond->notify_all();
  mutex->unlock();
}

TEST_F(MutexChecker, Condition) {
  CheckingMutex mutex("Mutex1", checker_.get());
  boost::condition_variable cond;
  boost::scoped_ptr<boost::thread> th;
  {
    boost::unique_lock<CheckingMutex> lock(mutex);
    th.reset(new boost::thread(SleepAndWait, &mutex, &cond));
    CheckingMutexCondWait(&lock, &cond);
  }
  th->join();
  {
    boost::unique_lock<CheckingMutex> lock(mutex);
  }
}

TEST_F(MutexChecker, ConditionTimed) {
  CheckingMutex mutex("Mutex", checker_.get());
  boost::condition_variable cond;
  boost::unique_lock<CheckingMutex> lock(mutex);
  EXPECT_FALSE(CheckingMutexCondTimedWait(&lock, &cond, ToTimeDuration(0.1)));
}

}  // namespace

}  // namespace cpfs
