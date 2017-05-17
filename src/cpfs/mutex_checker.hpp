#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define and implement a simple checker for mutex lock ordering.
 */

// This library does not use the mutex_util.hpp facility for locking,
// because we are defining how mutex_util.hpp should do locking.

#include <string>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/scope_exit.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

namespace cpfs {

/**
 * Check mutex lock ordering correctness.  This class is supposed to
 * be notified by the caller code with the various Notify* functions,
 * when a mutex is created, locked and unlocked.  Each mutex has a
 * "tag" associated, and mutexes with the same tag should have the
 * same location in the mutex hierarchy.  Whenever a lock is made, it
 * attempts to add the edge (most recent active lock held, the new
 * lock) to the dependency graph, and ensure that there is no cycle
 * formed.  If a cycle does form, the edge is not added, but instead a
 * warning is logged giving details about the lock cycle that it
 * generates.  The lock location is made easier to identify by
 * SetNextLockInfo, which should be the location when the lock
 * happens.  All the above calls is supposed to be added by macros.
 */
class IMutexChecker {
 public:
  virtual ~IMutexChecker() {}

  /**
   * Notify the creation of a mutex
   *
   * @param tag The mutex tag
   *
   * @return The tag ID
   */
  virtual unsigned NotifyMutex(std::string tag) = 0;

  /**
   * Set lock info for the upcoming NotifyLock().
   *
   * @param lock_info The lock info.  If empty, erase the set info
   * (needed if a NotifyLock() is eventually found to be unneeded)
   */
  virtual void SetNextLockInfo(const std::string& lock_info) = 0;

  /**
   * Notify that a mutex is locked for the currently running thread.
   *
   * @param tag_id The ID of the mutex to be locked
   *
   * @param is_try Whether the operation is a try_lock.  This argument
   * is currently unused
   */
  virtual void NotifyLock(unsigned tag_id, bool is_try) = 0;

  /**
   * Notify that a mutex is unlocked for the currently running thread.
   *
   * @param tag_id The ID of the mutex to be unlocked
   */
  virtual void NotifyUnlock(unsigned tag_id) = 0;
};

/**
 * @return A new mutex checker
 */
IMutexChecker* MakeMutexChecker();

/**
 * @return The default mutex checker
 */
IMutexChecker* GetDefaultMutexChecker();

/**
 * A wrapper of mutex which performs lock order checking.
 */
class CheckingMutex : public boost::mutex {
 public:
  /**
   * Create a mutex.
   */
  explicit CheckingMutex(std::string tag, IMutexChecker* checker = 0)
      : checker_(checker ? checker : GetDefaultMutexChecker()),
        tag_id_(checker_->NotifyMutex(tag)) {}

  /**
   * Set lock info of the next lock() or try_lock() operation.  The
   * lock info is used for showing additional information in case a
   * lock order problem needs to be logged.
   */
  void SetNextLockInfo(const std::string& lock_info) {
    checker_->SetNextLockInfo(lock_info);
  }

  /**
   * Lock a mutex, notifying the checker.
   */
  void lock() {
    checker_->NotifyLock(tag_id_, false);
    boost::mutex::lock();
  }

  /**
   * Try lock a mutex, notifying the checker if successful.
   */
  bool try_lock() {
    if (!boost::mutex::try_lock()) {
      checker_->SetNextLockInfo("");
      return false;
    }
    // Notify after-the-fact
    checker_->NotifyLock(tag_id_, true);
    return true;
  }

  /**
   * Unlock a mutex, notifying the checker.
   */
  void unlock() {
    // If the following two lines are swapped we get segfault in FC.
    // Look like compiler bug.
    checker_->NotifyUnlock(tag_id_);
    boost::mutex::unlock();
  }

 private:
  IMutexChecker* checker_; /**< Checker to be used */
  unsigned tag_id_; /**< ID of the mutex */
};

/**
 * Perform condition waiting.
 *
 * @param lock The unique lock held
 *
 * @param cond The condition to wait for
 */
inline void CheckingMutexCondWait(boost::unique_lock<CheckingMutex>* lock,
                                  boost::condition_variable* cond) {
  CheckingMutex* mutex = lock->mutex();
  // Condition variable recognize boost::unique_lock<boost::mutex>
  // only, so we have to fake one for it.
  boost::unique_lock<boost::mutex> my_lock(*mutex, boost::adopt_lock_t());
  BOOST_SCOPE_EXIT(&my_lock) {
    my_lock.release();
  } BOOST_SCOPE_EXIT_END;
  cond->wait(my_lock);
}

/**
 * Perform condition waiting, with timeout.
 *
 * @param lock The unique lock held
 *
 * @param cond The condition to wait for
 *
 * @param max_wait Maximum amount of time to wait
 */
inline bool CheckingMutexCondTimedWait(
    boost::unique_lock<CheckingMutex>* lock,
    boost::condition_variable* cond,
    const boost::posix_time::time_duration& max_wait) {
  CheckingMutex* mutex = lock->mutex();
  boost::unique_lock<boost::mutex> my_lock(*mutex, boost::adopt_lock_t());
  BOOST_SCOPE_EXIT(&my_lock) {
    my_lock.release();
  } BOOST_SCOPE_EXIT_END;
  return cond->timed_wait(my_lock, max_wait);
}

}  // namespace cpfs
