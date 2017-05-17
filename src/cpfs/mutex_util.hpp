#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define and implement various macros to perform mutex locking.
 */

#include <boost/date_time/posix_time/posix_time.hpp>  // IWYU pragma: export
#include <boost/preprocessor/cat.hpp>  // IWYU pragma: export
#include <boost/preprocessor/stringize.hpp>  // IWYU pragma: export
#include <boost/thread/lock_guard.hpp>  // IWYU pragma: export
#include <boost/thread/mutex.hpp>  // IWYU pragma: export
#include <boost/typeof/typeof.hpp>

#include "logger.hpp"
#ifdef CHECK_MUTEX
#include "mutex_checker.hpp"
#endif
#include "profile_util.hpp"  // IWYU pragma: export

namespace cpfs {

#ifdef CHECK_MUTEX
/** Mutex type to use */
#define MUTEX_TYPE cpfs::CheckingMutex
/** How to initialize mutex */
#define MUTEX_INIT "mutex@" __FILE__ ":" BOOST_PP_STRINGIZE(__LINE__)
/** How to declare mutex */
#define MUTEX_DECL(name) MUTEX_TYPE name(MUTEX_INIT)
/** Perform condition waiting on mutex */
#define MUTEX_WAIT(l, c) cpfs::CheckingMutexCondWait(&l, &c)
/** Perform timed condition waiting on mutex */
#define MUTEX_TIMED_WAIT(l, c, d) cpfs::CheckingMutexCondTimedWait(&l, &c, d)
#else
/** Mutex type to use */
#define MUTEX_TYPE boost::mutex
/** How to initialize mutex */
#define MUTEX_INIT
/** How to declare mutex */
#define MUTEX_DECL(name) MUTEX_TYPE name
/** Perform condition waiting on mutex */
#define MUTEX_WAIT(l, c) (c).wait(l)
/** Perform timed condition waiting on mutex */
#define MUTEX_TIMED_WAIT(l, c, d) (c).timed_wait(l, d)
#endif

/**
 * Set lock info before mutex locking if needed
 */
template<typename TMutex>
void SetNextLockInfo(TMutex* mutex, const char* info) {}

#ifdef CHECK_MUTEX
/**
 * Set lock info before mutex locking if needed
 */
template<>
inline void SetNextLockInfo<CheckingMutex>(CheckingMutex* mutex,
                                           const char* info) {
  mutex->SetNextLockInfo(info);
}
#endif

/**
 * Log locking actions.
 */
class LockLogger {
 public:
  /**
   * @param filename Should be passed __FILE__
   *
   * @param linenum Should be passed stringized __LINE__
   */
  LockLogger(const char* filename, const char* linenum) :
        filename_(filename), linenum_(linenum) {
    LOG(debug, Lock, "Lock at ", filename_, " line ", linenum_);
  }
  ~LockLogger() {
    LOG(debug, Lock, "Unlock at ", filename_, " line ", linenum_);
  }
 private:
  const char* filename_; /**< Source file filename using the guard */
  const char* linenum_; /**< Source file line number using the guard */
};

/**
 * Log a locking message, and an unlocking message on scope exit
 */
#define MUTEX_LOCK_SCOPE_LOG()                                          \
  LockLogger __attribute__((unused))                                    \
  BOOST_PP_CAT(mutex_util_log_guard_, __LINE__)                         \
      (__FILE__, BOOST_PP_STRINGIZE(__LINE__))

/**
 * Prelock work.  Record the starting time of mutex acquisition.
 *
 * @param id An id for generating the variable needed
 */
#define MUTEX_LOCK_PRELOCK(id)                                          \
  StopWatch BOOST_PP_CAT(mutex_lock_watch_, id)(false);                 \
  if (pantheios_fe_isSeverityLogged(0, PLEVEL(debug, Performance), 0))  \
    BOOST_PP_CAT(mutex_lock_watch_, id).Start()

/**
 * Postlock work.  Determine the time needed for mutex acquisition,
 * and log message for long locks.
 *
 * @param id An id for generating the variable needed
 */
#define MUTEX_LOCK_POSTLOCK(id)                                         \
  if (pantheios_fe_isSeverityLogged(0, PLEVEL(debug, Performance), 0)) { \
    uint64_t BOOST_PP_CAT(mutex_lock_dur_, id) =                        \
        BOOST_PP_CAT(mutex_lock_watch_, id).ReadTimerMicro();           \
    if (BOOST_PP_CAT(mutex_lock_dur_, id) > 1000U)                      \
      LOG(debug, Performance,                                           \
          "Lock(" __FILE__ ", " BOOST_PP_STRINGIZE(id) ") took ",       \
          PINT(BOOST_PP_CAT(mutex_lock_dur_, id)), " us");              \
  }

/**
 * Call MUTEX_LOCK_PRELOCK and MUTEX_LOCK_POSTLOCK around some statements
 *
 * @param stmt The statements
 */
#define MUTEX_LOCK_WRAP(stmt)                                           \
  MUTEX_LOCK_PRELOCK(__LINE__);                                         \
  stmt;                                                                 \
  MUTEX_LOCK_POSTLOCK(__LINE__)

/**
 * Create a lock variable for a Boost.Thread mutex.
 *
 * @param lock_type The type of lock
 *
 * @param mutex The mutex
 *
 * @param lock_var The name of the lock variable
 */
#define MUTEX_LOCK(lock_type, mutex, lock_var)                          \
  MUTEX_LOCK_SCOPE_LOG();                                               \
  BOOST_TYPEOF(mutex)& BOOST_PP_CAT(__MU_MUTEX, __LINE__) = mutex;      \
  SetNextLockInfo(&BOOST_PP_CAT(__MU_MUTEX, __LINE__),                  \
                  "lock@" __FILE__ ":" BOOST_PP_STRINGIZE(__LINE__));   \
  MUTEX_LOCK_WRAP(lock_type<BOOST_TYPEOF(mutex)>                        \
                  lock_var(BOOST_PP_CAT(__MU_MUTEX, __LINE__)))

/**
 * Create a lock guard for a Boost.Thread mutex.
 *
 * @param mutex The mutex
 */
#define MUTEX_LOCK_GUARD(mutex)                                         \
  MUTEX_LOCK(boost::lock_guard, mutex,                                  \
             BOOST_PP_CAT(mutex_util_guard_, __LINE__))

}  // namespace cpfs
