/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of TimeKeeper.  TimeKeeper keeps the time last
 * update epoch time and persist to disk after an configurable
 * interval, so that the time value could be restored later. The
 * implementation is thread-safe.
 */

#include "time_keeper_impl.hpp"

#include <inttypes.h>
#include <stdint.h>

#include <sys/xattr.h>

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <string>

#include "logger.hpp"
#include "mutex_util.hpp"
#include "time_keeper.hpp"

namespace cpfs {

namespace {

/**
 * Actual implementation of TimeKeeper
 */
class TimeKeeper : public ITimeKeeper {
 public:
  /**
   * Construct the TimeKeeper
   *
   * @param path Path to persist the time
   *
   * @param attr The xattr name for time persist
   *
   * @param min_interval Minimal interval to persist time
   */
  TimeKeeper(const std::string& path,
             const std::string& attr,
             uint64_t min_interval);

  ~TimeKeeper();

  void Start();
  void Stop();
  bool Update();
  uint64_t GetLastUpdate() const;

 private:
  std::string path_;
  std::string attr_;
  uint64_t min_interval_;
  mutable MUTEX_TYPE data_mutex_;  // Protect everything below
  uint64_t last_update_;
  uint64_t last_persist_;
  bool started_;

  void PersistTime_() {
    char buf[21];
    int ret = std::snprintf(buf, sizeof(buf), "%" PRIu64, last_update_);
    lsetxattr(path_.c_str(), attr_.c_str(), buf, ret, 0);
    last_persist_ = last_update_;
  }
};

TimeKeeper::TimeKeeper(const std::string& path,
                       const std::string& attr,
                       uint64_t min_interval)
    : path_(path), attr_(attr), min_interval_(min_interval),
      data_mutex_(MUTEX_INIT), last_persist_(0), started_(false) {
  char buf[21];
  int len = lgetxattr(path_.c_str(), attr_.c_str(), buf, sizeof(buf));
  if (len < 0) {
    last_update_ = 0;
    return;
  }
  char* endptr;
  buf[len] = '\0';
  last_update_ = std::strtoull(buf, &endptr, 10);
}

TimeKeeper::~TimeKeeper() {
  PersistTime_();
}

void TimeKeeper::Start() {
  MUTEX_LOCK_GUARD(data_mutex_);
  started_ = true;
}

void TimeKeeper::Stop() {
  MUTEX_LOCK_GUARD(data_mutex_);
  started_ = false;
}

bool TimeKeeper::Update() {
  boost::unique_lock<MUTEX_TYPE> repl_lock(
      data_mutex_, boost::try_to_lock_t());
  if (repl_lock) {
    uint64_t now = std::time(0);
    if (now < last_update_)
      LOG(warning, Server, "System clock has been set back");
    else if (!started_)
      return false;

    last_update_ = now;
    if (last_update_ < last_persist_ ||
        (last_update_ - last_persist_) >= min_interval_)
      PersistTime_();
  }  // if can't get lock, somebody has updated already
  return true;
}

uint64_t TimeKeeper::GetLastUpdate() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return last_update_;
}

}  // namespace

ITimeKeeper* MakeTimeKeeper(const std::string& path,
                            const std::string& attr,
                            uint64_t min_interval) {
  return new TimeKeeper(path, attr, min_interval);
}

}  // namespace cpfs
