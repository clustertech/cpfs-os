/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IStateMgr.
 */

#include "server/ms/state_mgr_impl.hpp"

#include <inttypes.h>
#include <stdint.h>

#include <sys/xattr.h>

#include <cstdio>
#include <cstdlib>
#include <list>
#include <stdexcept>
#include <string>

#include <boost/function.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "server/ms/state_mgr.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implementation for IHACounter interface.
 */
class HACounter : public IHACounter {
 public:
  /**
   * Create the HACounter
   *
   * @param data_path The meta directory
   *
   * @param role The role of MS: MS1 / MS2
   */
  HACounter(const std::string& data_path, const std::string& role)
      : data_path_(data_path), is_active_(false), confirm_pending_(false) {
    char buf[21] = {'\0'};
    int len =
        lgetxattr(data_path_.c_str(), "user.ha.counter", buf, sizeof(buf));
    if (len <= 0) {
      counter_ = (role == "MS1") ? 1 : 0;
      PersistCount(counter_);
    } else {
      counter_ = std::strtoull(buf, 0, 10);
    }
  }

  bool Elect(uint64_t peer_ha_counter, bool peer_active) {
    if (!is_active_) {
      if (peer_active) {
        is_active_ = false;
      } else if (counter_ == peer_ha_counter) {
        LOG(error, Server, "HA tie occurs. Server role not elected, counter ",
            PINT(counter_));
        return false;
      } else {
        is_active_ = peer_ha_counter < counter_;
      }
    }
    if (!is_active_)
      counter_ = peer_ha_counter - 1;
    PersistCount(counter_);
    return is_active_;
  }

  bool IsActive() const {
    return is_active_;
  }

  void SetActive() {
    is_active_ = true;
    confirm_pending_ = true;
  }

  void ConfirmActive() {
    if (confirm_pending_) {
      confirm_pending_ = false;
      counter_ += 2;
      PersistCount(counter_);
    }
  }

  uint64_t GetCount() const {
    return counter_;
  }

  void PersistCount(uint64_t ha_counter) const {
    if (ha_counter == kPersistActualHACount)
      ha_counter = counter_;
    char buf[21];
    int ret = std::snprintf(buf, sizeof(buf), "%" PRIu64, ha_counter);
    if (lsetxattr(data_path_.c_str(), "user.ha.counter", buf, ret, 0) < 0)
      throw std::runtime_error("Cannot persists HA counter");
  }

 private:
  std::string data_path_; /**< The meta directory path */
  uint64_t counter_; /**< HA counter loaded from xattr */
  bool is_active_; /**< Whether this server is active */
  bool confirm_pending_; /**< Whether confirmation is pending */
};

/**
 * Actual implementation of StateMgr
 */
class StateMgr : public IStateMgr {
 public:
  /**
   * Create the StateMgr
   */
  StateMgr() : data_mutex_(MUTEX_INIT), state_(kStateHANotReady) {}

  bool SwitchState(MSState state) {
    MSStateChangeCallback callback;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      if (state_ == kStateShuttingDown)
        return false;
      if (IsStickyState(state_) && IsStickyState(state)) {
        LOG(notice, Server, "Server state change defer: ", ToStr(state));
        pending_states_.push_back(state);
        return false;
      }
      if (pending_states_.size()) {
        LOG(notice, Server, "Server state change overridden: ", ToStr(state));
        state = pending_states_.front();
        pending_states_.pop_front();
      }
      state_ = state;
      LOG(notice, Server, "Server changed to state: ", ToStr(state_));
      if (state_callbacks_.find(state_) != state_callbacks_.end()) {
        callback = state_callbacks_[state_];
        state_callbacks_.erase(state_);
      }
    }
    if (callback)
      callback();
    return true;
  }

  void OnState(MSState state, MSStateChangeCallback callback) {
    if (state == GetState()) {
      callback();
    } else {
      MUTEX_LOCK_GUARD(data_mutex_);
      state_callbacks_[state] = callback;
    }
  }

  MSState GetState() const {
    MUTEX_LOCK_GUARD(data_mutex_);
    return state_;
  }

 private:
  mutable MUTEX_TYPE data_mutex_; /**< Protect states below */
  MSState state_; /**< Current server status */
  std::list<MSState> pending_states_; /**< Sticky states pending */
  /** State change callback */
  boost::unordered_map<MSState, MSStateChangeCallback> state_callbacks_;

  /**
   * Whether the specified state is an sticky state
   */
  static bool IsStickyState(MSState state) {
    return state == kStateFailover || state == kStateResync ||
           state == kStateShuttingDown;
  }
};

}  // namespace

IHACounter* MakeHACounter(const std::string& data_path,
                          const std::string& role) {
  return new HACounter(data_path, role);
}

IStateMgr* MakeStateMgr() {
  return new StateMgr;
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
