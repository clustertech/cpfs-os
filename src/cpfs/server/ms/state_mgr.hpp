#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define StateMgr and HACounter.
 * StateMgr manages server state change for meta server.
 * HACounter performs active / standby MS election.
 */

#include <stdint.h>

#include <limits>

#include <boost/function.hpp>

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Constant used in IHACounter::PersistCount().
 */
const uint64_t kPersistActualHACount = std::numeric_limits<uint64_t>::max();

/**
 * HA counter for active / standby election. The value is retrieved from and
 * persisted to disk
 */
class IHACounter {
 public:
  virtual ~IHACounter() {}
  /**
   * Perform election.  If elected it will become pending
   * confirmation.
   *
   * @param peer_ha_counter Peer counter value
   *
   * @param peer_active Whether peer is already active (thus always win)
   *
   * @return True if current server is elected as Active
   */
  virtual bool Elect(uint64_t peer_ha_counter, bool peer_active) = 0;
  /**
   * Determine whether current server is Active
   */
  virtual bool IsActive() const = 0;
  /**
   * Set the server as active, and set the counter to be pending
   * confirmation.
   */
  virtual void SetActive() = 0;
  /**
   * Confirm that the server is active.  Advance the count if it is
   * pending confirmation.
   */
  virtual void ConfirmActive() = 0;
  /**
   * Get HA count
   *
   * @return The count
   */
  virtual uint64_t GetCount() const = 0;
  /**
   * Persist the count, perhaps with a value different from the actual
   * count.
   *
   * @param ha_counter The count to persist.  If the special value
   * kPersistActualHACount (default) is used, persist the actual
   * counter.
   */
  virtual void PersistCount(
      uint64_t ha_counter = kPersistActualHACount) const = 0;
};

/** Callback for state change */
typedef boost::function<void()> MSStateChangeCallback;

/**
 * StateMgr manages the HA state of the current server.
 */
class IStateMgr {
 public:
  virtual ~IStateMgr() {}

  /**
   * Switch to another server state. The server state can only be
   * changed by this call, although the actual state after the call
   * may not be the specified state. In particular, if the current
   * state is a sticky state, calling SwitchState() to another sticky
   * state will be executed only when SwitchState() to non-sticky
   * state is later triggered.  In this case, the later non-sticky
   * state change will not be executed.
   *
   * @param state The state
   *
   * @return Whether state is changed
   */
  virtual bool SwitchState(MSState state) = 0;

  /**
   * Get server state
   *
   * @return The state
   */
  virtual MSState GetState() const = 0;

  /**
   * Register a callback to invoke when the specific state takes effect.
   * Callback will be invoked immediately if current state matches.
   * If another callback is previously registered for the same state and
   * is not executed yet, the previously registered callback will be cancelled.
   *
   * @param state The state
   *
   * @param callback The callback
   */
  virtual void OnState(MSState state, MSStateChangeCallback callback) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
