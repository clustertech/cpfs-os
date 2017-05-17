#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define classes for handling DS group states.
 */

#include <boost/thread/shared_mutex.hpp>

#include "common.hpp"

namespace cpfs {

/**
 * Current state of a DS group.
 */
enum DSGroupState {
  kDSGOutdated = -1, /**< Special value to indicate outdated state info */
  kDSGPending = 0, /**< Some DS is free for assign */
  kDSGReady, /**< All DS has been assigned, all are active */
  kDSGDegraded, /**< All DS has been assigned, one is lost */
  kDSGRecovering, /**< All DS has been assigned, one is recovering */
  kDSGFailed, /**< All DS has been assigned, multiple are lost */
  kDSGShuttingDown /**< All DS are shutting down */
};

/**
 * Get a string representation of the DS group state
 *
 * @param state The DS group state
 *
 * @return String representation of the DS group state
 */
inline const char* ToStr(DSGroupState state) {
  static const char* state_map[] = {
    "Pending",
    "Ready",
    "Degraded",
    "Recovering",
    "Failed",
    "Shutting Down"
  };
  if (std::size_t(state) >= (sizeof(state_map) / sizeof(state_map[0])))
    return "Unknown";
  return state_map[state];
}

/**
 * Keep information related to DSG state.
 */
struct DSGStateInfo {
  boost::shared_mutex data_mutex; /**< reader-writer lock for fields below */
  uint64_t state_change_id; /**< State change ID leading to current state */
  DSGroupState dsg_state; /**< Current DSG state */
  GroupRole failed_role; /**< Failed role for kDSGDegraded / kDSGRecovering */

  /**
   * @param dsg_state The initial DS group state
   */
  explicit DSGStateInfo(DSGroupState dsg_state)
      : state_change_id(0), dsg_state(dsg_state) {}

  /**
   * @param other The object to copy
   */
  DSGStateInfo(const DSGStateInfo& other)
      : state_change_id(other.state_change_id), dsg_state(other.dsg_state),
        failed_role(other.failed_role) {}
};

}  // namespace cpfs
