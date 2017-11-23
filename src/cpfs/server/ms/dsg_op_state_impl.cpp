/* Copyright 2017 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the implementation of DSG operation state class.
 */

#include "server/ms/dsg_op_state_impl.hpp"

#include <algorithm>
#include <iterator>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "op_completion.hpp"

#include "server/ms/dsg_op_state.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Represent the operational state of a DS group.
 *
 * At present it only keeps which inodes are currently resyncing and
 * thus cannot be truncated yet.
 */
struct DSGOpsState {
  boost::unordered_set<InodeNum> resyncing; /**< inodes is being resynced */
};

/**
 * Map from group number to state.
 */
typedef boost::unordered_map<GroupId, DSGOpsState> DSGOpsStateMap;

/**
 * Implement the IDSGOpStateMgr interface.
 */
class DSGOpStateMgr : public IDSGOpStateMgr {
 public:
  /**
   * @param checker_set The checker set to use
   */
  explicit DSGOpStateMgr(IOpCompletionCheckerSet* checker_set)
    : completion_checker_set_(checker_set) {}

  void RegisterInodeOp(InodeNum inode, const void* op) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, data_lock);
    bool logged = false;
    while (true) {
      bool wait = false;
      BOOST_FOREACH(const DSGOpsStateMap::value_type& item, ops_states_) {
        if (item.second.resyncing.find(inode) != item.second.resyncing.end()) {
          wait = true;
          break;
        }
      }
      if (!wait)
        break;
      if (!logged) {
        logged = true;
        LOG(informational, Degraded, "Inode op for inode ", PHex(inode),
            " waits because a resync is in progress");
      }
      MUTEX_WAIT(data_lock, wake_);
    }
    completion_checker_set_->Get(inode)->RegisterOp(op);
    if (logged)
      LOG(informational, Degraded, "Inode op for inode ", PHex(inode),
          " wait completed");
  }

  void CompleteInodeOp(InodeNum inode, const void* op) {
    completion_checker_set_->CompleteOp(inode, op);
  }

  void OnInodesCompleteOp(const std::vector<InodeNum> inodes,
                          OpCompletionCallback callback) {
    completion_checker_set_->OnCompleteAllSubset(inodes, callback);
  }

  void SetDsgInodesResyncing(
      GroupId group, const std::vector<InodeNum>& inodes) {
    bool need_notify = false;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      DSGOpsState& ops_state = ops_states_[group];
      if (ops_state.resyncing.size() > 0) {
        need_notify = true;
        ops_state.resyncing.clear();
      }
      std::copy(inodes.begin(), inodes.end(),
                std::inserter(ops_state.resyncing,
                              ops_state.resyncing.begin()));
    }
    if (need_notify)
      wake_.notify_all();
  }

 private:
  /** Completion checker for DS operations */
  boost::scoped_ptr<IOpCompletionCheckerSet> completion_checker_set_;
  MUTEX_TYPE data_mutex_; /**< Protect fields below */
  boost::condition_variable wake_;  /**< When there is an unlock */
  DSGOpsStateMap ops_states_; /**< DSG operation states */
};

}  // namespace

IDSGOpStateMgr* MakeDSGOpStateMgr(IOpCompletionCheckerSet* checker_set) {
  return new DSGOpStateMgr(checker_set);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
