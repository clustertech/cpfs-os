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

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"
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
  boost::shared_mutex data_mutex; /**< reader-writer lock for fields below */
  boost::unordered_set<InodeNum> resyncing; /**< inodes is being resynced */
};

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
    completion_checker_set_->Get(inode)->RegisterOp(op);
  }

  void CompleteInodeOp(InodeNum inode, const void* op) {
    completion_checker_set_->CompleteOp(inode, op);
  }

  void OnInodesCompleteOp(const std::vector<InodeNum> inodes,
                          OpCompletionCallback callback) {
    completion_checker_set_->OnCompleteAllSubset(inodes, callback);
  }

  void ReadLock(GroupId group, boost::shared_lock<boost::shared_mutex>* lock) {
    MUTEX_LOCK_GUARD(data_mutex_);
    DSGOpsState& ops_state = ops_states_[group];
    MUTEX_LOCK(boost::shared_lock, ops_state.data_mutex, my_lock);
    lock->swap(my_lock);
  }

  void SetDsgInodesResyncing(
      GroupId group, const std::vector<InodeNum>& inodes) {
    MUTEX_LOCK_GUARD(data_mutex_);
    DSGOpsState& ops_state = ops_states_[group];
    MUTEX_LOCK(boost::unique_lock, ops_state.data_mutex, my_lock);
    ops_state.resyncing.clear();
    std::copy(inodes.begin(), inodes.end(),
              std::inserter(ops_state.resyncing, ops_state.resyncing.begin()));
  }

  bool is_dsg_inode_resyncing(GroupId group, InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    DSGOpsState& ops_state = ops_states_[group];
    return ops_state.resyncing.find(inode) != ops_state.resyncing.end();
  }

 private:
  /** Completion checker for DS operations */
  boost::scoped_ptr<IOpCompletionCheckerSet> completion_checker_set_;
  mutable MUTEX_TYPE data_mutex_; /**< Protect fields below */
  boost::unordered_map<GroupId, DSGOpsState>
  ops_states_; /**< DSG operation states */
};

}  // namespace

IDSGOpStateMgr* MakeDSGOpStateMgr(IOpCompletionCheckerSet* checker_set) {
  return new DSGOpStateMgr(checker_set);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
