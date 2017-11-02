/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of operation completion checking facilities.
 */

#include "op_completion_impl.hpp"

#include <list>
#include <vector>

#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "inode_map.hpp"
#include "mutex_util.hpp"
#include "op_completion.hpp"

namespace cpfs {

namespace {

/**
 * Implement the IOpCompletionChecker interface.
 */
class OpCompletionChecker : public IOpCompletionChecker {
 public:
  OpCompletionChecker()
      : obj_mutex_(MUTEX_INIT),  // Separate the two for different mutex ID
        data_mutex_(MUTEX_INIT) {}
  ~OpCompletionChecker() {
    // Wait until object mutex is unlocked before proceeding to
    // destroy the checker.  This allows code to delete the checker
    // safely by (1) ensure that nobody is going to call further
    // RegisterOp(), (2) call OnCompleteAll(), registering a callback
    // to do (3), (3) when callback is called, notify another thread
    // that the checker is available for deletion, and (4) delete the
    // checker in that other thread.  Note that another thread must be
    // used to delete the checker, because otherwise the deletion
    // thread will block, making CompleteOp() block as well and we end
    // up into a deadlock.
    {
      MUTEX_LOCK_GUARD(obj_mutex_);
    }
  }
  void RegisterOp(const void* op);
  void CompleteOp(const void* op);
  void OnCompleteAll(OpCompletionCallback callback);
  bool AllCompleted();

 private:
  /**
   * An entry kept by the checker.
   */
  struct CheckerEntry {
    int num_pending; /**< Number of pending operations */
    /**
     * Completion callbacks to run when num_pending is 0 and it is the
     * first entry.
     */
    std::list<OpCompletionCallback> callbacks;

    CheckerEntry() : num_pending(0) {}
  };

  /**
   * CheckerEntry list, in reversed chronological order (front() is
   * the most recently added entry).
   */
  typedef std::list<CheckerEntry> CheckerEntryList;

  MUTEX_TYPE obj_mutex_; /**< Prevent object destruction */
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  CheckerEntryList entries_; /**< Entries kept */
  boost::unordered_map<const void*, CheckerEntryList::iterator> op_map_;
};

void OpCompletionChecker::RegisterOp(const void* op) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (entries_.empty() || !entries_.front().callbacks.empty())
    entries_.push_front(CheckerEntry());
  ++entries_.front().num_pending;
  op_map_[op] = entries_.begin();
}

void OpCompletionChecker::CompleteOp(const void* op) {
  MUTEX_LOCK_GUARD(obj_mutex_);
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (op_map_.find(op) == op_map_.end())
      return;
    --op_map_[op]->num_pending;
    op_map_.erase(op);
  }
  while (true) {
    OpCompletionCallback callback;
    {  // Call each callback without holding the mutex
      MUTEX_LOCK_GUARD(data_mutex_);
      if (entries_.empty())
        break;
      if (entries_.back().num_pending > 0)
        break;
      CheckerEntry& entry = entries_.back();
      if (entry.callbacks.empty()) {
        entries_.pop_back();
        continue;
      }
      callback = entry.callbacks.front();
      entry.callbacks.pop_front();
    }
    callback();
  }
}

void OpCompletionChecker::OnCompleteAll(OpCompletionCallback callback) {
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (!entries_.empty()) {
      entries_.front().callbacks.push_back(callback);
      return;
    }
  }
  callback();
}

bool OpCompletionChecker::AllCompleted() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return entries_.empty();
}

/**
 * A record for implementing OnCompleteAllGlobal.
 */
class AllInodesCompletionRec
    : public boost::enable_shared_from_this<AllInodesCompletionRec> {
 public:
  AllInodesCompletionRec()
      : data_mutex_(MUTEX_INIT), num_remain_callbacks_(0) {}

  /**
   * Create an inode completion callback.  When the callback is run,
   * it checks whether the number of times such callbacks are called
   * is the same as the number of such callbacks created, and
   * SetCallback() has already been called.  If both are true, call
   * the callback.
   */
  OpCompletionCallback GetInodeCompletionCallback() {
    MUTEX_LOCK_GUARD(data_mutex_);
    ++num_remain_callbacks_;
    boost::shared_ptr<AllInodesCompletionRec> ptr =
        boost::static_pointer_cast<AllInodesCompletionRec>(shared_from_this());
    return boost::bind(&AllInodesCompletionRec::InodeCompletion, ptr);
  }

  /**
   * Set the callback to be called once all inode completion callbacks
   * are called.  If all are already called, the callback is called
   * immediately.
   */
  void SetCallback(OpCompletionCallback callback) {
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      if (num_remain_callbacks_ > 0) {
        callback_ = callback;
        return;
      }
      callback_ = OpCompletionCallback();
    }
    if (callback)
      callback();
  }

 private:
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  int num_remain_callbacks_; /**< Number of callbacks yet to be called */
  OpCompletionCallback callback_; /**< The callback set */

  void InodeCompletion() {
    OpCompletionCallback callback;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      if (--num_remain_callbacks_ > 0)
        return;
      callback = callback_;
      callback_ = OpCompletionCallback();
    }
    if (callback)
      callback();
  }
};

/**
 * Implement the IOpCompletionCheckerSet interface.
 */
class OpCompletionCheckerSet : public IOpCompletionCheckerSet,
                                InodeMap<OpCompletionChecker> {
 public:
  boost::shared_ptr<IOpCompletionChecker> Get(InodeNum inode) {
    return operator[](inode);
  }

  void CompleteOp(InodeNum inode, const void* op) {
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      MapType::iterator it = data_.find(inode);
      if (it == data_.end())
        return;
      it->second->CompleteOp(op);
    }
    Clean(inode);
  }

  void OnCompleteAll(InodeNum inode, OpCompletionCallback callback) {
    boost::shared_ptr<IOpCompletionChecker> checker =
        InodeMap<OpCompletionChecker>::Get(inode);
    if (checker) {
      checker->OnCompleteAll(callback);
      Clean(inode);
      return;
    }
    callback();
  }

  void OnCompleteAllGlobal(OpCompletionCallback callback) {
    boost::shared_ptr<AllInodesCompletionRec> rec
        = boost::make_shared<AllInodesCompletionRec>();
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      for (MapType::iterator it = data_.begin(); it != data_.end(); ++it)
        it->second->OnCompleteAll(rec->GetInodeCompletionCallback());
    }
    rec->SetCallback(callback);
  }

  virtual void OnCompleteAllSubset(
      const std::vector<InodeNum>& subset, OpCompletionCallback callback) {
    boost::shared_ptr<AllInodesCompletionRec> rec
        = boost::make_shared<AllInodesCompletionRec>();
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      for (std::vector<InodeNum>::const_iterator it = subset.begin();
           it != subset.end();
           ++it) {
        MapType::iterator found = data_.find(*it);
        if (found != data_.end())
          found->second->OnCompleteAll(rec->GetInodeCompletionCallback());
      }
    }
    rec->SetCallback(callback);
  }

 protected:
  /**
   * Check whether an entry is currently unused and is thus free for
   * clean up.
   *
   * @param elt The element to check
   *
   * @return Whether the element is considered unused
   */
  bool IsUnused(const boost::shared_ptr<OpCompletionChecker>& elt) const {
    return elt->AllCompleted();
  }
};

}  // namespace

IOpCompletionChecker* MakeOpCompletionChecker() {
  return new OpCompletionChecker;
}

IOpCompletionCheckerSet* MakeOpCompletionCheckerSet() {
  return new OpCompletionCheckerSet;
}

}  // namespace cpfs
