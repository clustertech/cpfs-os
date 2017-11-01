/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of request completion checking facilities.
 */

#include "req_completion_impl.hpp"

#include <list>
#include <vector>

#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "inode_map.hpp"
#include "mutex_util.hpp"
#include "req_completion.hpp"
#include "req_entry.hpp"

namespace cpfs {

namespace {

/**
 * Implement the IReqCompletionChecker interface.
 */
class ReqCompletionChecker : public IReqCompletionChecker {
 public:
  ReqCompletionChecker()
      : obj_mutex_(MUTEX_INIT),  // Separate the two for different mutex ID
        data_mutex_(MUTEX_INIT) {}
  ~ReqCompletionChecker() {
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
  void OnCompleteAll(ReqCompletionCallback callback);
  bool AllCompleted();

 private:
  /**
   * An entry kept by the checker.
   */
  struct CheckerEntry {
    int num_pending; /**< Number of pending requests */
    /**
     * Completion callbacks to run when num_pending is 0 and it is the
     * first entry.
     */
    std::list<ReqCompletionCallback> callbacks;

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

void ReqCompletionChecker::RegisterOp(const void* op) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (entries_.empty() || !entries_.front().callbacks.empty())
    entries_.push_front(CheckerEntry());
  ++entries_.front().num_pending;
  op_map_[op] = entries_.begin();
}

void ReqCompletionChecker::CompleteOp(const void* op) {
  MUTEX_LOCK_GUARD(obj_mutex_);
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (op_map_.find(op) == op_map_.end())
      return;
    --op_map_[op]->num_pending;
    op_map_.erase(op);
  }
  while (true) {
    ReqCompletionCallback callback;
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

void ReqCompletionChecker::OnCompleteAll(ReqCompletionCallback callback) {
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (!entries_.empty()) {
      entries_.front().callbacks.push_back(callback);
      return;
    }
  }
  callback();
}

bool ReqCompletionChecker::AllCompleted() {
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
  ReqCompletionCallback GetInodeCompletionCallback() {
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
  void SetCallback(ReqCompletionCallback callback) {
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      if (num_remain_callbacks_ > 0) {
        callback_ = callback;
        return;
      }
      callback_ = ReqCompletionCallback();
    }
    if (callback)
      callback();
  }

 private:
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  int num_remain_callbacks_; /**< Number of callbacks yet to be called */
  ReqCompletionCallback callback_; /**< The callback set */

  void InodeCompletion() {
    ReqCompletionCallback callback;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      if (--num_remain_callbacks_ > 0)
        return;
      callback = callback_;
      callback_ = ReqCompletionCallback();
    }
    if (callback)
      callback();
  }
};

/**
 * Implement the IReqCompletionCheckerSet interface.
 */
class ReqCompletionCheckerSet : public IReqCompletionCheckerSet,
                                InodeMap<ReqCompletionChecker> {
 public:
  boost::shared_ptr<IReqCompletionChecker> Get(InodeNum inode) {
    return operator[](inode);
  }

  ReqAckCallback GetReqAckCallback(
      InodeNum inode,
      const boost::shared_ptr<IFimSocket>& originator) {
    return boost::bind(&ReqCompletionCheckerSet::AckReq, this,
                       inode, originator, _1);
  }

  void OnCompleteAll(InodeNum inode, ReqCompletionCallback callback) {
    boost::shared_ptr<IReqCompletionChecker> checker =
        InodeMap<ReqCompletionChecker>::Get(inode);
    if (checker) {
      checker->OnCompleteAll(callback);
      Clean(inode);
      return;
    }
    callback();
  }

  void OnCompleteAllGlobal(ReqCompletionCallback callback) {
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
      const std::vector<InodeNum>& subset, ReqCompletionCallback callback) {
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
  bool IsUnused(const boost::shared_ptr<ReqCompletionChecker>& elt) const {
    return elt->AllCompleted();
  }

 private:
  /**
   * The request ack callback returned by GetReqAckCallback
   *
   * @param inode The inode number
   *
   * @param req_entry The entry acknowledged
   */
  void AckReq(InodeNum inode, const boost::shared_ptr<IFimSocket>& originator,
              const boost::shared_ptr<IReqEntry>& req_entry);
};

void ReqCompletionCheckerSet::AckReq(
    InodeNum inode,
    const boost::shared_ptr<IFimSocket>& originator,
    const boost::shared_ptr<IReqEntry>& req_entry) {
  if (originator) {
    FIM_PTR<FinalReplyFim> final_reply = FinalReplyFim::MakePtr();
    final_reply->set_req_id(req_entry->req_id());
    final_reply->set_final();
    originator->WriteMsg(final_reply);
  }
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    MapType::iterator it = data_.find(inode);
    if (it == data_.end())
      return;
    it->second->CompleteOp(req_entry.get());
  }
  Clean(inode);
}

}  // namespace

IReqCompletionChecker* MakeReqCompletionChecker() {
  return new ReqCompletionChecker;
}

IReqCompletionCheckerSet* MakeReqCompletionCheckerSet() {
  return new ReqCompletionCheckerSet;
}

}  // namespace cpfs
