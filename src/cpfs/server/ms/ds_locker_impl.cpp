/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IDSLocker, an interface for DS locking in the
 * meta-server.
 */

#include "server/ms/ds_locker_impl.hpp"

#include <stdexcept>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "tracker_mapper.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/ds_locker.hpp"
#include "server/ms/topology.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implement the IMetaDSLock interface.
 */
class MetaDSLock : public IMetaDSLock {
 public:
  /**
   * @param tracker The tracker to lock
   *
   * @param inode The inode to perform DS lock
   */
  MetaDSLock(const boost::shared_ptr<IReqTracker>& tracker, InodeNum inode)
      : tracker_(tracker), inode_(inode), data_mutex_(MUTEX_INIT),
        acquired_(false), acquiring_(false), releasing_(false) {}

  virtual ~MetaDSLock() {
    PrepareRelease_();
  }

  void PrepareAcquire() {
    MUTEX_LOCK_GUARD(data_mutex_);
    PrepareAcquire_();
  }

  void WaitAcquireCompletion() {
    MUTEX_LOCK_GUARD(data_mutex_);
    WaitAcquireCompletion_();
  }

  void PrepareRelease() {
    MUTEX_LOCK_GUARD(data_mutex_);
    PrepareRelease_();
  }

 private:
  boost::shared_ptr<IReqTracker> tracker_; /**< Tracker for lock */
  InodeNum inode_; /**< Inode to lock */
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  bool acquired_; /**< Whether the lock has been acquired */
  bool acquiring_; /**< When !acquired_, Whether there acquire is pending */
  bool releasing_; /**< When acquired_, whether release is pending */
  boost::shared_ptr<IReqEntry> entry_; /**< Entry for acquiring_ */

  void PrepareAcquire_() {
    if (acquired_ || acquiring_ || !tracker_->GetFimSocket())
      return;
    boost::shared_ptr<IReqEntry> entry =
        MakeTransientReqEntry(tracker_, GetFim(true));
    if (!tracker_->AddRequestEntry(entry)) {
      LOG(informational, Lock,
          "When acquiring DS lock for inode ", PVal(inode_), ", skipping ",
          PVal(tracker_->name()), " as it is not connected");
      return;
    }
    acquiring_ = true;
    entry_ = entry;
  }

  void WaitAcquireCompletion_() {
    if (!acquiring_)
      return;
    try {
      entry_->WaitReply();
      acquired_ = true;
    } catch (std::runtime_error&) {
      LOG(informational, Lock,
          "When waiting for DS lock completion, locking of ", PVal(inode_),
          " skipped ", PVal(tracker_->name()), " due to disconnection");
    }
    acquiring_ = false;
  }

  void PrepareRelease_() {
    if (!acquired_ || releasing_ || !tracker_->GetFimSocket())
      return;
    tracker_->AddRequestEntry(MakeTransientReqEntry(tracker_, GetFim(false)));
  }

  FIM_PTR<DSInodeLockFim> GetFim(bool lock) {
    FIM_PTR<DSInodeLockFim> fim = DSInodeLockFim::MakePtr();
    (*fim)->inode = inode_;
    (*fim)->lock = lock;
    return fim;
  }
};

/**
 * Implement the IDSLocker interface.
 */
class DSLocker : public IDSLocker {
 public:
  /**
   * @param server The server using the locker
   */
  explicit DSLocker(BaseMetaServer* server);
  void Lock(InodeNum inode,
            const std::vector<GroupId>& group_ids,
            std::vector<boost::shared_ptr<IMetaDSLock> >* locks_ret);

 private:
  BaseMetaServer* server_;
};

DSLocker::DSLocker(BaseMetaServer* server) : server_(server) {}

void DSLocker::Lock(
    InodeNum inode, const std::vector<GroupId>& group_ids,
    std::vector<boost::shared_ptr<IMetaDSLock> >* locks_ret) {
  LOG(debug, Lock, "DS Locking inode ", PHex(inode));
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  for (std::vector<GroupId>::const_iterator group_it = group_ids.begin();
       group_it != group_ids.end();
       ++group_it) {
    GroupRole failed;
    DSGroupState state = server_->topology_mgr()->
        GetDSGState(*group_it, &failed);
    for (unsigned int role = 0; role < kNumDSPerGroup; ++role) {
      if (state != kDSGReady && role == failed)
        continue;
      boost::shared_ptr<MetaDSLock> lock(
          new MetaDSLock(tracker_mapper->GetDSTracker(*group_it, role),
                         inode));
      locks_ret->push_back(lock);
      lock->PrepareAcquire();
    }
  }
  for (std::vector<boost::shared_ptr<IMetaDSLock> >::iterator it =
           locks_ret->begin();
       it != locks_ret->end();
       ++it)
    (*it)->WaitAcquireCompletion();
  LOG(debug, Lock, "DS Locking inode ", PHex(inode), " done");
}

}  // namespace

IDSLocker* MakeDSLocker(BaseMetaServer* server) {
  return new DSLocker(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
