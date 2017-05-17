/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICleaner implementation.
 */

#include "server/ds/cleaner_impl.hpp"

#include <stdint.h>

#include <algorithm>
#include <ctime>
#include <vector>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_common.hpp"
#include "common.hpp"
#include "dsg_state.hpp"
#include "io_service_runner.hpp"
#include "mutex_util.hpp"
#include "periodic_timer.hpp"
#include "time_keeper.hpp"
#include "timed_list.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/cleaner.hpp"
#include "server/ds/store.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * The maximum age of recent removed inode.
 */
const int kRecentRemovedAge = 60;

/**
 * Implement the ICleaner interface, calling cache tracker and
 * recent reply set expiry methods periodically.
 */
class Cleaner : public ICleaner {
 public:
  /**
   * @param server The meta server to clean up
   */
  explicit Cleaner(BaseDataServer* server);
  void SetPeriodicTimerMaker(PeriodicTimerMaker maker);
  void SetMinInodeRemoveTime(unsigned remove_time);
  void Init();
  void Shutdown();
  void RemoveInodeLater(InodeNum inode);

 private:
  IOService io_service_;
  BaseDataServer* server_;
  PeriodicTimerMaker timer_maker_;
  boost::shared_ptr<IPeriodicTimer> timer_;
  time_t last_latch_;
  unsigned min_inode_remove_time_;
  MUTEX_TYPE data_mutex_;
  TimedList<InodeNum> inodes_to_remove_;

  bool Iteration();
};

Cleaner::Cleaner(BaseDataServer* server)
    : server_(server), data_mutex_(MUTEX_INIT) {}

void Cleaner::SetPeriodicTimerMaker(PeriodicTimerMaker maker) {
  timer_maker_ = maker;
}

void Cleaner::SetMinInodeRemoveTime(unsigned remove_time) {
  min_inode_remove_time_ = remove_time;
}

void Cleaner::Init() {
  timer_ = timer_maker_(&io_service_, kDSCleanerIterationTime);
  timer_->OnTimeout(boost::bind(&Cleaner::Iteration, this));
  server_->io_service_runner()->AddService(&io_service_);
  last_latch_ = std::time(0);
}

void Cleaner::Shutdown() {
  MUTEX_LOCK_GUARD(data_mutex_);
  BOOST_FOREACH(InodeNum inode, inodes_to_remove_.FetchAndClear()) {
    server_->store()->FreeData(inode, true);
  }
}

void Cleaner::RemoveInodeLater(InodeNum inode) {
  MUTEX_LOCK_GUARD(data_mutex_);
  inodes_to_remove_.Add(inode);
}

bool Cleaner::Iteration() {
  TimedList<InodeNum>::Elems expired;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    inodes_to_remove_.Expire(min_inode_remove_time_, &expired);
  }
  BOOST_FOREACH(InodeNum inode, expired) {
    server_->store()->FreeData(inode, true);
  }
  uint64_t state_change_id;
  GroupRole failed_role;
  time_t last_seen = server_->dsg_ready_time_keeper()->GetLastUpdate();
  if (last_seen > last_latch_ + kReplAndIODelay) {
    if (server_->dsg_state(&state_change_id, &failed_role) == kDSGReady) {
      server_->durable_range()->Latch();
      last_latch_ = std::max(last_seen, std::time(0));
    }
  }
  server_->inode_removal_tracker()->ExpireRemoved(kRecentRemovedAge);
  return true;
}

}  // namespace

ICleaner* MakeCleaner(BaseDataServer* server) {
  return new Cleaner(server);
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
