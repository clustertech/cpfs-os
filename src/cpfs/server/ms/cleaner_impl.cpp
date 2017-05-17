/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICleaner implementation.
 */

#include "server/ms/cleaner_impl.hpp"

#include <stdint.h>

#include <algorithm>
#include <ctime>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_common.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "io_service_runner.hpp"
#include "periodic_timer.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "server/ccache_tracker.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/cleaner.hpp"
#include "server/ms/replier.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/ugid_handler.hpp"
#include "server/reply_set.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * The maximum age of cache entries.
 */
const int kCacheMaxAge = 3600;
/**
 * The maximum age of recent callback stored by call from
 * Replier.DoReply().
 */
const int kRecentCbMaxAge = 60;
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
  explicit Cleaner(BaseMetaServer* server);
  void SetPeriodicTimerMaker(PeriodicTimerMaker maker);
  void Init();

 private:
  IOService io_service_;
  BaseMetaServer* server_;
  PeriodicTimerMaker timer_maker_;
  boost::shared_ptr<IPeriodicTimer> timer_;
  time_t last_latch_;

  bool Iteration();
};

Cleaner::Cleaner(BaseMetaServer* server) : server_(server) {}

void Cleaner::SetPeriodicTimerMaker(PeriodicTimerMaker maker) {
  timer_maker_ = maker;
}

void Cleaner::Init() {
  timer_ = timer_maker_(&io_service_, kMSCleanerIterationTime);
  timer_->OnTimeout(boost::bind(&Cleaner::Iteration, this));
  server_->io_service_runner()->AddService(&io_service_);
  last_latch_ = std::time(0);
}

bool Cleaner::Iteration() {
  server_->cache_tracker()->ExpireCache(kCacheMaxAge);
  if (server_->tracker_mapper()->GetMSFimSocket()) {
    server_->recent_reply_set()->ExpireReplies(kMSReplySetMaxAge);
    server_->ugid_handler()->Clean();
    time_t last_seen = server_->peer_time_keeper()->GetLastUpdate();
    time_t latch_itv = kReplAndIODelay +
        uint64_t(server_->configs().heartbeat_interval()) * 2;
    if (last_seen > last_latch_ + latch_itv) {
      MSState state = server_->state_mgr()->GetState();
      if (state == kStateActive || state == kStateStandby) {
        server_->durable_range()->Latch();
        last_latch_ = std::max(last_seen, std::time(0));
      }
    }
  }
  server_->replier()->ExpireCallbacks(kRecentCbMaxAge);
  server_->inode_removal_tracker()->ExpireRemoved(kRecentRemovedAge);
  return true;
}

}  // namespace

ICleaner* MakeCleaner(BaseMetaServer* server) {
  return new Cleaner(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
