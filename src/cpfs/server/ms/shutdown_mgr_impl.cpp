/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::ShutdownMgr.
 */
#include "server/ms/shutdown_mgr_impl.hpp"

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_policy.hpp"
#include "common.hpp"
#include "io_service_runner.hpp"
#include "logger.hpp"
#include "periodic_timer.hpp"
#include "shutdown_mgr_base.hpp"
#include "tracker_mapper.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/topology.hpp"
#include "server/thread_group.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Shutdown manager for Meta Server
 */
class ShutdownMgr : public BaseShutdownMgr {
 public:
  /**
   * Create a shutdown manager for Meta Server
   *
   * @param server Meta server
   *
   * @param maker The periodic timer maker
   */
  ShutdownMgr(BaseMetaServer* server, PeriodicTimerMaker maker)
      : server_(server), periodic_timer_maker_(maker) {}

  /**
   * Initialize shutdown for Meta server
   */
  bool DoInit() {
    LOG(notice, Server, "Server is shutting down");
    if (!server_->IsHAMode() || server_->ha_counter()->IsActive())
      server_->topology_mgr()->AnnounceShutdown();
    server_->thread_group()->Stop();
    server_->state_mgr()->SwitchState(kStateShuttingDown);
    // TOOD(Joseph): Handle no DS connected case
    return true;
  }

  /**
   * Perform shutdown for Meta server
   */
  bool DoShutdown() {
    if (!server_->IsHAMode() || server_->ha_counter()->IsActive()) {
      LOG(notice, Server, "Requesting clients and servers to halt");
      server_->topology_mgr()->AnnounceHalt();
      timer_ = periodic_timer_maker_(asio_policy_->io_service(), 1);
      timer_->OnTimeout(boost::bind(&ShutdownMgr::Timeout, this));
    } else {
      Terminate();
    }
    return true;
  }

 private:
  BaseMetaServer* server_;
  PeriodicTimerMaker periodic_timer_maker_; /**< How to make PeriodicTimer */
  boost::shared_ptr<IPeriodicTimer> timer_; /**< Check for completion */

  /**
   * Wait until halt messages are delivered and standby MS disconnected
   */
  bool Timeout() {
    if (server_->tracker_mapper()->HasPendingWrite())
      return true;
    if (server_->tracker_mapper()->GetMSFimSocket())
      return true;

    Terminate();
    return false;
  }

  /**
   * Shutdown the server immediately
   */
  void Terminate() {
    LOG(notice, Server, "Server halt");
    server_->io_service_runner()->Stop();
  }
};

}  // namespace

IShutdownMgr* MakeShutdownMgr(BaseMetaServer* server,
                              PeriodicTimerMaker maker) {
  return new ShutdownMgr(server, maker);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
