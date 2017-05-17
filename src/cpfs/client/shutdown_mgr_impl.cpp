/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the IShutdownMgr for client.
 */
#include "client/shutdown_mgr_impl.hpp"

#include "logger.hpp"
#include "service.hpp"
#include "shutdown_mgr_base.hpp"
#include "tracker_mapper.hpp"
#include "client/base_client.hpp"
#include "client/cache.hpp"

namespace cpfs {
namespace client {
namespace {

/**
 * Shutdown manager for FC
 */
class ShutdownMgr : public BaseShutdownMgr {
 public:
  /**
   * Create a shutdown manager for Client
   *
   * @param client The FC
   */
  explicit ShutdownMgr(BaseFSClient* client) : client_(client) {}

  /**
   * Initialize shutdown for Client.
   */
  bool DoInit() {
    LOG(notice, Server, "System shutdown request received from MS");
    client_->tracker_mapper()->Plug(false);
    client_->cache_mgr()->Shutdown();
    return true;
  }

  /**
   * Perform shutdown for client
   */
  bool DoShutdown() {
    LOG(notice, Server, "System halt request received from MS");
    client_->tracker_mapper()->Shutdown();
    client_->runner()->Shutdown();
    return true;
  }

 private:
  BaseFSClient* client_;
};

}  // namespace

IShutdownMgr* MakeShutdownMgr(BaseFSClient* client) {
  return new ShutdownMgr(client);
}

}  // namespace client
}  // namespace cpfs
