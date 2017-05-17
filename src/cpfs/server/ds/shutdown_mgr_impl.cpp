/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the ShutdownMgr for DS.
 */
#include "server/ds/shutdown_mgr_impl.hpp"

#include "io_service_runner.hpp"
#include "logger.hpp"
#include "shutdown_mgr_base.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/cleaner.hpp"
#include "server/thread_group.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * Shutdown manager for Data Server
 */
class ShutdownMgr : public BaseShutdownMgr {
 public:
  /**
   * Create a ShutdownMgr for Data Server
   *
   * @param server Data server
   */
  explicit ShutdownMgr(BaseDataServer* server) : server_(server) {}

  /**
   * Initialize shutdown for Data server
   */
  bool DoInit() {
    LOG(notice, Server, "Server is shutting down");
    return true;
  }

  /**
   * Perform shutdown for Data server
   */
  bool DoShutdown() {
    LOG(notice, Server, "Server halt");
    server_->io_service_runner()->Stop();
    server_->thread_group()->Stop();
    server_->cleaner()->Shutdown();
    return true;
  }

 private:
  BaseDataServer* server_;
};

}  // namespace

IShutdownMgr* MakeShutdownMgr(BaseDataServer* server) {
  return new ShutdownMgr(server);
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
