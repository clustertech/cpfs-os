/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define client::ICleaner implementation.
 */

#include "client/cleaner_impl.hpp"

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_common.hpp"
#include "io_service_runner.hpp"
#include "periodic_timer.hpp"
#include "client/base_client.hpp"
#include "client/cache.hpp"
#include "client/cleaner.hpp"

namespace cpfs {
namespace client {

namespace {

/**
 * The amount of time between iterations of cleanups.
 */
const double kIterationTime = 60.0;
/**
 * The maximum age of cache entries.
 */
const int kCacheMaxAge = 3600;

/**
 * Implement the ICleaner interface, clean up client data
 * structures periodically.
 */
class Cleaner : public ICleaner {
 public:
  /**
   * @param client The client to clean up
   */
  explicit Cleaner(BaseFSClient* client);
  void SetPeriodicTimerMaker(PeriodicTimerMaker maker);
  void Init();

 private:
  IOService io_service_;
  BaseFSClient* client_;
  PeriodicTimerMaker timer_maker_;
  boost::shared_ptr<IPeriodicTimer> timer_;

  bool Iteration();
};

Cleaner::Cleaner(BaseFSClient* client) : client_(client) {}

void Cleaner::SetPeriodicTimerMaker(PeriodicTimerMaker maker) {
  timer_maker_ = maker;
}

void Cleaner::Init() {
  timer_ = timer_maker_(&io_service_, kIterationTime);
  timer_->OnTimeout(boost::bind(&Cleaner::Iteration, this));
  client_->service_runner()->AddService(&io_service_);
}

bool Cleaner::Iteration() {
  client_->cache_mgr()->CleanMgr(kCacheMaxAge);
  return true;
}

}  // namespace

ICleaner* MakeCleaner(BaseFSClient* client) {
  return new Cleaner(client);
}

}  // namespace client
}  // namespace cpfs
