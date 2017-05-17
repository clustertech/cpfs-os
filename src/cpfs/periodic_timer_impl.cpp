/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of PeriodicTimer.
 */

#include "periodic_timer_impl.hpp"

#include <boost/bind.hpp>
#include <boost/chrono/duration.hpp>
#include <boost/chrono/system_clocks.hpp>
#include <boost/chrono/time_point.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>
#include <boost/functional/factory.hpp>
#include <boost/functional/forward_adapter.hpp>
#include <boost/make_shared.hpp>
#include <boost/ratio/ratio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#include "asio_common.hpp"
#include "periodic_timer.hpp"
#include "util.hpp"

namespace cpfs {

namespace {

/**
 * Actual implementation of PeriodicTimer
 */
class PeriodicTimer : public IPeriodicTimer,
                      public boost::enable_shared_from_this<PeriodicTimer> {
 public:
  /**
   * Construct the PeriodicTimer
   */
  explicit PeriodicTimer(IOService* io_service, double timeout);
  ~PeriodicTimer();

  void Reset(double tolerance);
  void Stop();
  void OnTimeout(PeriodicTimerCallback callback);
  boost::shared_ptr<IPeriodicTimer> Duplicate(IOService* io_service);

 protected:
  /**
   * Handle inactivity by calling inactivity callback and starting event timer
   */
  static void OnTimeout(boost::weak_ptr<PeriodicTimer> wptr,
                        const boost::system::error_code& error);

 private:
  DeadlineTimer timer_;
  double timeout_;
  boost::posix_time::time_duration timeout_duration_;
  boost::chrono::steady_clock::time_point last_reset_;
  PeriodicTimerCallback callback_;
  bool started_;
};

PeriodicTimer::PeriodicTimer(IOService* io_service, double timeout)
    : timer_(*io_service),
      timeout_(timeout),
      timeout_duration_(ToTimeDuration(timeout)),
      started_(true) {
}

PeriodicTimer::~PeriodicTimer() {
  Stop();
}

void PeriodicTimer::Reset(double tolerance) {
  boost::chrono::steady_clock::time_point now =
      boost::chrono::steady_clock::now();
  if (started_) {
    boost::chrono::duration<double> elapsed = now - last_reset_;
    if (started_ && elapsed.count() < tolerance * timeout_)
      return;
  }
  started_ = true;
  last_reset_ = now;
  timer_.expires_from_now(timeout_duration_);
  timer_.async_wait(boost::bind(
      &OnTimeout,
      boost::weak_ptr<PeriodicTimer>(shared_from_this()),
      _1));
}

void PeriodicTimer::Stop() {
  started_ = false;
  timer_.cancel();
}

void PeriodicTimer::OnTimeout(PeriodicTimerCallback callback) {
  callback_ = callback;
  if (started_)
    Reset(0);
}

void PeriodicTimer::OnTimeout(boost::weak_ptr<PeriodicTimer> wptr,
                              const boost::system::error_code& error) {
  boost::shared_ptr<PeriodicTimer> ptr = wptr.lock();
  if (ptr && !error)
    if (ptr->callback_())
      ptr->Reset(0);
}

boost::shared_ptr<IPeriodicTimer> PeriodicTimer::Duplicate(
    IOService* io_service) {
  boost::shared_ptr<PeriodicTimer> ret =
      boost::make_shared<PeriodicTimer>(io_service, timeout_);
  ret->callback_ = callback_;
  ret->started_ = started_;
  if (started_)
    ret->Reset(0);
  return ret;
}

}  // namespace

PeriodicTimerMaker kPeriodicTimerMaker = boost::forward_adapter<
  boost::factory<boost::shared_ptr<PeriodicTimer> > >();

}  // namespace cpfs
