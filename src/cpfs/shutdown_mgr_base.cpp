/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::ShutdownMgr.
 */

#include "shutdown_mgr_base.hpp"

#include <boost/bind.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "mutex_util.hpp"

namespace cpfs {

BaseShutdownMgr::BaseShutdownMgr()
    : data_mutex_(MUTEX_INIT), inited_(false), shutting_down_(false) {}

BaseShutdownMgr::~BaseShutdownMgr() {
  CancelAsync_();
}

void BaseShutdownMgr::SetAsioPolicy(IAsioPolicy* policy) {
  asio_policy_ = policy;
}

/**
 * Handle termination signals.
 *
 * @param shutdown_mgr The shutdown manager
 *
 * @param error The error passed by Asio
 */
void TermSignalHandler(IShutdownMgr* shutdown_mgr,
                       const boost::system::error_code& error) {
  if (!error)
    shutdown_mgr->Init();
}

void BaseShutdownMgr::SetupSignals(const int signals[], unsigned num_signals) {
  term_signal_set_.reset(asio_policy_->MakeSignalSet());
  asio_policy_->SetSignalHandler(
      term_signal_set_.get(),
      signals,
      num_signals,
      boost::bind(&TermSignalHandler, this, _1));
}

bool BaseShutdownMgr::Init(double timeout) {
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (inited_)
      return false;
    inited_ = true;
    term_signal_set_.reset();
    timer_.reset(asio_policy_->MakeDeadlineTimer());
    asio_policy_->SetDeadlineTimer(
        timer_.get(), timeout,
        boost::bind(&BaseShutdownMgr::HandleTimeout_, this, _1));
  }
  return DoInit();
}

bool BaseShutdownMgr::Shutdown() {
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (shutting_down_)
      return false;
    shutting_down_ = true;
    CancelAsync_();
  }
  return DoShutdown();
}

bool BaseShutdownMgr::inited() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return inited_;
}

bool BaseShutdownMgr::shutting_down() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return shutting_down_;
}

inline void BaseShutdownMgr::CancelAsync_() {
  if (timer_)
    asio_policy_->SetDeadlineTimer(timer_.get(), 0, IOHandler());
  if (term_signal_set_)
    asio_policy_->SetSignalHandler(term_signal_set_.get(), 0, 0,
                                   SignalHandler());
}

void BaseShutdownMgr::HandleTimeout_(const boost::system::error_code& error) {
  timer_.reset();
  if (!error)
    Shutdown();
}

}  // namespace cpfs
