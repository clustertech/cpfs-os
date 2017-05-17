#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Defines ShutdownMgr for client and server to perform shutdown.
 */

#include <boost/scoped_ptr.hpp>

#include "asio_common.hpp"
#include "mutex_util.hpp"
#include "shutdown_mgr.hpp"

namespace cpfs {

class IAsioPolicy;

/**
 * Generic ShutdownMgr for server
 */
class BaseShutdownMgr : public IShutdownMgr {
 public:
  BaseShutdownMgr();

  ~BaseShutdownMgr();

  void SetAsioPolicy(IAsioPolicy* policy);
  void SetupSignals(const int signals[], unsigned num_signals);
  bool Init(double timeout);
  bool Shutdown();
  bool inited() const;
  bool shutting_down() const;
  /**
   * Initialize shutdown implemented in subclass
   */
  virtual bool DoInit() = 0;
  /**
   * Perform shutdown implemented in subclass
   */
  virtual bool DoShutdown() = 0;

 protected:
  IAsioPolicy* asio_policy_; /**< The Asio policy to use */

 private:
  mutable MUTEX_TYPE data_mutex_; /**< Protect data below */
  boost::scoped_ptr<SignalSet> term_signal_set_; /**< Triggering signal set */
  boost::scoped_ptr<DeadlineTimer> timer_; /**< Timer for shutdown init */
  bool inited_; /** Whether shutdown is inited */
  bool shutting_down_; /** Whether server is shutting down */

  /**
   * Cancel async operations
   */
  inline void CancelAsync_();
  /**
   * Handle timeout for shutdown
   */
  void HandleTimeout_(const boost::system::error_code& error);
};

}  // namespace cpfs
