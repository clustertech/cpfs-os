#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Defines ShutdownMgr for client and server to perform shutdown.
 */

#include "asio_common.hpp"
#include "common.hpp"

namespace cpfs {

class IAsioPolicy;

/**
 * Interface for ShutdownMgr.
 *
 * Shutdown manager handles shutdown for client and server. A graceful shutdown
 * is Init() first to prepare for cleanup and called Shutdown() when parties
 * in CPFS all agreed to shutdown, or timeout occurs.
 */
class IShutdownMgr {
 public:
  virtual ~IShutdownMgr() {}

  /**
   * Set the Asio policy to use.  Must be called before calling
   * Init(), SetupSignals() or destroying the object.
   *
   * @param policy The Asio policy
   */
  virtual void SetAsioPolicy(IAsioPolicy* policy) = 0;

  /**
   * Setup signals to initiate shutdown.  After the call, the specified
   * signal will cause Init() to be called with default timeout.
   *
   * @param signals The signals which would trigger Init()
   *
   * @param num_signals Size of the signals array
   */
  virtual void SetupSignals(const int signals[], unsigned num_signals) = 0;

  /**
   * Initialize shutdown and set timeout. Shutdown() will be called on timeout.
   *
   * @param timeout Timeout to call Shutdown()
   *
   * @return True if initialized successfully
   */
  virtual bool Init(double timeout = kShutdownTimeout) = 0;

  /**
   * Perform shutdown and cancel the timeout
   *
   * @return True if shutdown performed
   */
  virtual bool Shutdown() = 0;

  /**
   * Whether Init() is called
   *
   * @return True if server shutdown is inited
   */
  virtual bool inited() const = 0;

 /**
   * Whether Shutdown() is called
   *
   * @return True if server is shutting down
   */
  virtual bool shutting_down() const = 0;
};

}  // namespace cpfs
