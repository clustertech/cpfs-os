#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::IOServiceRunner for delay the start of Asio io_service(s)
 * on thread(s) after io_service(s) are created and initialized.
 */

#include "asio_common.hpp"

namespace cpfs {

/**
 * Interface for IOServiceRunner
 */
class IIOServiceRunner {
 public:
  /**
   * Destructor
   */
  virtual ~IIOServiceRunner() {}

  /**
   * Add a service
   */
  virtual void AddService(IOService* service) = 0;

  /**
   * Non-blocking. Run the added but not launched io_services on new threads
   */
  virtual void Run() = 0;

  /**
   * Non-blocking. Stop all running services
   */
  virtual void Stop() = 0;

  /**
   * Block until all io_service.run() finished
   */
  virtual void Join() = 0;
};

}  // namespace cpfs
