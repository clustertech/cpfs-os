#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs:IService interface
 */

#include <boost/noncopyable.hpp>

namespace cpfs {

/**
 * Interface for Service classes.  Such classes run for a period of
 * time, and are initialized and shutdown explicitly before and after
 * the run.  Since it is not clear what should happen when a running
 * service is copied, services are by default not copiable.
 */
class IService : boost::noncopyable {
 public:
  virtual ~IService() {}

  /**
   * Initialize the service.
   */
  virtual void Init() = 0;

  /**
   * Run the service.
   */
  virtual int Run() = 0;

  /**
   * Shutdown the service.
   */
  virtual void Shutdown() = 0;
};

}  // namespace cpfs
