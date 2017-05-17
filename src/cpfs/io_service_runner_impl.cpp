/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::IOServiceRunner for delay the start of Asio io_service(s)
 * on thread(s) after io_service(s) are created and initialized.
 */

#include "io_service_runner_impl.hpp"

#include <cstddef>
#include <list>

#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>

#include "asio_common.hpp"
#include "io_service_runner.hpp"

namespace cpfs {

namespace {

/**
 * Runner for launching Boost.Asio io_service.run() in threads
 */
class IOServiceRunner : public IIOServiceRunner {
 public:
  /**
   * Add a service
   */
  void AddService(IOService* service) {
    io_services_.push_back(service);
  }

  ~IOServiceRunner() {
    Stop();
    Join();
  }

  /**
   * Non-blocking. Run the added but not launched io_services on new threads
   */
  void Run() {
    for (std::list<IOService*>::const_iterator itr = io_services_.begin();
         itr != io_services_.end(); ++itr) {
      IOService* service = *itr;
      works_.push_back(IOService::work(*service));
      threads_.create_thread(
          boost::bind(&boost::asio::io_service::run, service));
    }
    running_services_.splice(running_services_.end(), io_services_);
  }

  /**
   * Non-blocking. Stop all running services
   */
  void Stop() {
    works_.clear();
    for (std::list<IOService*>::iterator itr = running_services_.begin();
         itr != running_services_.end(); ++itr)
      (*itr)->stop();
    running_services_.clear();
  }

  /**
   * Block until all io_service.run() finished
   */
  void Join() {
    threads_.join_all();
  }

 private:
  /** The io_services to be launched in threads */
  std::list<IOService*> io_services_;
  std::list<IOService::work> works_; /**< Works preventing services to stop */
  std::list<IOService*> running_services_; /**< The io_services running */
  boost::thread_group threads_; /**< The threads running the io_services */
};

}  // namespace

IIOServiceRunner* MakeIOServiceRunner() {
  return new IOServiceRunner();
}

}  // namespace cpfs
