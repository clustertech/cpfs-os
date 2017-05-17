/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS server.
 */

#include <sys/resource.h>

#include <exception>

#include <boost/scoped_ptr.hpp>

#include <pantheios/frontends/stock.h>  // IWYU pragma: keep

#include "daemonizer.hpp"
#include "daemonizer_impl.hpp"
#include "logger.hpp"
#include "service.hpp"
#include "main/server_main.hpp"

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "CPFS";  /**< ID for logging */

/**
 * Main entry point.
 */
int main(int argc, char* argv[]) {
  setpriority(PRIO_PROCESS, 0, -10);
  try {
    boost::scoped_ptr<cpfs::IDaemonizer> daemonizer(cpfs::MakeDaemonizer());
    boost::scoped_ptr<cpfs::IService> service;
    service.reset(cpfs::main::MakeServer(argc, argv, daemonizer.get()));
    if (service) {
      service->Init();
      service->Run();
      LOG(notice, Server, "CPFS exiting");
      service->Shutdown();
      return 0;
    }
  } catch (const std::exception& ex) {
    LOG(error, Server, ex.what());
  }
  return 1;
}
