/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS.
 */

#include <sys/resource.h>

#include <exception>

#include <boost/scoped_ptr.hpp>

#include <pantheios/frontends/stock.h>  // IWYU pragma: keep

#include "logger.hpp"
#include "service.hpp"
#include "client/client_main.hpp"

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "CPFS";  /**< ID for logging */

/**
 * Main entry point.
 */
int main(int argc, char* argv[]) {
  setpriority(PRIO_PROCESS, 0, -10);
  try {
    boost::scoped_ptr<cpfs::IService> service;
    service.reset(cpfs::client::MakeFSClient(argc, argv));
    if (service) {
      service->Init();
      service->Run();
      LOG(notice, Server, "CPFS exiting");
      service->Shutdown();
      return 0;
    }
  } catch (const std::exception& ex) {
    cpfs::SetLogPath("/");
    LOG(error, Server, ex.what());
  }
  return 1;
}
