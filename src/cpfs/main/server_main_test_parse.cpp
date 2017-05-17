/* Copyright 2013 ClusterTech Ltd */
#include <boost/scoped_ptr.hpp>

#include <pantheios/frontends/stock.h>  // IWYU pragma: keep

#include "service.hpp"
#include "main/server_main.hpp"

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "CPFS";  /**< ID for logging */

// Some tests of server_main generate output on stderr
// unconditionally.  This program allows capturing of the output so
// that it is not dumped as test output, and allow capturing of the
// output for checking
int main(int argc, char* argv[]) {
  return boost::scoped_ptr<cpfs::IService>(
      cpfs::main::MakeServer(argc, argv, 0)) ? 0 : 1;
}
