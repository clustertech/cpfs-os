/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for checking data in the MS store.
 */
#include <cstdio>
#include <cstring>

#include <boost/scoped_ptr.hpp>

#include <pantheios/backend.h>
#include <pantheios/backends/bec.file.h>
#include <pantheios/frontends/stock.h>  // IWYU pragma: keep
#include <pantheios/pantheios.hpp>

#include "console_impl.hpp"
#include "server/ms/store_checker.hpp"
#include "server/ms/store_checker_impl.hpp"
#include "server/ms/store_checker_plugin_impl.hpp"

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "CPFS-ms-checker";  /**< ID for logging */

/**
 * Utility main
 */
int main(int argc, char** argv) {
  pantheios_be_file_setFilePath("/dev/stderr", 0, 0, PANTHEIOS_BEID_ALL);
  bool do_correct = false;
  int pos = 1;
  if (argc >= 2 && std::strcmp(argv[1], "-c") == 0) {
    do_correct = true;
    ++pos;
  }
  if (argc != pos + 1) {
    std::fprintf(stderr, "Usage: %s [-c] <Directory>\n", argv[0]);
    return 2;
  }
  boost::scoped_ptr<cpfs::server::ms::IStoreChecker> checker(
      cpfs::server::ms::MakeStoreChecker(
          argv[pos],
          cpfs::MakeConsole(stdin, stdout, stderr)));
  checker->RegisterPlugin(cpfs::server::ms::MakeTreeStoreCheckerPlugin());
  checker->RegisterPlugin(
      cpfs::server::ms::MakeSpecialInodeStoreCheckerPlugin());
  checker->RegisterPlugin(
      cpfs::server::ms::MakeInodeCountStoreCheckerPlugin());
  return checker->Run(do_correct) ? 0 : 1;
}
