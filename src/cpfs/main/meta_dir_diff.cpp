/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for comparing data in two MS stores.
 */
#include <inttypes.h>
#include <stdint.h>

#include <cstdio>
#include <string>

#include <pantheios/backend.h>
#include <pantheios/backends/bec.file.h>
#include <pantheios/frontends/stock.h>  // IWYU pragma: keep
#include <pantheios/pantheios.hpp>

#include "common.hpp"
#include "systest/meta_dir_diff.hpp"

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "CPFS";  /**< ID for logging */

/**
 * Utility main
 */
int main(int argc, char** argv) {
  pantheios_be_file_setFilePath("/dev/stderr", 0, 0, PANTHEIOS_BEID_ALL);
  if (argc != 3) {
    std::fprintf(stderr, "Usage: <Directory 1> <Directory 2>\n");
    return 2;
  }
  std::printf("Comparing MS directories %s and %s\n", argv[1], argv[2]);
  uint64_t total_checked = 0;
  uint64_t total_errors = 0;
  for (unsigned d_idx = 0; d_idx < cpfs::kNumBaseDir; ++d_idx) {
    char buf[4];
    std::snprintf(buf, sizeof(buf), "%03x", d_idx);
    std::string dir1 = std::string(argv[1]) + "/" + std::string(buf);
    std::string dir2 = std::string(argv[2]) + "/" + std::string(buf);
    uint64_t num_checked;
    bool ret = cpfs::systest::MetaDirDiff(dir1, dir2, &num_checked);
    total_checked += num_checked;
    if (!ret)
      ++total_errors;
  }
  std::printf("Checked %" PRIu64 " inodes\n", total_checked);
  if (total_errors) {
    std::printf("%" PRIu64 "/%d problematic subdirectories\n",
                total_errors, cpfs::kNumBaseDir);
    return 1;
  }
  std::printf("No error found\n");
  return 0;
}
