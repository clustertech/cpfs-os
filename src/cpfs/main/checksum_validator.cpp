/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for checking data in a DS group
 */
#include <inttypes.h>
#include <stdint.h>

#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include <pantheios/backend.h>
#include <pantheios/backends/bec.file.h>
#include <pantheios/frontends/stock.h>  // IWYU pragma: keep

#include "common.hpp"
#include "ds_iface.hpp"
#include "logger.hpp"
#include "server/ds/checksum_validator.hpp"

namespace po = boost::program_options;

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "checksum_validator";  /**< ID for logging */

/**
 * Parse command line options.
 *
 * @param argc The number of arguments
 *
 * @param argv The arguments
 *
 * @param desc Where to put option descriptions in case the program
 * wants to print it
 *
 * @param vm Where to return the parsed options
 *
 * @return Whether the parse is successful
 */
bool ParseOpts(int argc, char** argv,
               po::options_description* desc, po::variables_map* vm) {
  desc->add_options()
      ("help,h", "Produce help message")
      ("mode,m", po::value<std::string>()->default_value("validate"),
       "'validate' to validate checksum, 'pos' to show checksum group")
      ("inode,i", po::value<uint64_t>(), "Inode number to use in 'pos' mode")
      ("cg_off,o", po::value<uint64_t>(), "Checksum group DSG offset")
      ("root", "Root directory in 'validate' mode");
  po::positional_options_description p;
  p.add("root", -1);
  try {
    po::store(po::command_line_parser(argc, argv)
              .options(*desc).positional(p).run(),
              *vm);
    po::notify(*vm);
  } catch (const std::exception& e) {
    fprintf(stderr, "Error parsing arguments: %s\n", e.what());
    return false;
  }
  return true;
}

/**
 * Validate a root directory.
 *
 * @param vm The parsed options
 */
int validate(const po::variables_map& vm) {
  if (!vm.count("root")) {
    std::fprintf(stderr, "Root path not set in validate mode\n");
    return 2;
  }
  std::string root = vm["root"].as<std::string>();
  std::printf("Validating DSG data at %s\n", root.c_str());
  uint64_t num_checked;
  bool ret = cpfs::server::ds::ValidateChecksum(root, &num_checked);
  std::printf("Checked %" PRIu64 " inodes\n", num_checked);
  std::printf(ret ? "No error found\n" : "Some error(s) found\n");
  return ret ? 0 : 1;
}

/**
 * Show position of a DSG offset.
 *
 * @param vm The parsed options
 */
int pos(const po::variables_map& vm) {
  if (!vm.count("inode")) {
    std::fprintf(stderr, "Inode number not set in pos mode\n");
    return 2;
  }
  if (!vm.count("cg_off")) {
    std::fprintf(stderr, "Checksum group offset not set in pos mode\n");
    return 2;
  }
  cpfs::GroupId gid = 0;
  uint64_t inode = vm["inode"].as<uint64_t>();
  cpfs::FileCoordManager fcm(inode, &gid, 1);
  uint64_t cg_off = vm["cg_off"].as<uint64_t>();
  cg_off = cg_off / cpfs::kChecksumGroupSize * cpfs::kChecksumGroupSize;
  std::printf("Inode %" PRIu64 ", Checksum group %" PRIu64 ":\n",
              inode, cg_off);
  std::vector<cpfs::Segment> segments;
  fcm.GetSegments(cg_off, cpfs::kChecksumGroupSize, &segments);
  for (unsigned i = 0; i < segments.size(); ++i) {
    std::printf("  Segment %u\n", i);
    std::printf("    Offset %zu (0x%zx)\n",
                segments[i].file_off, segments[i].file_off);
    std::printf("    Data server %u\n", segments[i].dsr_data);
    std::printf("    Data file location: %zu (0x%zx)\n",
                cpfs::DsgToDataOffset(segments[i].file_off),
                cpfs::DsgToDataOffset(segments[i].file_off));
  }
  std::printf("  Checksum server %u\n", segments[0].dsr_checksum);
  std::printf("  Checksum file location: %zu (0x%zx)\n",
              cpfs::DsgToChecksumOffset(segments[0].file_off),
              cpfs::DsgToChecksumOffset(segments[0].file_off));
  return 0;
}

/**
 * Main entry point
 *
 * @param argc The number of arguments
 *
 * @param argv The arguments
 */
int main(int argc, char** argv) {
  pantheios_be_file_setFilePath("/dev/stderr", 0, 0, PANTHEIOS_BEID_ALL);
  cpfs::SetSeverityCeiling(0, pantheios::SEV_WARNING);
  po::options_description desc("Allowed options");
  po::variables_map vm;
  if (!ParseOpts(argc, argv, &desc, &vm))
    return 1;
  if (vm.count("help")) {
    std::stringstream desc_ss;
    desc.print(desc_ss);
    std::fprintf(stderr, "%s", desc_ss.str().c_str());
    return 0;
  }
  if (vm["mode"].as<std::string>() == "validate")
    return validate(vm);
  if (vm["mode"].as<std::string>() == "pos")
    return pos(vm);
  std::fprintf(stderr, "Mode not recognized: %s\n",
               vm["mode"].as<std::string>().c_str());
  return 1;
}
