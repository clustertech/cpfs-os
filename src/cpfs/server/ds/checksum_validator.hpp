#pragma once
/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for ValidateChecksum
 */
#include <stdint.h>

#include <string>

namespace cpfs {
namespace server {
namespace ds {

/**
 * Validate checksum among directories for a DSG.  The directories
 * must be located at ds1, ds2, ds3, ds4 and ds5 under a particular
 * directory.
 *
 * @param root_path The path to the working directory with DS folders
 *
 * @param num_checked_ret Where to return number of files checked
 *
 * @return Whether check succeed
 */
bool ValidateChecksum(const std::string& root_path, uint64_t* num_checked_ret);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
