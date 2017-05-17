#pragma once
/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for MetaDirDiff
 */
#include <stdint.h>

#include <sys/stat.h>

#include <map>
#include <string>
#include <vector>

namespace cpfs {
namespace systest {

/**
 * Represent the meta data loaded from a file path. Equality check can
 * be done with this class to determine if two meta data are equal
 */
class MetaData {
 public:
  /**
   * @param path The full path
   */
  explicit MetaData(const std::string& path);

  /**
   * Determine equality
   *
   * @param rhs The meta data to compare with
   *
   * @param diff_ret Where to return the difference found
   *
   * @param skip_owner Whether owner check should be skipped
   *
   * @return True if equal
   */
  bool Diff(const MetaData& rhs, std::vector<std::string>* diff_ret,
            bool skip_owner) const;

  /**
   * @return Whether the path is a directory
   */
  bool is_dir();

 private:
  std::string path_;
  struct stat stbuf_;
  std::vector<char> link_;
  std::map<std::string, std::string> xattrs_;

  void ReadExtendAttrs_();
};

/**
 * Compare two metadata server directories to check whether the size,
 * uid, gid, permissions etc, link target and xattr of its inodes are
 * identical.
 *
 * @param dir1 The first path
 *
 * @param dir2 The second path
 *
 * @param num_checked_ret Where to return number of files checked
 *
 * @return The number of files checked if the folders are identical
 */
bool MetaDirDiff(std::string dir1, std::string dir2,
                 uint64_t* num_checked_ret);

}  // namespace systest
}  // namespace cpfs
