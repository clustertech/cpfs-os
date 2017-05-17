/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for directory diff
 */
#include "meta_dir_diff.hpp"

#include <sys/stat.h>

#include <cstring>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/scoped_ptr.hpp>

#include "dir_iterator.hpp"
#include "dir_iterator_impl.hpp"
#include "logger.hpp"
#include "store_util.hpp"
#include "server/server_info.hpp"
#include "server/server_info_impl.hpp"

namespace cpfs {
namespace systest {
namespace {

/** Type to represent set of directory entries */
typedef std::set<std::string> DirSet;

/**
 * Represent a metadata directory comparison run.
 */
class MetaDirDiffRun {
 public:
  /**
   * @param dir1 The first directory to compare
   *
   * @param dir2 The second directory to compare
   */
  MetaDirDiffRun(std::string dir1, std::string dir2)
      : dir1_(dir1), dir2_(dir2), err_(false) {
    if (dir1_.empty() || dir1_[dir1.size() - 1] != '/')
      dir1_ += '/';
    if (dir2_.empty() || dir1_[dir2.size() - 1] != '/')
      dir2_ += '/';
  }

  /**
   * Run the full diff.
   *
   * @param num_checked_ret Where to return the number of inodes checked
   */
  bool operator()(uint64_t* num_checked_ret) {
    *num_checked_ret = 0;
    DirSet dir_set1 = LoadDir(dir1_);
    dir_set1.erase("c");
    dir_set1.erase("r");
    DirSet dir_set2 = LoadDir(dir2_);
    dir_set2.erase("c");
    dir_set2.erase("r");
    DirSet common_dir_set = CompareDirSets("", dir_set1, dir_set2);
    for (DirSet::iterator it = common_dir_set.begin();
         it != common_dir_set.end();
         ++it) {
      ++*num_checked_ret;
      CompareInodes(*it, false);
    }
    return !err_;
  }

 private:
  std::string dir1_;
  std::string dir2_;
  bool err_;

  /**
   * Compare the inodes at a path under the two top-level directories
   * for differences in attributes and contents.  The two inodes are
   * assumed to be existing.
   *
   * @param path The path
   *
   * @param skip_owner Whether owner check should be skipped
   */
  void CompareInodes(const std::string& path, bool skip_owner) {
    MetaData md1(dir1_ + path);
    MetaData md2(dir2_ + path);
    std::vector<std::string> diff;
    if (md1.Diff(md2, &diff, skip_owner)) {
      error("Meta data for inodes differs: " + path + ", diff: " +
            boost::algorithm::join(diff, ", "));
    }
    if (path.find("/") == path.npos && md1.is_dir() && md2.is_dir()) {
      DirSet dir_set1 = LoadDir(dir1_ + path);
      DirSet dir_set2 = LoadDir(dir2_ + path);
      DirSet common_dir_set = CompareDirSets(path + "/", dir_set1, dir_set2);
      for (DirSet::iterator it = common_dir_set.begin();
           it != common_dir_set.end();
           ++it)
        CompareInodes(path + "/" + *it, true);
    }
  }

  /**
   * Load a directory into a set.
   *
   * @param path The path of the directory
   */
  DirSet LoadDir(const std::string& path) {
    DirSet ret;
    {
      boost::scoped_ptr<IDirIterator> dir_iter(MakeDirIterator(path));
      if (dir_iter->missing())
        throw std::runtime_error(path + " is not a directory");
      std::string name;
      bool is_dir;
      while (dir_iter->GetNext(&name, &is_dir))
        ret.insert(name);
    }
    return ret;
  }

  /**
   * Compare two directory sets, emit errors as they are found.  Return
   * common entries.
   *
   * @param path The path under the top-level directory where the
   * directory sets indicates
   *
   * @param dir_set1 The first directory set
   *
   * @param dir_set1 The second directory set
   *
   * @return Common entries in the two directory sets
   */
  DirSet CompareDirSets(const std::string& path,
                        const DirSet& dir_set1, const DirSet& dir_set2) {
    DirSet ret;
    {
      DirSet::iterator it1 = dir_set1.begin();
      DirSet::iterator it2 = dir_set2.begin();
      while (it1 != dir_set1.end() || it2 != dir_set2.end()) {
        if (it2 == dir_set2.end() ||
            (it1 != dir_set1.end() && *it1 < *it2)) {
          error("No correspondance of " + dir1_ + path + *it1 + " in " + dir2_);
          ++it1;
        } else if (it1 == dir_set1.end() ||
                   (it2 != dir_set2.end() && *it2 < *it1)) {
          error("No correspondance of " + dir2_ + path + *it2 + " in " + dir1_);
          ++it2;
        } else {
          ret.insert(*it1);
          ++it1;
          ++it2;
        }
      }
    }
    return ret;
  }

  void error(const std::string& msg) {
    LOG(error, Server, msg);
    err_ = true;
  }
};

}  // namespace

MetaData::MetaData(const std::string& path) : path_(path) {
  memset(&stbuf_, 0, sizeof(struct stat));
  link_.clear();
  xattrs_.clear();
  if (lstat(path_.c_str(), &stbuf_) != 0)
    throw std::runtime_error("Cannot stat: " + path_);
  if (S_ISLNK(stbuf_.st_mode))
    ReadLinkTarget(path_, &link_);
  ReadExtendAttrs_();
}

bool MetaData::Diff(const MetaData& rhs, std::vector<std::string>* diff_ret,
                    bool skip_owner = false)
    const {
  bool ret = false;
  /** @cond */
#define META_DIR_DIFF_CHECK(field, msg)                                 \
  do {                                                                  \
      if (field != rhs.field) {                                         \
        ret = true;                                                     \
        diff_ret->push_back(msg);                                       \
      }                                                                 \
  } while (0)
  META_DIR_DIFF_CHECK(stbuf_.st_mode, "mode");
  META_DIR_DIFF_CHECK(stbuf_.st_nlink, "nlink");
  if (!skip_owner) {
    META_DIR_DIFF_CHECK(stbuf_.st_uid, "uid");
    META_DIR_DIFF_CHECK(stbuf_.st_gid, "gid");
  }
  META_DIR_DIFF_CHECK(stbuf_.st_rdev, "rdev");
  META_DIR_DIFF_CHECK(link_, "link target");
  META_DIR_DIFF_CHECK(xattrs_, "xattrs");
  if (S_ISREG(stbuf_.st_mode))
    META_DIR_DIFF_CHECK(stbuf_.st_size, "size");
#undef META_DIR_DIFF_CHECK
  /** @endcond */
  return ret;
}

void MetaData::ReadExtendAttrs_() {
  boost::scoped_ptr<server::IServerInfo> info(server::MakeServerInfo(path_));
  std::vector<std::string> keys = info->List();
  for (unsigned i = 0; i < keys.size(); ++i)
    xattrs_[keys[i]] = info->Get(keys[i]);
}

bool MetaData::is_dir() {
  return S_ISDIR(stbuf_.st_mode);
}

bool MetaDirDiff(std::string dir1, std::string dir2,
                 uint64_t* num_checked_ret) {
  return MetaDirDiffRun(dir1, dir2)(num_checked_ret);
}

}  // namespace systest
}  // namespace cpfs
