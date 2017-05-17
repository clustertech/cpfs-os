/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IStoreChecker interface and plugins.
 */
#include "server/ms/store_checker_plugin_impl.hpp"

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/format.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/scoped_array.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "console.hpp"
#include "finfo.hpp"
#include "store_util.hpp"
#include "server/ms/store_checker.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Base store checker plugin implementation, to be used by other plugins.
 */
class BaseStoreCheckerPlugin : public IStoreCheckerPlugin {
 public:
  BaseStoreCheckerPlugin() : checker_(0), do_correct_(false) {}

  bool Init(IStoreChecker* checker, bool do_correct) {
    checker_ = checker;
    do_correct_ = do_correct;
    return true;
  }

  bool InfoFound(std::string name, std::string value) {
    (void) name;
    (void) value;
    return true;
  }

  bool InfoScanCompleted() {
    return true;
  }

  bool InodeFound(InodeNum inode, struct stat* stbuf, int copy) {
    (void) inode;
    (void) stbuf;
    (void) copy;
    return true;
  }

  bool UnknownNodeFound(std::string name) {
    (void) name;
    return true;
  }

  bool InodeScanCompleted() {
    return true;
  }

  bool DentryFound(InodeNum parent, std::string name,
                   InodeNum inode, struct stat* stbuf) {
    (void) parent;
    (void) name;
    (void) inode;
    (void) stbuf;
    return true;
  }

  bool UnknownDentryFound(InodeNum parent, std::string name) {
    (void) parent;
    (void) name;
    return true;
  }

  bool DentryScanCompleted() {
    return true;
  }

 protected:
  IStoreChecker* checker_; /**< The checker using the plugin */
  bool do_correct_; /**< Whether to do correction */
};

/**
 * A store checker plugin that checks the directory tree structure and
 * link counts
 */
class TreeStoreCheckerPlugin : public BaseStoreCheckerPlugin {
 public:
  bool InodeFound(InodeNum inode, struct stat* stbuf, int copy) {
    InodeData& data = inode_data_map_[inode];
    if (copy < 0)
      return true;
    bool ok = true;
    if (data.link_files_found == 0) {
      std::string inode_path = checker_->GetInodePath(inode);
      FSTime ctime;
      if (!GetCtrlCtime(inode_path, &ctime)) {
        checker_->GetConsole()->PrintLine(
            "Cannot find ctime for " + inode_path);
        ok = false;
      }
      std::vector<GroupId> groups;
      if (S_ISREG(stbuf->st_mode) &&
          GetFileGroupIds(inode_path, &groups) != 0) {
        checker_->GetConsole()->PrintLine(
            "Cannot determine DSG allocation for " + inode_path);
        ok = false;
      }
    }
    data.is_dir = S_ISDIR(stbuf->st_mode);
    data.link_count = stbuf->st_nlink;
    ++data.link_files_found;
    data.max_link_file_found = std::max(data.max_link_file_found,
                                        unsigned(copy));
    return ok;
  }

  bool DentryFound(InodeNum parent, std::string name,
                   InodeNum inode, struct stat* stbuf) {
    bool ok = true;
    bool is_dir = S_ISDIR(stbuf->st_mode);
    InodeData& data = inode_data_map_[inode];
    if (is_dir != data.is_dir) {
      std::string dentry_path = checker_->GetDentryPath(parent, name);
      checker_->GetConsole()->PrintLine(
          "Dentry type does not match inode type for " + dentry_path +
          (is_dir ? " (dir)" : " (file)") + " vs " +
          checker_->GetInodePath(inode) +
          (data.is_dir ? " (dir)" : " (file)"));
      throw SkipItemException(dentry_path);
    }
    if (is_dir) {
      std::string dentry_path = checker_->GetDentryPath(parent, name);
      std::string inode_path = checker_->GetInodePath(inode);
      if (data.dentry_links_found > 0) {
        checker_->GetConsole()->PrintLine(
            "Multiple dentries found for directory inode " +
            inode_path + ": " + dentry_path);
        throw SkipItemException(dentry_path);
      }
      InodeNum attr_par_inode = 0;
      try {
        attr_par_inode = GetDirParent(inode_path);
      } catch (const std::runtime_error& err) {
      }
      if (attr_par_inode == 0) {
        checker_->GetConsole()->PrintLine(
            "No parent set for directory inode " + inode_path);
        ok = false;
      } else if (attr_par_inode != parent) {
        checker_->GetConsole()->PrintLine(
            "Parent mismatched for " + dentry_path + " vs " + inode_path);
        ok = false;
      }
    }
    ++data.dentry_links_found;
    return ok;
  }

  bool DentryScanCompleted() {
    bool ok = true;
    ++inode_data_map_[1].dentry_links_found;
    for (InodeDataMap::iterator it = inode_data_map_.begin();
         it != inode_data_map_.end();
         ++it) {
      InodeData& inode_data = it->second;
      if (inode_data.link_files_found == 0 &&
          inode_data.dentry_links_found == 0) {
        checker_->GetConsole()->PrintLine(
            (boost::format("Inode control file found for missing inode %s") %
             checker_->GetInodePath(it->first)).str());
        ok = false;
        continue;
      } else if (inode_data.dentry_links_found == 0) {
        checker_->GetConsole()->PrintLine(
            (boost::format("Dangling inode %s") %
             checker_->GetInodePath(it->first)).str());
        ok = false;
      }
      if (inode_data.is_dir) {
        if (inode_data.max_link_file_found > 0) {
          checker_->GetConsole()->PrintLine(
              (boost::format("Directory link inode %s found (max %d)") %
               checker_->GetInodePath(it->first) %
               inode_data.max_link_file_found).str());
          ok = false;
        }
      } else {
        unsigned dentry_nl = inode_data.dentry_links_found;
        if (inode_data.link_count != dentry_nl ||
            inode_data.link_files_found != dentry_nl ||
            inode_data.max_link_file_found + 1 != dentry_nl) {
          checker_->GetConsole()->PrintLine(
              (boost::format("File link counts for %s mismatched: "
                             "dentry count %d, link count %d, "
                             "link files %d, max link file %d") %
               checker_->GetInodePath(it->first) %
               inode_data.dentry_links_found % inode_data.link_count %
               inode_data.link_files_found %
               inode_data.max_link_file_found).str());
          ok = false;
        }
      }
    }
    return ok;
  }

 private:
  struct InodeData {
    InodeData()
        : is_dir(false), link_count(0),
          link_files_found(0), max_link_file_found(0), dentry_links_found(0) {}
    bool is_dir; /**< Whether inode file is a directory */
    unsigned link_count; /**< Value found in stat */
    unsigned link_files_found; /**< Number of .n files */
    unsigned max_link_file_found; /**< Maximum n among .n files */
    unsigned dentry_links_found; /**< Number of dentry linking to this file */
  };
  typedef boost::unordered_map<InodeNum, InodeData> InodeDataMap;
  InodeDataMap inode_data_map_;
};

/**
 * A store checker plugin that lets special inode files to pass
 */
class SpecialInodeStoreCheckerPlugin : public BaseStoreCheckerPlugin {
 public:
  bool UnknownNodeFound(std::string name) {
    if (name == "d" || name == "m" || name == "r" || name == "u")
      throw SkipItemException(name, false);
    return true;
  }
};

/**
 * Value from, and needs to be in sync with that in
 * inode_src_impl.cpp.  We ensure that all inode number hole ranges
 * after the inode found in the /c link are at most this long.
 */
const int kUnusedFuzzy = 20;

/**
 * Value from, and needs to be in sync with that in
 * inode_src_impl.cpp.  We ensure that all inode numbers are at most
 * this number greater than the value found in the /c link.
 */
const int kLazyFactor = 2000;

/**
 * A store checker plugin that checks the inode count link validity.
 */
class BaseInodeCountStoreCheckerPlugin : public BaseStoreCheckerPlugin {
 public:
  /**
   * @param dir The subdirectory within root to check
   */
  explicit BaseInodeCountStoreCheckerPlugin(const std::string& dir)
      : c_link_count_(0), max_inode_(0), dir_(dir) {}

  bool Init(IStoreChecker* checker, bool do_correct) {
    bool ret = BaseStoreCheckerPlugin::Init(checker, do_correct);
    int retcode =
        SymlinkToInode(checker_->GetRoot() + "/" + dir_ + "/c", &c_link_count_);
    if (retcode < 0 && retcode != ENOENT) {
      checker_->GetConsole()->PrintLine(
          (boost::format("Cannot interpret inode count link %s/%s/c") %
           checker_->GetRoot() % dir_).str());
      return false;
    }
    found_.reset(new bool[kLazyFactor + 1]);
    for (int i = 0; i <= kLazyFactor; ++i)
      found_[i] = false;
    return ret;
  }

  bool InodeFound(InodeNum inode, struct stat* stbuf, int copy) {
    (void) stbuf;
    if (copy == 0) {
      max_inode_ = std::max(max_inode_, inode);
      if (inode >= c_link_count_ && int(inode - c_link_count_) <= kLazyFactor)
        found_[inode - c_link_count_] = true;
    }
    return true;
  }

  bool UnknownNodeFound(std::string name) {
    if (name == "c")
      throw SkipItemException(name, false);
    return true;
  }

  bool InodeScanCompleted() {
    int last_idx = max_inode_ - c_link_count_;
    bool ret = last_idx <= kLazyFactor;
    if (!ret)
      checker_->GetConsole()->PrintLine(
          (boost::format("Inode count link value too small: is 0x%x, need 0x%x")
           % c_link_count_ % max_inode_).str());
    if (max_inode_ > c_link_count_) {
      int start = 0;
      last_idx = std::min(last_idx, kLazyFactor);
      for (int i = 0; i < last_idx; ++i) {
        if (found_[i]) {
          start = i + 1;
        } else if (i - start == kUnusedFuzzy) {
          checker_->GetConsole()->PrintLine(
              (boost::format("Too wide inode hole starting from 0x%x")
               % (start + c_link_count_)).str());
          ret = false;
        }
      }
    }
    return ret;
  }

 private:
  InodeNum c_link_count_; /**< /c link value */
  InodeNum max_inode_;
  boost::scoped_array<bool> found_;
  std::string dir_;
};

/**
 * Implement the InodeCountStoreCheckerPlugin
 */
class InodeCountStoreCheckerPlugin : public BaseStoreCheckerPlugin {
 public:
  InodeCountStoreCheckerPlugin() {
    for (unsigned i = 0; i < kNumBaseDir; ++i)
      plugins_.push_back(new BaseInodeCountStoreCheckerPlugin(
          (boost::format("%03x") % i).str()));
  }

  bool Init(IStoreChecker* checker, bool do_correct) {
    for (unsigned i = 0; i < plugins_.size(); ++i)
      if (!plugins_[i].Init(checker, do_correct))
        return false;
    return true;
  }

  bool InodeFound(InodeNum inode, struct stat* stbuf, int copy) {
    return plugins_[inode >> 52].InodeFound(inode, stbuf, copy);
  }

  bool UnknownNodeFound(std::string name) {
    if (name.size() >= kBaseDirNameLen + 1 && name[kBaseDirNameLen] == '/') {
      std::string prefix = name.substr(0, 3);
      const char* buf = prefix.c_str();
      char *endptr;
      int inode = std::strtoul(buf, &endptr, 16);
      if (*endptr == '\0')
        return plugins_[inode].UnknownNodeFound(StripPrefixInodeStr(name));
    }
    return true;
  }

  bool InodeScanCompleted() {
    for (unsigned i = 0; i < plugins_.size(); ++i)
      if (!plugins_[i].InodeScanCompleted())
        return false;
    return true;
  }

 private:
  boost::ptr_vector<BaseInodeCountStoreCheckerPlugin> plugins_;
};

}  // namespace

IStoreCheckerPlugin* MakeBaseStoreCheckerPlugin() {
  return new BaseStoreCheckerPlugin();
}

IStoreCheckerPlugin* MakeTreeStoreCheckerPlugin() {
  return new TreeStoreCheckerPlugin();
}

IStoreCheckerPlugin* MakeSpecialInodeStoreCheckerPlugin() {
  return new SpecialInodeStoreCheckerPlugin();
}

IStoreCheckerPlugin* MakeInodeCountStoreCheckerPlugin() {
  return new InodeCountStoreCheckerPlugin();
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
