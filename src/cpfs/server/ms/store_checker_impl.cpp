/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IStoreChecker interface and plugins.
 */

#include "server/ms/store_checker_impl.hpp"

#include <sys/stat.h>

#include <cstddef>
#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/scoped_ptr.hpp>

#include "common.hpp"
#include "console.hpp"
#include "dir_iterator.hpp"
#include "store_util.hpp"
#include "server/ms/store.hpp"
#include "server/ms/store_checker.hpp"
#include "server/ms/store_impl.hpp"
#include "server/server_info.hpp"
#include "server/server_info_impl.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implement the IStoreChecker interface.
 */
class StoreChecker : public IStoreChecker {
 public:
  /**
   * @param data_path The data directory
   *
   * @param console The console used for printing
   */
  StoreChecker(std::string data_path, IConsole* console);
  void RegisterPlugin(IStoreCheckerPlugin* plugin);
  bool Run(bool do_correct);
  std::string GetRoot();
  std::string GetInodePath(InodeNum inode);
  std::string GetDentryPath(InodeNum parent, std::string name);
  IConsole* GetConsole();
  IStore* GetStore();

 private:
  std::string data_path_;
  boost::scoped_ptr<IServerInfo> server_info_;
  boost::scoped_ptr<IStore> store_;
  boost::scoped_ptr<IConsole> console_;
  boost::ptr_vector<IStoreCheckerPlugin> plugins_;
  bool valid_;

  void ScanInfo();
  void ScanInodes();
  void ScanDentries();

  bool SplitInodeName(std::string name, InodeNum* inode_ret, int* copy_ret) {
    size_t size = name.size();
    size_t dpos = name.find('.');
    size_t xpos = name.find('x');
    if (dpos != std::string::npos && dpos > 0) {
      size = dpos;
      name[dpos] = '\0';
      try {
        *copy_ret = boost::lexical_cast<int>(name.substr(dpos + 1));
      } catch (boost::bad_lexical_cast) {
        return false;
      }
    } else if (xpos != std::string::npos) {
      size = xpos;
      name[xpos] = '\0';
      *copy_ret = -1;
    }
    if (!ParseInodeNum(name.data(), size, inode_ret))
      return false;
    std::string reconstructed = GetInodeStr(*inode_ret);
    if (dpos != std::string::npos) {
      reconstructed += std::string(1, '\0') +
          boost::lexical_cast<std::string>(*copy_ret);
    } else if (xpos != std::string::npos) {
      reconstructed += std::string(1, '\0');
    }
    return reconstructed == name;
  }

  // The following are placed here to avoid a Doxygen bug
/** @cond */
#define STORE_CHECKER_RUN_PLUGINS(method, ...)                          \
  BOOST_FOREACH(                                                        \
      IStoreCheckerPlugin& plugin,                                      \
      plugins_) {                                                       \
    valid_ = plugin.method(__VA_ARGS__) && valid_;                      \
  }

#define HANDLE_SKIP_ITEM(code)                                          \
  try {                                                                 \
    code;                                                               \
  } catch (const SkipItemException& exc) {                              \
    if (exc.is_error)                                                   \
      valid_ = false;                                                   \
    continue;                                                           \
  }                                                                     \
/** @endcond */
};

StoreChecker::StoreChecker(std::string data_path, IConsole* console)
    : data_path_(data_path),
      server_info_(MakeServerInfo(data_path)),
      store_(MakeStore(data_path)),
      console_(console),
      valid_(true) {}

void StoreChecker::RegisterPlugin(IStoreCheckerPlugin* plugin) {
  plugins_.push_back(plugin);
}

bool StoreChecker::Run(bool do_correct) {
  STORE_CHECKER_RUN_PLUGINS(Init, this, do_correct);
  ScanInfo();
  ScanInodes();
  ScanDentries();
  return valid_;
}

void StoreChecker::ScanInfo() {
  std::vector<std::string> keys = server_info_->List();
  BOOST_FOREACH(std::string key, keys) {
    std::string value = server_info_->Get(key);
    HANDLE_SKIP_ITEM(STORE_CHECKER_RUN_PLUGINS(InfoFound, key, value));
  }
  STORE_CHECKER_RUN_PLUGINS(InfoScanCompleted);
}

void StoreChecker::ScanInodes() {
  boost::scoped_ptr<IDirIterator> dir_iter(store_->List(""));
  std::string name;
  bool is_dir;
  struct stat stbuf;
  while (dir_iter->GetNext(&name, &is_dir, &stbuf)) {
    if (name.size() >= kBaseDirNameLen + 1 && name[kBaseDirNameLen] == '/') {
      InodeNum inode;
      int copy = 0;
      std::string inode_s = StripPrefixInodeStr(name);
      if (SplitInodeName(inode_s, &inode, &copy)) {
        HANDLE_SKIP_ITEM(STORE_CHECKER_RUN_PLUGINS(InodeFound, inode,
                                                   &stbuf, copy));
        continue;
      }
    }
    HANDLE_SKIP_ITEM(STORE_CHECKER_RUN_PLUGINS(UnknownNodeFound, name));
    console_->PrintLine(std::string("Unrecognized inode entry ") +
                        GetRoot() + "/" + name);
    valid_ = false;
  }
  STORE_CHECKER_RUN_PLUGINS(InodeScanCompleted);
}

void StoreChecker::ScanDentries() {
  std::vector<InodeNum> pending;
  pending.push_back(1);
  while (!pending.empty()) {
    InodeNum dir_inode = pending.back();
    pending.pop_back();
    std::vector<InodeNum> subdirs;
    boost::scoped_ptr<IDirIterator> dir_iter(
        store_->List(GetPrefixedInodeStr(dir_inode)));
    std::string name;
    bool is_dir;
    struct stat stbuf;
    while (dir_iter->GetNext(&name, &is_dir, &stbuf)) {
      InodeNum child_inode;
      struct stat stbuf;
      std::string dentry_path = GetDentryPath(dir_inode, name);
      if (DentryToInode(dentry_path, &child_inode, &stbuf) == 0) {
        HANDLE_SKIP_ITEM(STORE_CHECKER_RUN_PLUGINS(DentryFound, dir_inode, name,
                                                   child_inode, &stbuf));
        if (is_dir && S_ISDIR(stbuf.st_mode))
          subdirs.push_back(child_inode);
      } else {
        HANDLE_SKIP_ITEM(STORE_CHECKER_RUN_PLUGINS(
            UnknownDentryFound, dir_inode, name));
        console_->PrintLine(std::string("Unrecognized dentry entry ") +
                            GetRoot() + "/" + GetInodePath(dir_inode) + name);
        valid_ = false;
      }
    }
    pending.insert(pending.end(), subdirs.rbegin(), subdirs.rend());
  }
  STORE_CHECKER_RUN_PLUGINS(DentryScanCompleted);
}

#undef HANDLE_SKIP_ITEM
#undef STORE_CHECKER_RUN_PLUGINS

std::string StoreChecker::GetRoot() {
  return data_path_;
}

std::string StoreChecker::GetInodePath(InodeNum inode) {
  return GetRoot() + "/" + GetPrefixedInodeStr(inode);
}

std::string StoreChecker::GetDentryPath(InodeNum parent, std::string name) {
  return GetRoot() + "/" + GetPrefixedInodeStr(parent) + "/" + name;
}

IConsole* StoreChecker::GetConsole() {
  return console_.get();
}

IStore* StoreChecker::GetStore() {
  return store_.get();
}

}  // namespace

IStoreChecker* MakeStoreChecker(std::string data_path, IConsole* console) {
  return new StoreChecker(data_path, console);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
