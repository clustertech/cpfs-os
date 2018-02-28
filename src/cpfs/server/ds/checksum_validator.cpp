/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for checking data in a DS group
 */
#include "checksum_validator.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstring>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "logger.hpp"
#include "store_util.hpp"
#include "util.hpp"
#include "server/ds/store.hpp"
#include "server/ds/store_impl.hpp"

namespace fs = boost::filesystem;

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * Record of data fetched from a checksum group.
 */
struct CgIterRec {
  /**
   * The underlying checksum group iterator
   */
  boost::shared_ptr<IChecksumGroupIterator> iter;
  std::size_t cg_off; /**< The last fetched checksum group offset */
  std::size_t size; /**< The last fetched buffer size */
  char buf[kSegmentSize]; /**< Buffer holding fetched data */

  /**
   * @param iter The underlying checksum group iterator
   */
  explicit CgIterRec(IChecksumGroupIterator* iter) : iter(iter) {}

  /**
   * Read the next segment of data from the checksum group iterator.
   */
  void Refill() {
    size = iter->GetNext(&cg_off, buf);
  }
};

/**
 * Validator for a DS group.
 */
class DSGroupValidator : boost::noncopyable {
 public:
  /**
   * @param group The group Id
   */
  explicit DSGroupValidator(GroupId group) : group_(group) {}

  /**
   * Add a store to the validator.
   */
  void AddStore(IStore* store) {
    stores_.push_back(boost::shared_ptr<IStore>(store));
  }

  /**
   * Check all inodes in the DS group.
   *
   * @param num_inode_checked_ret The inode check count to modify
   */
  bool CheckAll(uint64_t* num_inode_checked_ret) {
    SetInodeIter(0);
    bool ret = true;
    InodeNum inode;
    while ((inode = GetInode())) {
      ++*num_inode_checked_ret;
      if (!CheckInode(inode))
        ret = false;
    }
    return ret;
  }

 private:
  GroupId group_; /**< The group Id */
  std::vector<boost::shared_ptr<IStore> > stores_; /**< The stores */
  boost::unordered_set<InodeNum> visited_inodes_; /**< Checked inodes */
  unsigned inode_lister_idx_; /**< Next index in inode_lister_ to check */
  boost::scoped_ptr<IDirIterator> inode_lister_; /**< Where to find inodes */

  void SetInodeIter(unsigned idx) {
    inode_lister_idx_ = idx;
    if (idx < stores_.size())
      inode_lister_.reset(stores_[idx]->List());
  }

  InodeNum GetInode() {
    while (true) {
      std::string name;
      bool is_dir;
      if (inode_lister_idx_ == stores_.size())
        return 0;
      if (!inode_lister_->GetNext(&name, &is_dir)) {
        SetInodeIter(inode_lister_idx_ + 1);
        continue;
      }
      if (name.size() < 6)
        continue;
      name = StripPrefixInodeStr(name);
      if (name.substr(name.size() - 2) == ".d" && !is_dir) {
        name = name.substr(0, name.size() - 2);
        InodeNum ret;
        if (ParseInodeNum(name.c_str(), name.size(), &ret)
            && visited_inodes_.find(ret) == visited_inodes_.end()) {
          visited_inodes_.insert(ret);
          return ret;
        }
      }
    }
  }

  bool CheckInode(InodeNum inode) {
    std::vector<CgIterRec> records;
    for (unsigned i = 0; i < stores_.size(); ++i) {
      records.push_back(CgIterRec(stores_[i]->GetChecksumGroupIterator(inode)));
      records.back().Refill();
    }
    const char null_bytes[kSegmentSize] = {0};
    for (;;) {
      for (int i = records.size() - 1; i >= 0; --i)
        if (records[i].size == 0)
          records.erase(records.begin() + i);
      if (records.size() == 0)
        return true;
      std::size_t min_cg_off = records[0].cg_off;
      for (unsigned i = 1; i < records.size(); ++i)
        min_cg_off = std::min(min_cg_off, records[i].cg_off);
      char xored_data[kSegmentSize] = {0};
      for (unsigned i = 0; i < records.size(); ++i) {
        if (records[i].cg_off == min_cg_off) {
          XorBytes(xored_data, records[i].buf, records[i].size);
          records[i].Refill();
        }
      }
      if (std::memcmp(xored_data, null_bytes, kSegmentSize) != 0) {
        for (unsigned pos = 0; pos < kSegmentSize; ++pos) {
          if (xored_data[pos] == 0)
            continue;
          LOG(error, Store, "Inode ", PHex(inode), " is corrupted at offset ",
              PVal(min_cg_off + pos), " in group ", PVal(group_));
          break;
        }
        return false;
      }
    }
  }
};

}  // namespace

bool ValidateChecksum(const std::string& root_path,
                      uint64_t* num_inode_checked_ret) {
  typedef std::map<GroupId, boost::shared_ptr<DSGroupValidator> >
      GroupValidatorMap;
  if (!fs::exists(root_path))
    throw std::runtime_error("Cannot find the root path");
  GroupValidatorMap validators;
  for (fs::directory_iterator dir_iter = fs::directory_iterator(root_path);
       dir_iter != fs::directory_iterator();
       ++dir_iter) {
    std::string name = dir_iter->path().filename().string();
    if (name.substr(0, 2) == "ds" && isdigit(name[name.size() - 1])) {
      UNIQUE_PTR<IStore> store(MakeStore(dir_iter->path().string()));
      if (!store->is_role_set())
        continue;
      GroupId group = store->ds_group();
      if (!validators[group])
        validators[group].reset(new DSGroupValidator(group));
      validators[group]->AddStore(store.release());
    }
  }
  bool ret = true;
  *num_inode_checked_ret = 0;
  for (GroupValidatorMap::iterator it = validators.begin();
       it != validators.end();
       ++it)
    if (!it->second->CheckAll(num_inode_checked_ret))
      ret = false;
  return ret;
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
