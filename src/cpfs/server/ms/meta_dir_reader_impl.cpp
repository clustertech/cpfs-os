/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IMetaDirReader interface for
 * resynchronization.
 */

#include "server/ms/meta_dir_reader_impl.hpp"

#include <dirent.h>
#include <stdint.h>

#include <sys/stat.h>

#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "store_util.hpp"
#include "server/ms/meta_dir_reader.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implement interface for reading meta-server data directory.
 */
class MetaDirReader : public IMetaDirReader {
 public:
  /**
   * @param data_path Path to data directory
   */
  explicit MetaDirReader(const std::string& data_path)
      : data_path_(data_path) {}

  FIM_PTR<ResyncInodeFim> ToInodeFim(const std::string& prefixed_inode_s) {
    std::string path = GetRoot() + "/" + prefixed_inode_s;
    std::string name = StripPrefixInodeStr(prefixed_inode_s);
    InodeNum inode;
    ParseInodeNum(name.c_str(), name.length(), &inode);
    FIM_PTR<ResyncInodeFim> rfim = ResyncInodeFim::MakePtr();
    (*rfim)->inode = inode;
    (*rfim)->extra_size = 0;
    if (GetFileAttr(path, &((*rfim)->fa)) < 0)
      throw std::runtime_error("Cannot read " + path);
    std::string xattrs = XAttrListToBuffer(DumpXAttr(path));
    uint32_t st_mode = ((*rfim)->fa).mode;
    if (S_ISREG(st_mode)) {
      // File: Put server group info to tail buffer
      std::vector<GroupId> groups;
      GetFileGroupIds(path, &groups);
      std::size_t groups_size = groups.size() * sizeof(GroupId);
      (*rfim)->extra_size = groups_size;
      rfim->tail_buf_resize(groups_size + xattrs.size());
      std::memcpy(rfim->tail_buf(), groups.data(), groups_size);
    } else if (S_ISDIR(st_mode)) {
      // Directory: Put parent inode to tail buffer
      InodeNum parent = GetDirParent(path);
      (*rfim)->extra_size = sizeof(parent);
      rfim->tail_buf_resize(sizeof(parent) + xattrs.size());
      std::memcpy(rfim->tail_buf(), &parent, sizeof(parent));
    } else if (S_ISLNK(st_mode)) {
      // Symlink: Put link target to tail buffer
      std::vector<char> link_target;
      ReadLinkTarget(path, &link_target);
      (*rfim)->extra_size = link_target.size() + 1;
      rfim->tail_buf_resize(link_target.size() + 1 + xattrs.size());
      rfim->tail_buf()[link_target.size()] = '\0';
      std::memcpy(rfim->tail_buf(), link_target.data(), link_target.size());
    }
    // Extended attributes
    std::memcpy(rfim->tail_buf() + (*rfim)->extra_size,
                xattrs.data(), xattrs.size());
    return rfim;
  }

  FIM_PTR<ResyncDentryFim> ToDentryFim(
      const std::string& prefixed_parent_s,
      const std::string& name, char type) {
    std::string dentry_path = GetRoot() + "/" + prefixed_parent_s + "/" + name;
    FIM_PTR<ResyncDentryFim> fim
        = ResyncDentryFim::MakePtr(name.length() + 1);
    std::memcpy(fim->tail_buf(), name.c_str(), name.length());
    fim->tail_buf()[name.length()] = '\0';
    std::string parent = prefixed_parent_s.substr(kBaseDirNameLen + 1);
    ParseInodeNum(parent.c_str(), parent.length(), &((*fim)->parent));
    struct stat stbuf;
    if (lstat(dentry_path.c_str(), &stbuf) < 0)
      throw std::runtime_error("Cannot stat " + dentry_path);
    (*fim)->uid = stbuf.st_uid;
    (*fim)->gid = stbuf.st_gid;
    if (type == 'D') {
      InodeNum target_inode;
      XattrToInode(dentry_path, &target_inode);
      (*fim)->type = DT_DIR;
      (*fim)->target = target_inode;
    } else {
      InodeNum target_inode;
      SymlinkToInodeFT(dentry_path, &target_inode, &(*fim)->type);
      (*fim)->target = target_inode;
    }
    return fim;
  }

 private:
  std::string data_path_;

  std::string GetRoot() {
    return data_path_;
  }
};

}  // namespace

IMetaDirReader* MakeMetaDirReader(const std::string& data_path) {
  return new MetaDirReader(data_path);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
