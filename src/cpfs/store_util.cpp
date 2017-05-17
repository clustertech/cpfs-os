/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement utilities to handle stores.
 */

#include "store_util.hpp"

#include <dirent.h>
#include <stdint.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/xattr.h>

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "common.hpp"
#include "dir_iterator.hpp"
#include "dir_iterator_impl.hpp"
#include "finfo.hpp"
#include "logger.hpp"
#include "util.hpp"

namespace cpfs {

std::vector<std::string> LListUserXattr(const char* path) {
  std::vector<std::string> ret;
  {  // Prevent coverage false positive
    size_t size = 1024;
    boost::scoped_array<char> buf;
    ssize_t len = -1;
    while (len == -1) {
      size *= 2;
      buf.reset(new char[size]);
      len = llistxattr(path, buf.get(), size);
      if (len >= 0)
        break;
      if (len == -1 && errno != ERANGE)
        throw std::runtime_error(std::string("Failed listing xattr: ")
                                 + strerror(errno));
    }
    size_t pos = 0;
    while (pos < size_t(len)) {
      char* key = &buf[pos];
      size_t len = std::strlen(key);
      if (std::strncmp("user.", key, 5) == 0)
        ret.push_back(std::string(key + 5, len - 5));
      pos += len + 1;
    }
  }
  return ret;
}

std::string LGetUserXattr(const char* path, const std::string& key) {
  std::string full_key = "user." + key;
  size_t size = 1024;
  boost::scoped_array<char> buf;
  ssize_t len = -1;
  for (;;) {
    size *= 2;
    buf.reset(new char[size]);
    len = lgetxattr(path, full_key.c_str(), buf.get(), size);
    if (len >= 0)
      return std::string(buf.get(), len);
    if (len == -1 && errno != ERANGE)
      throw std::runtime_error(std::string("Failed getting xattr: ")
                               + strerror(errno));
  }
}

void LSetUserXattr(const char* path, const std::string& key,
                   const std::string& val) {
  std::string full_key = "user." + key;
  int ret = lsetxattr(path, full_key.c_str(), val.data(), val.size(), 0);
  if (ret < 0)
    throw std::runtime_error(std::string("Failed setting xattr: ")
                             + strerror(errno));
}

XAttrList DumpXAttr(const std::string& path) {
  XAttrList ret;
  {
    const char* cpath = path.c_str();
    try {
      BOOST_FOREACH(const std::string& key, LListUserXattr(cpath)) {
        std::string val;
        try {
          val = LGetUserXattr(cpath, key);
          ret.push_back(std::make_pair(key, val));
        } catch (const std::runtime_error&) {  // Defensive, cannot cover
        }
      }
    } catch (const std::runtime_error&) {  // Do nothing
    }
  }
  return ret;
}

std::string XAttrListToBuffer(const XAttrList& xattr_list) {
  std::string ret;
  std::size_t offset = 0;
  for (XAttrList::const_iterator itr = xattr_list.begin();
       itr != xattr_list.end(); ++itr) {
    // Serialize by encoding: <name size> <name> <value size> <value>
    const std::string& name = itr->first;
    const std::string& value = itr->second;
    uint64_t name_size = name.size();
    uint64_t value_size = value.size();
    uint64_t data_size = sizeof(name_size) + name_size
        + sizeof(value_size) + value_size;
    ret.resize(ret.size() + data_size);
    // Store xattr name
    std::memcpy(&ret[offset], &name_size, sizeof(name_size));
    offset += sizeof(name_size);
    std::memcpy(&ret[offset], name.data(), name_size);
    offset += name_size;
    // Store xattr value
    std::memcpy(&ret[offset], &value_size, sizeof(value_size));
    offset += sizeof(value_size);
    std::memcpy(&ret[offset], value.data(), value_size);
    offset += value_size;
  }
  return ret;
}

XAttrList BufferToXattrList(const std::string& buffer) {
  XAttrList ret;
  for (std::size_t offset = 0; offset < buffer.size();) {
    // Restore xattr name
    uint64_t name_size = *(reinterpret_cast<const uint64_t*>(&buffer[offset]));
    offset += sizeof(name_size);
    const char* name_p = reinterpret_cast<const char*>(&buffer[offset]);
    std::string name = std::string(name_p, name_size);
    offset += name_size;
    // Restore xattr value
    uint64_t value_size = *(reinterpret_cast<const uint64_t*>(&buffer[offset]));
    offset += sizeof(value_size);
    const char* value_p = reinterpret_cast<const char*>(&buffer[offset]);
    std::string value = std::string(value_p, value_size);
    offset += value_size;
    ret.push_back(std::make_pair(name, value));
  }
  return ret;
}

namespace {

/**
 * Parse the string to number
 *
 * @tparam TNum Number type
 *
 * @param buf String buffer
 *
 * @param len String length
 *
 * @param ret Number returned
 */
template <typename TNum>
bool _ParseNum(const char* buf, int len, TNum* ret) {
  if (std::size_t(len) >= (sizeof(TNum) * 2 + 1))
    return false;
  char* endptr;
  if (sizeof(TNum) >= 8)
    *ret = std::strtoull(buf, &endptr, 16);
  else
    *ret = std::strtoul(buf, &endptr, 16);
  if (endptr == buf || (*endptr != '\0' && *endptr != '-'))
    return false;
  return true;
}

/** @cond */
// std::min(...) and std::max(...) are not compile-time constant expressions
#define MIN(a, b) ((a) > (b) ? (b) : (a))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
/** @endcond */
// These characters on-purpose avoids the range 'A' to 'F', so that it
// is impossible to mistaken a file type representation character with
// the actual inode number component (in hex).
const char kFTChBlk = 'K';  /**< File type repr char for block devices */
const char kFTChChr = 'H';  /**< File type repr char for character devices */
const char kFTChDir = 'I';  /**< File type repr char for directories */
const char kFTChFifo = 'P';  /**< File type repr char for FIFO */
const char kFTChLnk = 'L';  /**< File type repr char for symlinks */
const char kFTChReg = 'R';  /**< File type repr char for regular files */
const char kFTChSock = 'S';  /**< File type repr char for sockets */
/** Mininum file type repr char */
const char kMinFTch =
    MIN(MIN(MIN(kFTChBlk, kFTChChr), MIN(kFTChDir, kFTChFifo)),
        MIN(MIN(kFTChLnk, kFTChReg), kFTChSock));
/** Maximum file type repr char */
const char kMaxFTch =
    MAX(MAX(MAX(kFTChBlk, kFTChChr), MAX(kFTChDir, kFTChFifo)),
        MAX(MAX(kFTChLnk, kFTChReg), kFTChSock));
const unsigned kMaxCh2FTIdx = kMaxFTch - kMinFTch; /**< Max index in kCh2FT */
/** Convert type char to file type; index off by kMinFTch */
static unsigned char kCh2FT[kMaxCh2FTIdx + 1] = {0};
/** Max index to kFT2Ch array */
const unsigned char kMaxFT = MAX(MAX(MAX(DT_BLK, DT_CHR), MAX(DT_DIR, DT_FIFO)),
                                 MAX(MAX(DT_LNK, DT_REG), DT_SOCK));
/** Convert file type to type char */
static char kFT2Ch[kMaxFT + 1] = {0};
#undef MIN
#undef MAX

/**
 * Class to initialize the kCh2FT and kFT2Ch arrays.
 */
class InitFTMaps {
 public:
  InitFTMaps() {
    kFT2Ch[DT_BLK] = kFTChBlk;
    kFT2Ch[DT_CHR] = kFTChChr;
    kFT2Ch[DT_DIR] = kFTChDir;
    kFT2Ch[DT_FIFO] = kFTChFifo;
    kFT2Ch[DT_LNK] = kFTChLnk;
    kFT2Ch[DT_REG] = kFTChReg;
    kFT2Ch[DT_SOCK] = kFTChSock;
    for (unsigned char i = 0; i < kMaxFT; ++i)
      if (kFT2Ch[i])
        kCh2FT[kFT2Ch[i] - kMinFTch] = i;
  }
} _init_ft_maps;  /**< Initializer variable */

}  // namespace

unsigned char Mod2FT(uint64_t mode) {
  // Order them with roughly decending expected frequency as optimization
  if (S_ISREG(mode))
    return DT_REG;
  if (S_ISDIR(mode))
    return DT_DIR;
  if (S_ISLNK(mode))
    return DT_LNK;
  if (S_ISCHR(mode))
    return DT_CHR;
  if (S_ISBLK(mode))
    return DT_BLK;
  if (S_ISSOCK(mode))
    return DT_SOCK;
  if (S_ISFIFO(mode))
    return DT_FIFO;
  return DT_UNKNOWN;
}

std::string GetInodeFTStr(InodeNum inode, unsigned char ftype) {
  char buf[18];
  if (ftype > kMaxFT || !kFT2Ch[ftype])
    return GetInodeStr(inode);
  std::snprintf(buf, sizeof(buf), "%c%016" PRIx64, kFT2Ch[ftype], inode);
  return buf;
}

bool ParseInodeNum(const char* buf, int len, InodeNum* inode_ret) {
  return _ParseNum(buf, len, inode_ret);
}

bool ParseInodeNumFT(const char* buf, int len,
                     InodeNum* inode_ret, unsigned char* ftype_ret) {
  unsigned int idx = buf[0] - kMinFTch;
  if (idx <= kMaxCh2FTIdx && kCh2FT[idx] != 0) {
    *ftype_ret = kCh2FT[idx];
    ++buf;
    --len;
  } else {
    *ftype_ret = DT_UNKNOWN;
  }
  return ParseInodeNum(buf, len, inode_ret);
}

bool ParseClientNum(const char* buf, int len, ClientNum* client_ret) {
  return _ParseNum(buf, len, client_ret);
}

bool ParseFileSize(const char* buf, int len, uint64_t* file_size_ret) {
  return _ParseNum(buf, len, file_size_ret);
}

bool ParseXattrTime(const char* time_str, FSTime* time_ret) {
  char* eptr;
  errno = 0;
  time_ret->sec = strtoull(time_str, &eptr, 10);
  if (errno != 0 || *eptr != '-')
    return false;
  time_ret->ns = strtoull(eptr + 1, &eptr, 10);
  return errno == 0 && *eptr == '\0';
}

int SymlinkToInode(const std::string& path, InodeNum* inode_ret) {
  char buf[18];
  ssize_t len = readlink(path.c_str(), buf, sizeof(buf) - 1);
  if (len == -1)
    return errno == EINVAL ? -EIO : -errno;
  buf[len] = 0;
  if (!ParseInodeNum(buf, len, inode_ret)) {
    LOG(error, Store, "Invalid inode symlink at ", path);
    return -EIO;
  }
  return 0;
}

int SymlinkToInodeFT(const std::string& path, InodeNum* inode_ret,
                     unsigned char* ftype_ret) {
  char buf[18];
  ssize_t len = readlink(path.c_str(), buf, sizeof(buf) - 1);
  if (len == -1)
    return errno == EINVAL ? -EIO : -errno;
  buf[len] = 0;
  if (!ParseInodeNumFT(buf, len, inode_ret, ftype_ret)) {
    LOG(error, Store, "Invalid inode symlink at ", path);
    return -EIO;
  }
  return 0;
}

int XattrToInode(const std::string& path, InodeNum* inode_ret) {
  char buf[18];
  int len = GET_XATTR(path.c_str(), "user.ino", buf);
  if (len < 0)
    return len == -ENODATA || len == -EPERM ? -EIO : len;
  if (!ParseInodeNum(buf, len, inode_ret)) {
    LOG(error, Store, "Invalid inode xattr at ", path);
    return -EIO;
  }
  return 0;
}

int DentryToInode(const std::string& path, InodeNum* inode_ret,
                  struct stat* stbuf) {
  int ret = lstat(path.c_str(), stbuf);
  if (ret == -1)
    return -errno;
  unsigned char ftype;
  return S_ISDIR(stbuf->st_mode) ?
      XattrToInode(path, inode_ret) : SymlinkToInodeFT(path, inode_ret, &ftype);
}

int GetFileAttr(const std::string& inode_path, FileAttr* fa) {
  struct stat stbuf;
  if (lstat(inode_path.c_str(), &stbuf) == -1)
    return -errno;
  fa->mode = stbuf.st_mode;
  fa->nlink = stbuf.st_nlink;
  fa->uid = stbuf.st_uid;
  fa->gid = stbuf.st_gid;
  fa->rdev = stbuf.st_rdev;
  fa->size = stbuf.st_size;
  fa->atime.FromTimeSpec(stbuf.st_atim);
  fa->mtime.FromTimeSpec(stbuf.st_mtim);
  if (!GetCtrlCtime(inode_path, &fa->ctime))
    return -EIO;
  return 0;
}

bool GetCtrlCtime(const std::string& inode_path, FSTime* ctime_ret) {
  char buf[128];
  int len = GET_XATTR((inode_path + "x").c_str(), "user.ct", buf);
  if (len < 0) {
    LOG(error, Store, "Cannot get ctime xattr at ", inode_path);
    return false;
  }
  buf[len] = 0;
  if (!ParseXattrTime(buf, ctime_ret)) {
    LOG(error, Store, "Cannot parse ctime xattr at ", inode_path);
    return false;
  }
  return true;
}

bool SetCtrlCtime(const std::string& inode_path, const FSTime& ctime) {
    char buf[128];
    int ret = std::snprintf(buf, sizeof(buf), "%" PRIu64 "-%" PRIu64,
                            ctime.sec, ctime.ns);
    return lsetxattr((inode_path + "x").c_str(), "user.ct", buf, ret, 0) >= 0;
}

int ReadLinkTarget(const std::string& path, std::vector<char>* buf_ret) {
  unsigned buf_size = 32;  // Half initial size
  ssize_t size;
  do {
    buf_size *= 2;
    buf_ret->resize(buf_size);
    size = readlink(path.c_str(), &(*buf_ret)[0], buf_size);
    if (size < 0)
      return -errno;
  } while (size >= buf_size);
  buf_ret->resize(size);
  return 0;
}

namespace {

/**
 * Convert an extended attribute to a vector of server group IDs.
 *
 * @param xattr The extended attribute value.
 *
 * @param groups Where to store the vector of server group IDs.
 * Original content will not be erased first.
 *
 * @return Normally 0, -EIO if there is an error.
 */
int XattrToGroups(std::string xattr, std::vector<GroupId>* groups) {
  std::vector<std::string> splitted;
  boost::split(splitted, xattr, boost::is_any_of(","));
  errno = 0;
  for (unsigned i = 0; i < splitted.size(); ++i) {
    const char* grp_str = splitted[i].c_str();
    char* endptr;
    uint64_t val = std::strtoull(grp_str, &endptr, 10);
    if (*grp_str == '\0' || *endptr != '\0' || errno)
      return -EIO;
    groups->push_back(val);
  }
  return 0;
}

/**
 * Convert a vector of server group IDs to an extended attribute.
 *
 * @param groups The vector of server group IDs.
 *
 * @return The extended attribute value.
 */
std::string GroupsToXattr(const std::vector<GroupId>& groups) {
  std::string ret;
  for (unsigned i = 0; i < groups.size(); ++i) {
    if (i)
      ret += ',';
    ret += (boost::format("%d") % groups[i]).str();
  }
  return ret;
}

}  // namespace

int GetFileGroupIds(const std::string& path,
                    std::vector<GroupId>* group_ids_ret) {
  char buf[1024];
  int len = GET_XATTR((path + "x").c_str(), "user.sg", buf);
  if (len < 0)
    return len;
  return XattrToGroups(std::string(buf, len), group_ids_ret);
}

int SetFileGroupIds(const std::string& path,
                    const std::vector<GroupId>& group_ids) {
  std::string str = GroupsToXattr(group_ids);
  errno = 0;
  lsetxattr((path + "x").c_str(), "user.sg", str.data(), str.size(), 0);
  return -errno;
}

InodeNum GetDirParent(const std::string& path) {
  char buf[32];
  int len = GET_XATTR((path + "x").c_str(), "user.par", buf);
  if (len < 0)
    throw std::runtime_error("Cannot get parent for inode path " + path);
  InodeNum ret;
  if (!ParseInodeNum(buf, len, &ret))
    throw std::runtime_error("Invalid parent attribute at " + path
                             + ": " + buf);
  return ret;
}

void SetDirParent(const std::string& inode_path, const std::string& par_inode) {
  if (lsetxattr((inode_path + "x").c_str(), "user.par",
                par_inode.c_str(), par_inode.size(), 0) != 0)
    throw std::runtime_error("Cannot set parent for inode path " + inode_path);
}

int RemoveDirInode(const std::string& inode_path) {
  boost::scoped_ptr<IDirIterator> dir_iter(MakeDirIterator(inode_path));
  std::string name;
  bool is_dir;
  while (dir_iter->GetNext(&name, &is_dir)) {
    std::string to_remove = inode_path + "/" + name;
    if (is_dir) {
      if (rmdir(to_remove.c_str()) != 0)
        return -errno;
    } else {
      if (unlink(to_remove.c_str()) != 0)
        return -errno;
    }
  }
  return rmdir(inode_path.c_str());
}

std::string LoadOrCreateUUID(const std::string& data_path) {
  char buf[37];
  if (lgetxattr(data_path.c_str(), "user.uuid", buf, sizeof(buf)) == -1) {
    std::string uuid = CreateUUID();
    if (lsetxattr(data_path.c_str(), "user.uuid", uuid.c_str(), 37U, 0) != 0)
      throw std::runtime_error("Failed initializing uuid");
    return uuid;
  } else {
    buf[36] = '\0';
    return buf;
  }
}

}  // namespace cpfs
