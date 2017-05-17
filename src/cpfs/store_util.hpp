#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define utilities to handle actual stores.
 */

#include <inttypes.h>
#include <stdint.h>

#include <sys/stat.h>  // IWYU pragma: keep
#include <sys/xattr.h>

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "common.hpp"
#include "finfo.hpp"

namespace cpfs {

/**
 * Macro to get extended attributes to a string buffer and
 * nul-terminate it.
 *
 * @param f File to get the extended attribute, either a string or an
 * int depending on whether lf is L or F
 *
 * @param name The name of the xattr, including the "user." part
 *
 * @param buf The buffer to store the result, must be a char[], not a
 * *char
 *
 * @return Length of attribute (before nul termination) if successful,
 * -errno otherwise
 */
#define GET_XATTR(f, name, buf) LGetXAttrTerminate(f, name, buf, sizeof(buf))

/**
 * Get extended attributes from a named file to a string buffer and
 * nul-terminate it.
 *
 * @param path Path to the file to get the extended attribute
 *
 * @param name The name of the xattr, including the "user." part
 *
 * @param buf The buffer to store the result, must be a char[], not a
 * *char
 *
 * @param size The size of the buffer
 *
 * @return Length of attribute (before nul termination) if successful,
 * -errno otherwise
 */
inline int LGetXAttrTerminate(const char* path, const char* name,
                              char* buf, std::size_t size) {
  int len = lgetxattr(path, name, buf, size - 1);
  if (len == -1) {
    return -errno;
  } else {
    buf[len] = 0;
    return len;
  }
}

/**
 * List "user." extended attributes for a file (without the "user."
 * namespace).  Throw runtime_error on error.
 *
 * @param path The path of the file
 *
 * @return The list of attributes
 */
std::vector<std::string> LListUserXattr(const char* path);

/**
 * Get a "user." extended attribute for a file.  Throw runtime_error
 * on error.
 *
 * @param path The path of the file
 *
 * @param key The key of the attribute (without "user." prefix)
 *
 * @return The value of the attribute
 */
std::string LGetUserXattr(const char* path, const std::string& key);

/**
 * Set a "user." extended attribute for a file.  Throw runtime_error
 * on error.
 *
 * @param path The path of the file
 *
 * @param key The key of the attribute (without "user." prefix)
 *
 * @param val The value of the attribute
 */
void LSetUserXattr(const char* path, const std::string& key,
                   const std::string& val);

/**
 * List of extended attribute names and values
 */
typedef std::list<std::pair<std::string, std::string > > XAttrList;

/**
 * Dump all extended attributes from a given path
 *
 * @param inode_path The inode path
 *
 * @return List of extended attribute names and values
 */
XAttrList DumpXAttr(const std::string& inode_path);

/**
 * Serialize the given XAttrList into array buffer
 *
 * @param xattr_list The list of extended attribute names and values
 *
 * @return Serialized bytes of attribute names and values
 */
std::string XAttrListToBuffer(const XAttrList& xattr_list);

/**
 * Deserialize the given buffer to XAttrList
 *
 * @param buffer The serialized bytes of attribute names and values
 *
 * @return The deserialized list of extended attribute names and values
 */
XAttrList BufferToXattrList(const std::string& buffer);

/** Minimum length to use for GetFileTimeStr */
const int kMinFiletimeLen = 40;

/**
 * Get the string representation of a time to be stored as extended
 * attributes of a file.
 *
 * @param buf Where to put the result.  It should have at least
 * kMinFiletimeLen bytes
 *
 * @param time The time
 *
 * @return The length of the representation
 */
inline int GetFileTimeStr(char* buf, const FSTime& time) {
  return std::snprintf(buf, kMinFiletimeLen, "%" PRIu64 "-%" PRIu64,
                       time.sec, time.ns);
}

/**
 * Convert file mode to DT_* constant in <dirent.h>.
 *
 * @param mode The file mode
 *
 * @return The DT_* constant
 */
unsigned char Mod2FT(uint64_t mode);

/**
 * Get the string representation of an inode.
 *
 * @param inode The inode number to represent
 *
 * @return The string representation
 */
inline std::string GetInodeStr(InodeNum inode) {
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016" PRIx64, inode);
  return buf;
}

/**
 * Get the prefixed string representation of an inode. e.g. 000/00123...
 *
 * @param inode The inode number to represent
 *
 * @return The string representation
 */
inline std::string GetPrefixedInodeStr(InodeNum inode) {
  std::string ret = GetInodeStr(inode);
  return ret.substr(0, kBaseDirNameLen) + "/" + ret;
}

/**
 * Strip the prefix from inode string, if exists
 */
inline std::string StripPrefixInodeStr(const std::string& str) {
  return str.substr(kBaseDirNameLen + 1);
}

/**
 * Get the string representation of an inode with file type info.
 *
 * @param inode The inode number to represent
 *
 * @param ftype The file type, one of the DT_* constants defined in
 * <dirent.h>
 *
 * @return The string representation
 */
std::string GetInodeFTStr(InodeNum inode, unsigned char ftype);

/**
 * Parse a string as an inode number.
 *
 * @param buf The buffer storing the string
 *
 * @param len The length of the inode number
 *
 * @param inode_ret Where to store the inode number
 *
 * @return Whether the parsing is successful
 */
bool ParseInodeNum(const char* buf, int len, InodeNum* inode_ret);

/**
 * Parse a string as an inode number and file type.
 *
 * @param buf The buffer storing the string
 *
 * @param len The length of the inode number
 *
 * @param inode_ret Where to store the inode number
 *
 * @param ftype_ret Where to store the file type, which is one of the
 * DT_* constants defined in <dirent.h>
 *
 * @return Whether the parsing is successful
 */
bool ParseInodeNumFT(const char* buf, int len,
                     InodeNum* inode_ret, unsigned char* ftype_ret);

/**
 * Parse a string as a client number.
 *
 * @param buf The buffer storing the string
 *
 * @param len The length of the inode number
 *
 * @param client_ret Where to store the client number
 *
 * @return Whether the parsing is successful
 */
bool ParseClientNum(const char* buf, int len, ClientNum* client_ret);

/**
 * Parse a string as a file size.
 *
 * @param buf The buffer storing the string
 *
 * @param len The length of the inode number
 *
 * @param file_size_ret Where to store the file size
 *
 * @return Whether the parsing is successful
 */
bool ParseFileSize(const char* buf, int len, uint64_t* file_size_ret);

/**
 * Parse a string as stored in a file xattr representing a time.
 *
 * @param time_str The string
 *
 * @param time_ret Where to put the parsed time
 */
bool ParseXattrTime(const char* time_str, FSTime* time_ret);

/**
 * Read ctime from an MS control file.
 *
 * @param inode_path The inode path
 *
 * @param ctime_ret Where to return the ctime
 *
 * @return Whether the ctime is successfully obtained
 */
bool GetCtrlCtime(const std::string& inode_path, FSTime* ctime_ret);

/**
 * Set ctime for an MS control file.
 *
 * @param inode_path The inode path
 *
 * @param ctime The ctime to set
 *
 * @return Whether the ctime is successfully set.  If not, errno is
 * maintained to signify the error that happened
 */
bool SetCtrlCtime(const std::string& inode_path, const FSTime& ctime);

/**
 * Read a symlink and interpret it as an inode number.
 *
 * @param path The path
 *
 * @param inode_ret Where to put the inode read
 *
 * @return 0 or -errno
 */
int SymlinkToInode(const std::string& path, InodeNum* inode_ret);

/**
 * Read a symlink and interpret it as an inode number with file type.
 *
 * @param path The path
 *
 * @param inode_ret Where to put the inode read
 *
 * @param ftype_ret Where to put the file type information
 *
 * @return 0 or -errno
 */
int SymlinkToInodeFT(const std::string& path, InodeNum* inode_ret,
                     unsigned char* ftype_ret);

/**
 * Read inode extended attribute from a file and interpret it.
 *
 * @param path The path
 *
 * @param inode_ret Where to put the inode read
 *
 * @return 0 or -errno
 */
int XattrToInode(const std::string& path, InodeNum* inode_ret);

/**
 * Get inode number from a dentry item.  Combine the SymlinkToInode
 * and XattrToInode calls in one call.
 *
 * @param path The path
 *
 * @param inode_ret Where to put the inode read
 *
 * @param stbuf The stat buffer to hold the stat of the directory
 * entry
 *
 * @return 0 or -errno
 */
int DentryToInode(const std::string& path, InodeNum* inode_ret,
                  struct stat* stbuf);

/**
 * Get file attributes from a file or directory
 *
 * @param inode_path The path
 *
 * @param fa The file attributes retrieved
 *
 * @return 0 or -errno
 */
int GetFileAttr(const std::string& inode_path, FileAttr* fa);

/**
 * Read link target
 *
 * @param path Path to inode
 *
 * @param buf_ret The link target
 *
 * @return 0 or -errno
 */
int ReadLinkTarget(const std::string& path, std::vector<char>* buf_ret);

/**
 * Get the group information of a file.
 *
 * @param path The inode path.
 *
 * @param group_ids_ret Where to return the group Ids of the server
 * groups to be used by the file
 *
 * @return 0 if successful, -errno if failed.
 */
int GetFileGroupIds(const std::string& path,
                    std::vector<GroupId>* group_ids_ret);

/**
 * Set the group information of a file.
 *
 * @param path The inode path
 *
 * @param group_ids The group Ids of the server groups to be used by
 * the file.  The ordering is important, the first data block should
 * use group group_ids[0]
 *
 * @return 0 if successful, -errno if failed.
 */
int SetFileGroupIds(const std::string& path,
                    const std::vector<GroupId>& group_ids);

/**
 * Get parent directory for a directory inode.
 *
 * @param path The inode path
 *
 * @return The parent inode
 */
InodeNum GetDirParent(const std::string& path);

/**
 * Set parent directory for a directory inode.
 *
 * @param inode_path The inode path
 *
 * @param par_inode The parent inode
 */
void SetDirParent(const std::string& inode_path, const std::string& par_inode);

/**
 * Remove a directory inode and all contents in it
 *
 * @param inode_path The inode path
 *
 * @return 0 if successful, -errno if failed.
 */
int RemoveDirInode(const std::string& inode_path);

/**
 * Load or create the UUID from data directory if not exists
 */
std::string LoadOrCreateUUID(const std::string& data_path);

}  // namespace cpfs
