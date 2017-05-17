#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the IStore interface.
 */

#include <stdint.h>

#include <cstddef>
#include <map>
#include <stdexcept>

#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "finfo.hpp"

namespace boost {

template <class Y> class scoped_ptr;

}  // namespace boost

namespace cpfs {

struct CreateReq;
struct CreateDirReq;
struct FileAttr;
class IDirIterator;
struct OpContext;

namespace server {

class IDurableRange;
class IInodeRemovalTracker;

namespace ms {

class IDSGAllocator;
class IInodeSrc;
class IInodeUsage;
class IUgidHandler;

/**
 * Indicate that an operation requires a lock that has not been
 * acquired.
 */
class MDNeedLock : public std::runtime_error {
 public:
  /**
   * @param arg The exception message.
   */
  explicit MDNeedLock(const std::string& arg) : std::runtime_error(arg) {}
};

/**
 * For returning information about a removed inode during an operation
 */
struct RemovedInodeInfo {
  InodeNum inode; /**< The inode number of the removed inode */
  bool to_free; /**< Whether the inode needs to be freed */
  /** The DS groups used by the inode */
  boost::shared_ptr<std::vector<GroupId> > groups;
};

/**
 * Map of UUID to role name
 */
typedef std::map<std::string, std::string> UUIDInfoMap;

/**
 * Interface for meta-server directory operations.
 */
class IStore {
 public:
  virtual ~IStore() {}

  /**
   * Check whether the data directory has already been initialized.
   * Initialize() should only be called if this method returns false,
   * while all other methods should only be called if this method
   * returns true.
   */
  virtual bool IsInitialized() = 0;

  /**
   * Initialize the data directory.
   */
  virtual void Initialize() = 0;

  /**
   * Prepare and keeps the UUID for this store
   */
  virtual void PrepareUUID() = 0;

  /**
   * Get UUID for this store
   */
  virtual std::string GetUUID() const = 0;

  /**
   * Load all UUIDs in this cluster
   */
  virtual UUIDInfoMap LoadAllUUID() = 0;

  /**
   * Persist all UUIDs
   */
  virtual void PersistAllUUID(const UUIDInfoMap& uuids) = 0;

  /**
   * Set a user / group id handler to handle user and group id changes.
   */
  virtual void SetUgidHandler(IUgidHandler* handler) = 0;

  /**
   * Set a server group allocator for use when creating files.
   *
   * @param allocator The allocator.
   */
  virtual void SetDSGAllocator(IDSGAllocator* allocator) = 0;

  /**
   * Set an inode source for use when giving out inodes and when
   * creating / removing inodes.
   *
   * @param inode_src The inode source.
   */
  virtual void SetInodeSrc(IInodeSrc* inode_src) = 0;

  /**
   * Set an inode usage for tracking inode open, close and unlink
   *
   * @param inode_usage The inode usage.
   */
  virtual void SetInodeUsage(IInodeUsage* inode_usage) = 0;

  /**
   * Set tracker for recording deleted inodes and dentries
   *
   * @param tracker Inode removal tracker.
   */
  virtual void SetInodeRemovalTracker(IInodeRemovalTracker* tracker) = 0;

  /**
   * Set durable range for tracking recently modified inode ranges.
   *
   * @param durable_range Durable range
   */
  virtual void SetDurableRange(IDurableRange* durable_range) = 0;

  /**
   * List entries in the store.
   *
   * @param path Relative path of the directory from store to list
   *
   * @return An iterator to list the entries
   */
  virtual IDirIterator* List(const std::string& path) = 0;

  /**
   * List inode paths in the store matching some ranges.
   *
   * @param ranges The ranges to match
   *
   * @return An iterator to list the paths, relative to data store
   * root directory
   */
  virtual IDirIterator* InodeList(const std::vector<InodeNum>& ranges) = 0;

  /**
   * Get the file attributes of an inode.  Unlike Attr(), it is
   * triggered by the system, and thus does no checking of context.
   *
   * @param inode The inode to get attributes for
   *
   * @param fa_ret Where to return the file attribute
   *
   * @return 0, or -errno
   */
  virtual int GetInodeAttr(InodeNum inode, FileAttr* fa_ret) = 0;

  /**
   * Update the mtime and size attributes of an inode.  Unlike Attr(),
   * it is triggered by the system, nad thus does no checking of
   * context.
   *
   * @param inode The inode to update attributes
   *
   * @param mtime The new mtime / ctime
   *
   * @param size The new file size
   *
   * @return 0, or -errno
   */
  virtual int UpdateInodeAttr(InodeNum inode, FSTime mtime, uint64_t size) = 0;

  /**
   * Get the group information of a file.
   *
   * @param inode The inode number of the file.
   *
   * @param group_ids The group Ids of the server groups to be used by
   * the file.  The ordering is important, the first data block should
   * use group group_ids[0].
   *
   * @return 0 if successful, -errno if failed.
   */
  virtual int GetFileGroupIds(InodeNum inode,
                              std::vector<GroupId>* group_ids) = 0;

  /**
   * Set time values for the last resynced directory
   *
   * @return 0 if successful, -errno if failed.
   */
  virtual int SetLastResyncDirTimes() = 0;

  /**
   * Remove inodes since the specified ctime
   *
   * @param ranges The inode ranges to remove, as recorded by the
   * durable range
   *
   * @param ctime Inodes to remove since this ctime
   *
   * @return 0 if successful, -errno if failed.
   */
  virtual int RemoveInodeSince(const std::vector<InodeNum>& ranges,
                               uint64_t ctime) = 0;

  // Generic inode handling

  /**
   * Prepare for inode operations to be done outside the store.  This
   * locks the inode mutex of the inode, so that other operations of
   * the same inode will not be done until the operation context is
   * released.
   *
   * @param context The operation context.
   *
   * @param inode The inode to be operated on.
   */
  virtual void OperateInode(OpContext* context, InodeNum inode) = 0;

  /**
   * Get / set file attributes.
   *
   * @param context The operation context.
   *
   * @param inode The inode with attributes to get.
   *
   * @param fa The new file information, to be set if specified by
   * fa_mask.  If 0, get file attributes only.
   *
   * @param fa_mask The fields which are to be set.  Fields are
   * specified with the same constants as the to_set argument of
   * fuse_lowlevel.h in the setattr method, although the _NOW masks
   * are ignored (they should have been handled by clients).
   *
   * @param locked Whether a lock is already obtained for the
   * operation.  Currently truncation requires this to be true,
   * otherwise an MDNeedLock exception will be thrown.
   *
   * @param fa_ret The file information structure to put the new
   * attributes to.
   *
   * @return Either 0 (success) or -errno.
   */
  virtual int Attr(OpContext* context, InodeNum inode,
                   const FileAttr* fa, uint32_t fa_mask,
                   bool locked, FileAttr* fa_ret) = 0;

  /**
   * Set the extended attribute for the inode specified
   *
   * @param context The operation context.
   *
   * @param inode The inode with attribute to set.
   *
   * @param name The name of the extended attribute.
   *
   * @param value The value of the extended attribute.
   *
   * @param size The length of extended attribute value.
   *
   * @param flags The param for refining semantics of the operation.
   *
   * @return Either 0 (success) or -errno.
   */
  virtual int SetXattr(OpContext* context, InodeNum inode,
                       const char* name, const char* value,
                       std::size_t size, int flags) = 0;

  /**
   * Get the extended attribute for the inode specified
   *
   * @param context The operation context
   *
   * @param inode The inode with attribute to set
   *
   * @param name The name of the extended attribute
   *
   * @param buf The buffer for the extended attribute.  Used only if
   * buf_size is non-zero
   *
   * @param buf_size The size of the buf
   *
   * @return Either the actual size of the xattr (success) or -errno.
   */
  virtual int GetXattr(OpContext* context, InodeNum inode,
                       const char* name, char* buf, std::size_t buf_size) = 0;

  /**
   * List the extended attribute names for the inode specified.  Only
   * user attributes are returned, other attributes are skipped.
   *
   * @param context The operation context
   *
   * @param inode The inode with attribute to set
   *
   * @param buf The buffer for the extended attribute.  This is used
   * only if buf_size is not 0
   *
   * @param buf_size The size of buf
   *
   * @return Either amount of space used (success) or -errno.
   */
  virtual int ListXattr(OpContext* context, InodeNum inode,
                        char* buf, std::size_t buf_size) = 0;

  /**
   * Remove the extended attribute for the inode specified
   *
   * @param context The operation context.
   *
   * @param inode The inode with attribute to set.
   *
   * @param name The name of the extended attribute.
   *
   * @return Either 0 (success) or -errno.
   */
  virtual int RemoveXattr(OpContext* context, InodeNum inode,
                          const char* name) = 0;

  // Non-directory inode handling

  /**
   * Open a file.
   *
   * @param context The operation context.
   *
   * @param inode The inode number of the file to open.
   *
   * @param flags The open flags.
   *
   * @param context The request context.
   *
   * @param group_ids The group Ids of the server groups to be used by
   * the file.  The ordering is important, the first data block should
   * use group group_ids[0].
   *
   * @param truncated_ret Where to return whether the file is
   * truncated during open on successful calls
   *
   * @return Either 0 (success) or -errno.
   */
  virtual int Open(OpContext* context, InodeNum inode, int32_t flags,
                   std::vector<GroupId>* group_ids, bool* truncated_ret) = 0;

  /**
   * Check access for a file.
   *
   * @param context The operation context.
   *
   * @param inode The inode number of the file to check access for.
   *
   * @param mask The permission to check.
   */
  virtual int Access(OpContext* context, InodeNum inode, int32_t mask) = 0;

  /**
   * Get advise that a file is being written.
   *
   * @param context The operation context.
   *
   * @param inode The inode number of the file being written.
   *
   * @param off The last offset of the file written.
   *
   * @return -errno
   */
  virtual int AdviseWrite(OpContext* context, InodeNum inode,
                          uint64_t off) = 0;

  /**
   * Remove an inode.  Caller should have ensured that there is no
   * dentry using it.
   *
   * @param inode The inode number.
   */
  virtual int FreeInode(InodeNum inode) = 0;

  /**
   * Read symbolic link.
   *
   * @param context The operation context.
   *
   * @param inode The inode to read the link
   *
   * @param context The request context
   *
   * @param buf The buffer to hold the link read
   *
   * @return Either 0 (success) or -errno
   */
  virtual int Readlink(OpContext* context, InodeNum inode,
                       std::vector<char>* buf) = 0;

  // Read directories

  /**
   * Lookup a file.
   *
   * @param context The operation context.
   *
   * @param inode The inode number of the directory to lookup the
   * file
   *
   * @param name The name of the file to lookup
   *
   * @param inode_found Where to put the inode number found
   *
   * @param fa The file information structure to put result to
   *
   * @return Either 0 (success) or -errno
   */
  virtual int Lookup(OpContext* context, InodeNum inode, const char* name,
                     InodeNum* inode_found, FileAttr* fa) = 0;

  /**
   * Open a directory.
   *
   * @param context The operation context.
   *
   * @param inode The inode of the directory
   *
   * @return Either 0 (success) or -errno
   */
  virtual int Opendir(OpContext* context, InodeNum inode) = 0;

  /**
   * Read entries in a directory.
   *
   * @param context The operation context.
   *
   * @param inode The inode number of the directory to read.
   *
   * @param cookie The cookie of the file to start returning entries.
   *
   * @param buf The buffer to be used for filling entries.
   *
   * @return Either number of bytes in buffer filled, or -errno.
   */
  virtual int Readdir(OpContext* context, InodeNum inode, DentryCookie cookie,
                      std::vector<char>* buf) = 0;

  // Create directory entries

  /**
   * Create empty file.
   *
   * @param context The operation context.
   *
   * @param inode The inode number of the directory containing the
   * newly created file
   *
   * @param name The name of the file to create
   *
   * @param create_req The create request information
   *
   * @param group_ids The group Ids of the server groups to be used by
   * the file.  If empty, will allocate a set of groups, and the
   * result will be set to the vector.  The ordering is important, the
   * first data block should use group group_ids[0]
   *
   * @param fa_ret The file information structure to put attributes of
   * new file to
   *
   * @param removed_info_ret Return removed inode information if an
   * inode is removed, otherwise left untouched
   *
   * @return Either 0 (success) or -errno
   */
  virtual int Create(OpContext* context, InodeNum inode, const char* name,
                     const CreateReq* create_req,
                     std::vector<GroupId>* group_ids,
                     FileAttr* fa_ret,
                     boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret) = 0;

  /**
   * Create directory.
   *
   * @param context The operation context.
   *
   * @param parent The parent inode number of the parent directory.
   *
   * @param name The name of the directory to create.
   *
   * @param create_req The create directory request information.
   *
   * @param fa_ret The file information structure to put the new
   * attributes to.
   *
   * @return Either 0 (success) or -errno.
   */
  virtual int Mkdir(OpContext* context, InodeNum parent, const char* name,
                    const CreateDirReq* create_req, FileAttr* fa_ret) = 0;

  /**
   * Create symbolic link.
   *
   * @param context The operation context.
   *
   * @param parent The parent inode number of the parent directory
   *
   * @param name The name of the symlink to create
   *
   * @param new_inode The inode number of the created symlink
   *
   * @param target The link target
   *
   * @param fa_ret The file information structure to put the new
   * attributes to
   *
   * @return Either 0 (success) or -errno
   */
  virtual int Symlink(OpContext* context, InodeNum parent,
                      const char* name, InodeNum new_inode, const char* target,
                      FileAttr* fa_ret) = 0;

  /**
   * Create device node.
   *
   * @param context The operation context.
   *
   * @param parent The parent inode number of the parent directory
   *
   * @param name The name of the device node to create
   *
   * @param new_inode The inode number of the created device node
   *
   * @param mode The file type and mode of the device node
   *
   * @param rdev The device number, in case it is a device
   *
   * @param group_ids The group Ids of the server groups to be used by
   * the file.  If empty, will allocate a set of groups, and the
   * result will be set to the vector.  The ordering is important, the
   * first data block should use group group_ids[0]
   *
   * @param fa_ret The file information structure to put the new
   * attributes to
   *
   * @return Either 0 (success) or -errno
   */
  virtual int Mknod(OpContext* context, InodeNum parent,
                    const char* name, InodeNum new_inode, uint64_t mode,
                    uint64_t rdev,
                    std::vector<GroupId>* group_ids,
                    FileAttr* fa_ret) = 0;

  // Resynchronization
  /**
   * Resynchronize first level inode of the filesystem structure
   *
   * @param inode Inode number
   *
   * @param fa The file information structure for the inode
   *
   * @param buf If inode is a symlink, buf stores the symlink target.
   *            If inode is a file, buf stores the server group.
   *
   * @param extra_size The size of data in tail buffer excluding xattrs
   *
   * @param buf_size The size of the whole tail buffer including xattrs
   */
  virtual int ResyncInode(InodeNum inode,
                          const FileAttr& fa,
                          const char* buf,
                          std::size_t extra_size,
                          std::size_t buf_size) = 0;

  /**
   * Resynchronize dentry
   *
   * @param uid The user id
   *
   * @param gid The group id
   *
   * @param parent The inode number of the parent directory
   *
   * @param target The dentry target
   *
   * @param ftype Type of the entry as a <dirent.h> DT_* constant
   *
   * @param dentry_name The dentry name
   */
  virtual int ResyncDentry(uint32_t uid, uint32_t gid,
                           InodeNum parent, InodeNum target,
                           unsigned char ftype, const char* dentry_name) = 0;
  /**
   * Resynchronize inode removal
   *
   * @param inode Inode number
   */
  virtual int ResyncRemoval(InodeNum inode) = 0;

  // Manipulate directory entries

  /**
   * Create a hard link.
   *
   * @param context The operation context.
   *
   * @param inode The inode number of the file to be hard-linked
   *
   * @param dir_inode The inode number of the directory to hold the
   * link
   *
   * @param name The name of the hard link
   *
   * @param fa_ret The file information structure to put attributes of
   * new file to
   */
  virtual int Link(OpContext* context, InodeNum inode,
                   InodeNum dir_inode, const char* name, FileAttr* fa_ret) = 0;

  /**
   * Rename a file.
   *
   * @param context The operation context.
   *
   * @param parent The parent of the file to rename
   *
   * @param name The name of the file to rename
   *
   * @param newparent The new parent of the file
   *
   * @param newname The new name of the file
   *
   * @param moved_ret Return inode number of the inode renamed
   *
   * @param removed_info_ret Return removed inode information if an
   * inode is removed, otherwise left untouched
   *
   * @return Either 0 (success) or -errno (error)
   */
  virtual int Rename(OpContext* context, InodeNum parent,
                     const char *name, InodeNum newparent, const char *newname,
                     InodeNum* moved_ret,
                     boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret) = 0;

  // Remove directory entries

  /**
   * Unlink a file.  If the inode is being opened, only dentry but not
   * inode is removed.
   *
   * @param context The operation context.
   *
   * @param parent The inode number of the directory containing the
   * file to unlink.
   *
   * @param name The name of the file to unlink
   *
   * @param removed_info_ret Return removed inode information if an
   * inode is removed, otherwise left untouched
   *
   * @return Either 0 (success) or -errno (error)
   */
  virtual int Unlink(OpContext* context, InodeNum parent,
                     const char* name,
                     boost::scoped_ptr<RemovedInodeInfo>* removed_info_ret) = 0;

  /**
   * Remove a directory.
   *
   * @param context The operation context.
   *
   * @param parent The parent of the directory containing the
   * directory to remove
   *
   * @param name The name of the directory to remove
   *
   * @return Either 0 (success) or -errno (error)
   */
  virtual int Rmdir(OpContext* context, InodeNum parent,
                    const char* name) = 0;

  /**
   * Get file system statistics of this store
   *
   * @param total_inodes Total inodes in the store
   *
   * @param free_inodes Free inodes available in the store
   *
   * @return 0, or -errno
   */
  virtual int Stat(uint64_t* total_inodes, uint64_t* free_inodes) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
