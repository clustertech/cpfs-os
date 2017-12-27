#pragma once

/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Define the VFS interface for common filesystem operations.
 */

#include <stdint.h>

#include <sys/stat.h>
#include <sys/uio.h>

#include <cstddef>
#include <vector>

#include "fim.hpp"

namespace boost { template <class Y> class shared_ptr; }

namespace cpfs {

class IReqEntry;

namespace client {

class BaseFSClient;

/**
 * User and group identity
 */
struct FSIdentity {
  uint32_t uid;  /**< UID */
  uint32_t gid;  /**< GID */
  /**
   * Construct the user and group identity
   */
  FSIdentity(uint32_t uid, uint32_t gid) : uid(uid), gid(gid) {}
};

/**
 * Values returned from Open()
 */
struct FSOpenReply {
  uint64_t fh;  /**< The file handle */
  FIM_PTR<IFim> reply;  /**< Reply received for this request */
};

/**
 * Values returned from Lookup()
 */
struct FSLookupReply {
  uint64_t inode;  /**< The inode returned */
  struct stat attr;  /**< The stat attr for this inode */
  double attr_timeout;  /**< The timeout for inode cached */
  double entry_timeout;  /**< The timeout for inode cached */
  FIM_PTR<IFim> reply;  /**< Reply received for this request */
};

/**
 * Values returned from Open()
 */
struct FSCreateReply {
  uint64_t inode;  /**< The inode returned */
  struct stat attr;  /**< The stat attr for this inode */
  uint64_t fh;  /**< The file handle returned */
  FIM_PTR<IFim> reply;  /**< Reply received for this request */
};

/**
 * Values returned from Read()
 */
struct FSReadReply {
  char* buf;  /**< The pointer to allocated buffer for data read */
  FIM_PTR<IFim> reply;  /**< Reply received for this request */
};

/**
 * Values returned from Readv()
 */
struct FSReadvReply {
  std::vector<struct iovec> iov;  /**< IO vectors for data read */
  /**
   * Replies received for this request.  If an error is seen, its Fim
   * will be the last one (after that the Readv call stops collecting
   * Fims
   */
  std::vector<FIM_PTR<IFim> > replies;
};

/**
 * Values returned from Write()
 */
struct FSWriteReply {
  int deferred_errno;  /**< The derred error number */
};

/**
 * Values returned from Getattr()
 */
struct FSGetattrReply {
  struct stat* stbuf;  /**< The pointer to stat of the inode */
  FIM_PTR<IFim> reply;  /**< Reply received for this request */
};

/**
 * Values returned from Setattr()
 */
struct FSSetattrReply {
  struct stat stbuf;  /**< The stat of the new inode */
  FIM_PTR<IFim> reply;  /**< Reply received for this request */
};

/**
 * Callback to run when a inode-creating request is replied.  This
 * will do anything only if the reply is an AttrReply, and may perform
 * two things:
 *
 * 1. Set the created inode number reply
 *
 * 2. Set the DS group list of the reply to the request.  The DS group
 *    list is assumed to be the full of the tail buffer of the reply.
 *
 * @param ent The request entry
 */
void CreateReplyCallback(const boost::shared_ptr<IReqEntry>& ent);

/**
 * Base VFS interface for common filesystem operations.
 */
class IFSCommonLL {
 public:
  virtual ~IFSCommonLL() {}
  /**
   * Set the CPFS client object.
   *
   * @param client The client object
   */
  virtual void SetClient(BaseFSClient* client) = 0;

  /**
   * Open the given inode
   *
   * @param identity The UID and GID
   *
   * @param inode The inode to open
   *
   * @param flags The POSIX flags to use
   *
   * @param ret The FSOpenReply returned
   *
   * @return True if successful, false otherwise
   */
  virtual bool Open(const FSIdentity& identity,
                    uint64_t inode, int flags, FSOpenReply* ret) = 0;
  /**
   * Lookup the inode number from the specified name
   *
   * @param identity The UID and GID
   *
   * @param parent The inode of the parent directory
   *
   * @param name The name to lookup
   *
   * @param ret The FSLookupReply returned
   *
   * @return True if successful, false otherwise
   */
  virtual bool Lookup(const FSIdentity& identity, uint64_t parent,
                      const char* name, FSLookupReply* ret) = 0;
  /**
   * Create a new inode
   *
   * @param identity The UID and GID
   *
   * @param parent The inode of the parent directory
   *
   * @param name The name to lookup
   *
   * @param mode The POSIX file mode
   *
   * @param flags The POSIX file creation flags
   *
   * @param ret The FSCreateReply returned
   *
   * @return True if successful, false otherwise
   */
  virtual bool Create(
      const FSIdentity& identity, uint64_t parent, const char* name,
      mode_t mode, int flags, FSCreateReply* ret) = 0;

  /**
   * Read data from an inode
   *
   * @param fh The file handle for reading
   *
   * @param inode The inode to read
   *
   * @param size The size of data to read
   *
   * @param off The offset
   *
   * @param ret Reply received for this request
   *
   * @return True if successful, false otherwise
   */
  virtual bool Read(
      uint64_t fh, uint64_t inode, std::size_t size, off_t off,
      FSReadReply* ret) = 0;

  /**
   * Read vectored data from an inode.
   *
   * @param fh The file handle for reading
   *
   * @param inode The inode to read
   *
   * @param size The size of data to read
   *
   * @param off The offset
   *
   * @param ret Reply received for this request
   *
   * @return True if successful, false otherwise
   */
  virtual bool Readv(
      uint64_t fh, uint64_t inode, std::size_t size, off_t off,
      FSReadvReply* ret) = 0;

  /**
   * Write data to an inode
   *
   * @param fh The file handle for writing
   *
   * @param inode The inode to read
   *
   * @param buf The buffer to received data
   *
   * @param size The size of data to read
   *
   * @param off The offset
   *
   * @param ret Reply received for this request
   *
   * @return True if successful, false otherwise
   */
  virtual bool Write(uint64_t fh, uint64_t inode,
                     const char* buf, std::size_t size, off_t off,
                     FSWriteReply* ret) = 0;

  /**
   * Release the given inode
   *
   * @param inode The inode
   *
   * @param fh The file handle
   *
   * @param flags The POSIX flags
   */
  virtual void Release(uint64_t inode, uint64_t* fh, int flags) = 0;

  /**
   * Getattr from an inode
   *
   * @param identity The UID and GID
   *
   * @param inode The inode to use
   *
   * @param ret Reply received for this request
   *
   * @return True if successful, false otherwise
   */
  virtual bool Getattr(const FSIdentity& identity,
                       uint64_t inode, FSGetattrReply* ret) = 0;

  /**
   * Setattr for an inode
   *
   * @param identity The UID and GID
   *
   * @param inode The inode to use
   *
   * @param attr The attr to set
   *
   * @param to_set The FUSE flag that specifies stat fields to set
   *
   * @param ret Reply received for this request
   *
   * @return True if successful, false otherwise
   */
  virtual bool Setattr(const FSIdentity& identity, uint64_t inode,
                       const struct stat* attr, int to_set,
                       FSSetattrReply* ret) = 0;

  /**
   * Flush for an inode
   */
  virtual void Flush(uint64_t inode) = 0;

  /**
   * Fsync for an inode
   */
  virtual void Fsync(uint64_t inode) = 0;
};

}  // namespace client

}  // namespace cpfs
