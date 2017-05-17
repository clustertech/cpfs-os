#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define types to represent file information and requests for them.
 * These types are fixed-width and fully aligned, and can be part of
 * Fim's.
 */

#include <stdint.h>

#include <fuse/fuse_lowlevel.h>

#include <sys/stat.h>
#include <sys/time.h>

#include <cstddef>
#include <cstring>
#include <ctime>

#include "common.hpp"

namespace cpfs {

/**
 * Stores a date-time in the FS.  Same function as the C timespec, but
 * uses members of fixed width.
 */
struct FSTime {
  uint64_t sec; /**< The seconds part, from epoch */
  /**
   * The nanoseconds part.  Only 32 bits are needed, but better for
   * the whole structure to be 64-bit aligned
   */
  uint64_t ns;

  /**
   * Fill values from a timespec structure.
   *
   * @param ts The timespec structure
   */
  void FromTimeSpec(const struct timespec& ts) {
    sec = ts.tv_sec;
    ns = ts.tv_nsec;
  }

  /**
   * Put values to a timespec structure.
   *
   * @param ts The timespec structure
   */
  void ToTimeSpec(struct timespec* ts) const {
    ts->tv_sec = sec;
    ts->tv_nsec = ns;
  }

  /**
   * Fill values from the current date / time.
   */
  void FromNow() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    FromTimeSpec(ts);
  }
};

/**
 * Compare two FSTime for equality.
 *
 * @param t1 The first FSTime
 *
 * @param t2 The second FSTime
 *
 * @return True if the two times are equal
 */
inline bool operator==(const FSTime& t1, const FSTime& t2) {
  return t1.sec == t2.sec && t1.ns == t2.ns;
}

/**
 * Compare two FSTime for before-after.
 *
 * @param t1 The first FSTime
 *
 * @param t2 The second FSTime
 *
 * @return True if t1 is before t2
 */
inline bool operator<(const FSTime& t1, const FSTime& t2) {
  return t1.sec == t2.sec ? t1.ns < t2.ns : t1.sec < t2.sec;
}

/**
 * Context of requests.
 */
struct ReqContext {
  uint32_t uid; /**< The UID of the requestor */
  uint32_t gid; /**< The GID of the requestor */
  FSTime optime; /**< The last status change time */

  /**
   * Fill the structure from a FUSE ctx structure.
   *
   * @param ctx The FUSE ctx structure.
   */
  void FromFuseCtx(const fuse_ctx* ctx) {
    uid = ctx->uid;
    gid = ctx->gid;
    optime.FromNow();
  }
};

/**
 * Structure holding information about a file to return during
 * Getattr().  Note that this structure is meant to be part of a Fim,
 * so all types used are concrete types specifying the bit-length of
 * the field.  The nlink field is not quite enough to hold the full
 * nlink on 64-bit platforms, but 32-bit should be enough anyway.
 */
struct FileAttr {
  uint32_t mode;  /**< The file mode */
  uint32_t nlink;  /**< The number of links to the file */
  uint32_t uid;  /**< The UID of the file. */
  uint32_t gid;  /**< The GID of the file. */
  uint64_t rdev;  /**< The device number the file refers to. */
  uint64_t size;  /**< The size of the file. */
  FSTime atime;  /**< The last access time of the file. */
  FSTime mtime;  /**< The last modification time of the file. */
  FSTime ctime;  /**< The last status change time of the file. */

  /**
   * Fill a stat buffer using the structure.
   */
  void ToStat(InodeNum ino, struct stat* stbuf) const {
    std::memset(stbuf, '\0', sizeof(*stbuf));
    stbuf->st_ino = ino;
    stbuf->st_mode = mode;
    stbuf->st_nlink = nlink;
    stbuf->st_uid = uid;
    stbuf->st_gid = gid;
    stbuf->st_rdev = rdev;
    stbuf->st_size = size;
    stbuf->st_blksize = 32768;
    if (S_ISREG(stbuf->st_mode))
      stbuf->st_blksize *= (kNumDSPerGroup - 1);
    stbuf->st_blocks = (size + 511) / 512;
    atime.ToTimeSpec(&stbuf->st_atim);
    mtime.ToTimeSpec(&stbuf->st_mtim);
    ctime.ToTimeSpec(&stbuf->st_ctim);
  }
};

/**
 * Request for file creation.
 */
struct CreateReq {
  /**
   * The inode number of the file to be created.  This is filled by
   * the MS initially, and is updated in the FC once initial reply is
   * received in case of resend.
   */
  InodeNum new_inode;
  int32_t flags;  /**< File open flags */
  int32_t mode;  /**< Mode of the file to create. */
};

/**
 * Record in replies to ReaddirFim tail buffer.
 */
struct ReaddirRecord {
  InodeNum inode; /**< The inode number of the file */
  DentryCookie cookie; /**< Cookie to be used in the next request */
  unsigned char name_len; /**< Length of name, not including null-terminator */
  unsigned char file_type; /**< Type of file */
  char name[6]; /**< Null-terminated name, can be up to 255 bytes */

  /**
   * @return Whether record name_len matches its name.
   */
  bool IsValid() const {
    if (name_len == 0)
      return false;
    return reinterpret_cast<const char*>(std::memchr(name, '\0', name_len + 1))
        - reinterpret_cast<const char*>(name) == name_len;
  }

  /**
   * @return The length of the record.  The record is padded so that
   * it is always a multiple of 8.
   */
  int GetLen() const {
    return GetLenCore(name_len);
  }

  /**
   * @return The length required for storing a name.
   */
  static int GetLenForName(const char* name) {
    return GetLenCore(std::strlen(name));
  }

 private:
  static int GetLenCore(std::size_t len) {
    return ((offsetof(ReaddirRecord, name) + len + 8) / 8) * 8;
  }
};

/**
 * Request for directory creation.
 */
struct CreateDirReq {
  /**
   * The inode number of the folder to be created.  This is filled by
   * the MS initially, and is updated in the FC once initial reply is
   * received in case of resend.
   */
  InodeNum new_inode;
  uint64_t mode;  /**< Mode of the folder to create, long for alignment */
};

}  // namespace cpfs
