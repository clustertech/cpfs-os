#pragma once

/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * The CPFS access API header
 */

#include <stdint.h>
#include <sys/stat.h>

#if defined(__cplusplus)
extern "C" {
#endif

struct CpfsFilesystem;  /**< The filesystem object, one per virtual mount */
typedef struct CpfsFilesystem CpfsFilesystem;  /**< Alias of CpfsFileSystem */

/**
 * Define the handle to file on CPFS
 */
typedef struct {
  uint64_t inode;  /**< The internal inode represented */
  uint64_t fh;  /**< The internal fh represented */
  int flags;  /**< The flags used to open this inode */
  int opened;  /**< Whether the file is opened */
} CpfsFileHandle;

// Equivalent to 'to_set' fields in fuse_lowlevel.h
#define SET_ATTR_MODE  (1 << 0)  /**< st_mode */
#define SET_ATTR_UID   (1 << 1)  /**< st_uid */
#define SET_ATTR_GID   (1 << 2)  /**< st_gid */
#define SET_ATTR_SIZE  (1 << 3)  /**< st_size */
#define SET_ATTR_ATIME (1 << 4)  /**< st_atime */
#define SET_ATTR_MTIME (1 << 5)  /**< st_mtime */
#define SET_ATTR_ATIME_NOW (1 << 7)  /**< st_atime from now */
#define SET_ATTR_MTIME_NOW (1 << 8)  /**< st_mtime from now */

/**
 * Set the logging level to use
 *
 * @param level The logging level (0 - 7).  May also be specified
 * for particular category, E.g., 5:Server=7 use 7 for Server, 5 for others
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsSetLogLevel(const char* level);

/**
 * Set the logging path
 *
 * @param path The absolute path to logging file
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsSetLogPath(const char* path);

/**
 * Initialize the CPFS
 *
 * @param ms1 The IP:Port
 *
 * @param ms2 The IP:Port (Optional. Specify NULL for non-HA configuration)
 *
 * @param num_threads The number of I/O worker threads to use
 *
 * @return Return 0 on success, -ve on error
 */
CpfsFilesystem* CpfsInit(
    const char* ms1, const char* ms2, unsigned int num_threads);

/**
 * Shutdown the CPFS
 *
 * @return Return 0 on success, -ve on error
 */
void CpfsShutdown(CpfsFilesystem* fs);

/**
 * Open the given path
 *
 * @param path The path to file
 *
 * @param flags The POSIX flags
 *
 * @param mode The POSIX mode
 *
 * @param fs The filesystem object
 *
 * @param handle The file handle returned
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsOpen(const char* path,
             int flags,
             mode_t mode,
             CpfsFilesystem* fs,
             CpfsFileHandle* handle);

/**
 * Close the given path
 *
 * @param fs The filesystem object
 *
 * @param handle The file handle
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsClose(CpfsFilesystem* fs, CpfsFileHandle* handle);

/**
 * Read data to buffer, either synchronously or asynchronously. If callback
 * is non-NULL, this method will be blocked until data is returned, otherwise
 * this callback is invoked when data becomes ready.
 *
 * @param buf The buffer
 *
 * @param size The size to read
 *
 * @param off The offset to read from
 *
 * @param fs The filesystem object
 *
 * @param handle The file handle
 *
 * @param callback The callback to call on async completion
 *
 * @param data The opaque data to pass to callback
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsRead(char* buf, uint32_t size, off_t off,
             CpfsFilesystem* fs,
             CpfsFileHandle* handle,
             void (*callback)(int, void*),
             void* data);

/**
 * Write data from buffer, either synchronously or asynchronously. If callback
 * is non-NULL, this method will be blocked until data is written, otherwise
 * this callback is invoked when data write completed.
 *
 * @param buf The buffer
 *
 * @param size The size to read
 *
 * @param off The offset to read from
 *
 * @param fs The filesystem object
 *
 * @param handle The file handle
 *
 * @param callback The callback to call on async completion
 *
 * @param data The opaque data to pass to callback
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsWrite(const char* buf, uint32_t size, off_t off,
              CpfsFilesystem* fs,
              const CpfsFileHandle* handle,
              void (*callback)(int, void*),
              void* data);

/**
 * Get stat attributes from the file
 *
 * @param stbuf The stat returned
 *
 * @param fs The filesystem object

 * @param handle The file handle
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsGetattr(struct stat* stbuf,
                CpfsFilesystem* fs,
                CpfsFileHandle* handle);

/**
 * Set stat attributes for the file
 *
 * @param stbuf The stat returned
 *
 * @param to_set The SET_ATTR_xxx flags to control which fields to set
 *
 * @param fs The filesystem object
 *
 * @param handle The file handle
 *
 * @return Return 0 on success, -ve on error
 */
int CpfsSetattr(struct stat* stbuf, int to_set,
                CpfsFilesystem* fs, CpfsFileHandle* handle);

#ifdef __cplusplus
}
#endif
