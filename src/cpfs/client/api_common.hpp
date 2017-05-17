#pragma once

/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Define cpfs::client::APICommon for access via API.
 */

#include "cpfs_api.h"  // NOLINT(build/include)

namespace cpfs {

namespace client {

/**
 * The interface for filesystem common
 */
class IAPICommon {
 public:
  virtual ~IAPICommon() {}
  /**
   * Open the given path on the CPFS. Equivalent to POSIX open()
   *
   * @param path The file path
   *
   * @param flags The open flags
   *
   * @param mode The file mode
   *
   * @param handle Return the handle to this inode
   *
   * @return Return 0 on success, -ve otherwise
   */
  virtual int Open(const char* path, int flags, mode_t mode,
                   CpfsFileHandle* handle) = 0;
  /**
   * Close the handle
   *
   * @param handle The handle to the opened inode
   *
   * @return Return 0 on success, -ve otherwise
   */
  virtual int Close(CpfsFileHandle* handle) = 0;
  /**
   * Read data from the given handle
   *
   * @param buf The pointer to allocated buffer
   *
   * @param size The file size
   *
   * @param off The read offset
   *
   * @param handle The handle to the opened inode
   *
   * @return Return 0 on success, -ve otherwise
   */
  virtual int Read(char* buf, uint32_t size, off_t off,
                   CpfsFileHandle* handle) = 0;
  /**
   * Write data to the given handle
   *
   * @param buf The pointer to buffer of data to be written
   *
   * @param size The file size
   *
   * @param off The write offset
   *
   * @param handle The handle to the opened inode
   *
   * @return Return 0 on success, -ve otherwise
   */
  virtual int Write(const char* buf, uint32_t size, off_t off,
                    const CpfsFileHandle* handle) = 0;
  /**
   * Getattr from the given inode
   *
   * @param stbuf The stat buffer
   *
   * @param handle The handle to the opened inode
   *
   * @return Return 0 on success, -ve otherwise
   */
  virtual int Getattr(struct stat* stbuf, CpfsFileHandle* handle) = 0;
  /**
   * Setattr for the given inode
   *
   * @param stbuf The stat buffer to set
   *
   * @param to_set The fields to set for stbuf. See access_api.h
   *
   * @param handle The handle to the opened inode
   *
   * @return Return 0 on success, -ve otherwise
   */
  virtual int Setattr(struct stat* stbuf,
                      int to_set, CpfsFileHandle* handle) = 0;
};

/**
 * Interface of the filesystem request to CPFS API
 */
class IAPIRequest {
 public:
  virtual ~IAPIRequest() {}
  /**
   * Execute this request with the provided api
   *
   * @param api The API layer of the virtual mount to execute request on
   */
  virtual void Execute(IAPICommon* api) = 0;
};

/**
 * The read request to CPFS API
 */
class APIReadRequest : public IAPIRequest {
 public:
  /**
   * Construct the APIReadRequest
   *
   * @param buf The pointer to buffer for read
   *
   * @param size The buffer size
   *
   * @param off The offset to use
   *
   * @param handle The file handle
   *
   * @param callback The callback to be invoked once the Read() is completed
   *
   * @param data The opaque data to pass to callback during invocation
   */
  APIReadRequest(char* buf, uint32_t size, off_t off,
                 CpfsFileHandle* handle,
                 void (*callback)(int, void*), void* data)
      : buf_(buf), size_(size), off_(off),
        handle_(handle),
        callback_(callback), data_(data) {}

  /**
   * Execute read request with the provided api
   *
   * @param api The API layer of the virtual mount to execute request on
   */
  void Execute(IAPICommon* api) {
    int ret = api->Read(buf_, size_, off_, handle_);
    // Invoke user supplied callback with data and ret code
    (*callback_)(ret, data_);
  }

 private:
  char* buf_;
  uint32_t size_;
  off_t off_;
  CpfsFileHandle* handle_;
  void (*callback_)(int, void*);
  void* data_;
};

/**
 * The write request to CPFS API
 */
class APIWriteRequest : public IAPIRequest {
 public:
  /**
   * Construct the APIWriteRequest
   *
   * @param buf The pointer to buffer for write
   *
   * @param size The buffer size
   *
   * @param off The offset to use
   *
   * @param handle The file handle
   *
   * @param callback The callback to be invoked once the Write() is completed
   *
   * @param data The opaque data to pass to callback during invocation
   */
  APIWriteRequest(const char* buf, uint32_t size, off_t off,
                  const CpfsFileHandle* handle,
                  void (*callback)(int, void*), void* data)
      : buf_(buf), size_(size), off_(off),
        handle_(handle),
        callback_(callback), data_(data) {}

  /**
   * Execute write request with the provided api
   *
   * @param api The API layer of the virtual mount to execute request on
   */
  void Execute(IAPICommon* api) {
    int ret = api->Write(buf_, size_, off_, handle_);
    // Invoke user supplied callback with data and ret code
    (*callback_)(ret, data_);
  }

 private:
  const char* buf_;
  uint32_t size_;
  off_t off_;
  const CpfsFileHandle* handle_;
  void (*callback_)(int, void*);
  void* data_;
};

/**
 * Helper class for executing FSCommon Read() or Write() asynchronously
 */
class IFSAsyncRWExecutor {
 public:
  virtual ~IFSAsyncRWExecutor() {}
  /**
   * Start the executor with given number of worker threads
   *
   * @param num_workers Number of threads
   */
  virtual void Start(unsigned num_workers) = 0;
  /**
   * Stop the executor
   */
  virtual void Stop() = 0;
  /**
   * Add read or write request to executor
   *
   * @param req The request to be executed
   */
  virtual void Add(IAPIRequest* req) = 0;
};

}  // namespace client

}  // namespace cpfs
