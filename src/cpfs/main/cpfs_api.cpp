/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for access library
 */

#include "cpfs_api.h"  // NOLINT(build/include)

#include <string>
#include <vector>

#include "common.hpp"
#include "connector.hpp"
#include "io_service_runner.hpp"
#include "logger.hpp"
#include "client/access_api_main.hpp"
#include "client/api_common.hpp"
#include "client/base_client.hpp"
#include "client/conn_mgr.hpp"

#if defined(__cplusplus)
extern "C" {
#endif

/** Cast the opaque pointer to APIClient object */
#define CLIENT_PTR(fs) reinterpret_cast<cpfs::client::BaseAPIClient*>(fs)

/**
 * The CPFS API C++ instance object
 */
int CpfsSetLogLevel(const char* level) {
  cpfs::SetSeverityCeiling(level);
  return 0;
}

int CpfsSetLogPath(const char* path) {
  cpfs::SetLogPath(path);
  return 0;
}

CpfsFilesystem* CpfsInit(
    const char* ms1, const char* ms2, unsigned int num_threads) {
  cpfs::client::BaseAPIClient* client;
  client = cpfs::client::MakeAPIClient(num_threads);
  client->Init();
  client->connector()->set_heartbeat_interval(
      cpfs::kDefaultHeartbeatInterval);
  client->connector()->set_socket_read_timeout(
      cpfs::kDefaultSocketReadTimeout);
  std::vector<std::string> servers;
  servers.push_back(ms1);
  if (ms2)
    servers.push_back(ms2);
  client->conn_mgr()->Init(servers);
  client->service_runner()->Run();
  return reinterpret_cast<CpfsFilesystem*>(client);
}

void CpfsShutdown(CpfsFilesystem* fs) {
  CLIENT_PTR(fs)->Shutdown();
  delete CLIENT_PTR(fs);
}

int CpfsOpen(const char* path, int flags, mode_t mode,
             CpfsFilesystem* fs, CpfsFileHandle* handle) {
  return CLIENT_PTR(fs)->api_common()->Open(path, flags, mode, handle);
}

int CpfsClose(CpfsFilesystem* fs, CpfsFileHandle* handle) {
  return CLIENT_PTR(fs)->api_common()->Close(handle);
}

int CpfsRead(char* buf, uint32_t size, off_t off,
             CpfsFilesystem* fs, CpfsFileHandle* handle,
             void (*callback)(int, void*), void* data) {
  if (callback) {
    CLIENT_PTR(fs)->fs_async_rw()->Add(new cpfs::client::APIReadRequest(
        buf, size, off, handle, callback, data));
    return 0;
  } else {
    return CLIENT_PTR(fs)->api_common()->Read(buf, size, off, handle);
  }
}

int CpfsWrite(const char* buf, uint32_t size, off_t off,
              CpfsFilesystem* fs, const CpfsFileHandle* handle,
              void (*callback)(int, void*), void* data) {
  if (callback) {
    CLIENT_PTR(fs)->fs_async_rw()->Add(new cpfs::client::APIWriteRequest(
        buf, size, off, handle, callback, data));
    return 0;
  } else {
    return CLIENT_PTR(fs)->api_common()->Write(buf, size, off, handle);
  }
}

int CpfsGetattr(struct stat* stbuf,
                CpfsFilesystem* fs, CpfsFileHandle* handle) {
  return CLIENT_PTR(fs)->api_common()->Getattr(stbuf, handle);
}

int CpfsSetattr(struct stat* stbuf, int to_set,
                CpfsFilesystem* fs, CpfsFileHandle* handle) {
  return CLIENT_PTR(fs)->api_common()->Setattr(stbuf, to_set, handle);
}

#ifdef __cplusplus
}
#endif
