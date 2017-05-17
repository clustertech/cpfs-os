#pragma once

/* Copyright 2015 ClusterTech Ltd */

#include "client/fs_common_lowlevel.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {
namespace client {

#define OBJ_METHODS                                                     \
  ((SetClient, void, (BaseFSClient*)))                                  \
  ((Open, bool, (const FSIdentity&)(uint64_t)(int)(FSOpenReply*)))      \
  ((Lookup, bool,                                                       \
    (const FSIdentity&)(uint64_t)(const char*)(FSLookupReply*)))        \
  ((Create, bool,                                                       \
    (const FSIdentity&)(uint64_t)(const char*)(mode_t)(int)             \
    (FSCreateReply*)))                                                  \
  ((Read, bool,                                                         \
    (uint64_t)(uint64_t)(std::size_t)(off_t)(FSReadReply*)))            \
  ((Readv, bool,                                                        \
    (uint64_t)(uint64_t)(std::size_t)(off_t)(FSReadvReply*)))           \
  ((Write, bool,                                                        \
    (uint64_t)(uint64_t)(const char*)(std::size_t)(off_t)               \
    (FSWriteReply*)))                                                   \
  ((Release, void, (uint64_t)(uint64_t*)(int)))                         \
  ((Getattr, bool, (const FSIdentity&)(uint64_t)(FSGetattrReply*)))     \
  ((Setattr, bool,                                                      \
    (const FSIdentity&)(uint64_t)(const struct stat*)(int)              \
    (FSSetattrReply*)))                                                 \
  ((Flush, void, (uint64_t)))                                           \
  ((Fsync, void, (uint64_t)))

class MockIFSCommonLL : public IFSCommonLL {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace client
}  // namespace cpfs
