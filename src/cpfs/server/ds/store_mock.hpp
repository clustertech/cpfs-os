#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ds/store.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ds {

#define OBJ_METHODS                                                     \
  ((GetInfo, void, (FSTime*)(uint64_t*)))                               \
  ((GetNext, std::size_t, (std::size_t*)(char*)))

class MockIFileDataIterator : public IFileDataIterator {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Write, void, (char*)(std::size_t)(std::size_t)))                    \
  ((fd, int,))

class MockIFileDataWriter : public IFileDataWriter {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((GetInfo, void, (FSTime*)(uint64_t*)))                               \
  ((GetNext, std::size_t, (std::size_t*)(char*)))

class MockIChecksumGroupIterator : public IChecksumGroupIterator {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Write, void, (char*)(std::size_t)(std::size_t)))

class MockIChecksumGroupWriter : public IChecksumGroupWriter {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((SetPosixFS, void, (IPosixFS*)))                                     \
  ((SetInodeRemovalTracker, void, (IInodeRemovalTracker*)))             \
  ((SetDurableRange, void, (IDurableRange*)))                           \
  ((is_role_set, bool,))                                                \
  ((ds_group, GroupId,))                                                \
  ((ds_role, GroupRole,))                                               \
  ((GetUUID, std::string,, CONST))                                      \
  ((SetRole, void, (GroupId)(GroupRole)))                               \
  ((GetChecksumGroupIterator, IChecksumGroupIterator*, (InodeNum)))     \
  ((GetChecksumGroupWriter, IChecksumGroupWriter*, (InodeNum)))         \
  ((List, IDirIterator*,))                                              \
  ((InodeList, IDirIterator*, (const std::vector<InodeNum>&)))          \
  ((RemoveAll, void,))                                                  \
  ((Write, int,                                                         \
    (InodeNum)(const FSTime&)(uint64_t)(std::size_t)(const void*)       \
    (std::size_t)(void*)))                                              \
  ((Read, int, (InodeNum)(bool)(std::size_t)(void*)(std::size_t)))      \
  ((ApplyDelta, int,                                                    \
    (InodeNum)(const FSTime&)(uint64_t)(std::size_t)(const void*)       \
    (std::size_t)(bool)))                                               \
  ((TruncateData, int,                                                  \
    (InodeNum)(const FSTime&)(std::size_t)(std::size_t)(std::size_t)    \
    (char*)))                                                           \
  ((UpdateMtime, int, (InodeNum)(const FSTime&)(uint64_t*)))            \
  ((FreeData, int, (InodeNum)(bool)))                                   \
  ((GetAttr, int, (InodeNum)(FSTime*)(uint64_t*)))                      \
  ((SetAttr, int,                                                       \
    (InodeNum)(const FSTime&)(uint64_t)(SetFileXattrFlags)))            \
  ((UpdateAttr, void, (InodeNum)(const FSTime&)(uint64_t)))             \
  ((Stat, int, (uint64_t*)(uint64_t*)))

class MockIStore : public IStore {
  MAKE_MOCK_METHODS(OBJ_METHODS)
};

#undef OBJ_METHODS

}  // namespace ds
}  // namespace server
}  // namespace cpfs
