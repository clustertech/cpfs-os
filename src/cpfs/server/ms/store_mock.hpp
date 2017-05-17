#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "finfo.hpp"
#include "mock_helper.hpp"
#include "server/ms/store.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((IsInitialized, bool,))                                              \
  ((Initialize, void,))                                                 \
  ((PrepareUUID, void,))                                                \
  ((GetUUID, std::string,, CONST))                                      \
  ((LoadAllUUID, UUIDInfoMap,))                                         \
  ((PersistAllUUID, void, (const UUIDInfoMap&)))                        \
  ((SetUgidHandler, void, (IUgidHandler*)))                             \
  ((SetDSGAllocator, void, (IDSGAllocator*)))                           \
  ((SetInodeSrc, void, (IInodeSrc*)))                                   \
  ((SetInodeUsage, void, (IInodeUsage*)))                               \
  ((SetInodeRemovalTracker, void, (IInodeRemovalTracker*)))             \
  ((SetDurableRange, void, (IDurableRange*)))                           \
  ((List, IDirIterator*, (const std::string&)))                         \
  ((InodeList, IDirIterator*, (const std::vector<InodeNum>&)))          \
  ((GetInodeAttr, int, (InodeNum)(FileAttr*)))                          \
  ((UpdateInodeAttr, int, (InodeNum)(FSTime)(uint64_t)))                \
  ((GetFileGroupIds, int, (InodeNum)(std::vector<GroupId>*)))           \
  ((SetLastResyncDirTimes, int,))                                       \
  ((RemoveInodeSince, int, (const std::vector<InodeNum>&)(uint64_t)))   \
  ((OperateInode, void, (OpContext*)(InodeNum)))                        \
  ((Attr, int,                                                          \
    (OpContext*)(InodeNum)(const FileAttr*)(uint32_t)(bool)(FileAttr*)  \
  ))                                                                    \
  ((SetXattr, int,                                                      \
    (OpContext*)(InodeNum)(const char*)(const char*)(std::size_t)(int)  \
  ))                                                                    \
  ((GetXattr, int,                                                      \
    (OpContext*)(InodeNum)(const char*)(char*)(std::size_t)))           \
  ((ListXattr, int, (OpContext*)(InodeNum)(char*)(std::size_t)))        \
  ((RemoveXattr, int, (OpContext*)(InodeNum)(const char*)))             \
  ((Open, int,                                                          \
    (OpContext*)(InodeNum)(int32_t)(std::vector<GroupId>*)(bool*)))     \
  ((Access, int, (OpContext*)(InodeNum)(int32_t)))                      \
  ((AdviseWrite, int, (OpContext*)(InodeNum)(uint64_t)))                \
  ((FreeInode, int, (InodeNum)))                                        \
  ((Readlink, int, (OpContext*)(InodeNum)(std::vector<char>*)))         \
  ((Lookup, int,                                                        \
    (OpContext*)(InodeNum)(const char*)(InodeNum*)(FileAttr*)))         \
  ((Opendir, int, (OpContext*)(InodeNum)))                              \
  ((Readdir, int,                                                       \
    (OpContext*)(InodeNum)(DentryCookie)(std::vector<char>*)))          \
  ((Create, int,                                                        \
    (OpContext*)(InodeNum)(const char*)(const CreateReq*)               \
    (std::vector<GroupId>*)(FileAttr*)                                  \
    (boost::scoped_ptr<RemovedInodeInfo>*)))                            \
  ((Mkdir, int,                                                         \
    (OpContext*)(InodeNum)(const char*)(const CreateDirReq*)(FileAttr*) \
  ))                                                                    \
  ((Symlink, int,                                                       \
    (OpContext*)(InodeNum)(const char*)(InodeNum)(const char*)          \
    (FileAttr*)))                                                       \
  ((Mknod, int,                                                         \
    (OpContext*)(InodeNum)(const char*)(InodeNum)(uint64_t)(uint64_t)   \
    (std::vector<GroupId>*)(FileAttr*)))                                \
  ((ResyncInode, int,                                                   \
    (InodeNum)(const FileAttr&)(const char*)(std::size_t)(std::size_t)  \
  ))                                                                    \
  ((ResyncDentry, int,                                                  \
    (uint32_t)(uint32_t)(InodeNum)(InodeNum)(unsigned char)             \
    (const char*)))                                                     \
  ((ResyncClientDir, int, (ClientNum)(InodeNum)))                       \
  ((ResyncRemoval, int, (InodeNum)))                                    \
  ((Link, int,                                                          \
    (OpContext*)(InodeNum)(InodeNum)(const char*)(FileAttr*)))          \
  ((Rename, int,                                                        \
    (OpContext*)(InodeNum)(const char*)(InodeNum)(const char*)          \
    (InodeNum*)(boost::scoped_ptr<RemovedInodeInfo>*)))                 \
  ((Unlink, int,                                                        \
    (OpContext*)(InodeNum)(const char*)                                 \
    (boost::scoped_ptr<RemovedInodeInfo>*)))                            \
  ((Rmdir, int, (OpContext*)(InodeNum)(const char*)))                   \
  ((Stat, int, (uint64_t*)(uint64_t*)))

class MockIStore : public IStore {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
