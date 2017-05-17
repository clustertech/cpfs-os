#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <string>

#include "mock_helper.hpp"
#include "client/cache.hpp"  // IWYU pragma: export

namespace cpfs {
namespace client {

#define OBJ_METHODS                                                     \
  ((GetMutex, MUTEX_TYPE*,))                                            \
  ((InodeInvalidated, bool, (InodeNum)(bool), CONST))

class MockICacheInvRecord : public ICacheInvRecord {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((SetFuseChannel, void, (fuse_chan*)))                                \
  ((Init, void, ))                                                      \
  ((Shutdown, void, ))                                                  \
  ((StartLookup, boost::shared_ptr<ICacheInvRecord>, ))           \
  ((AddEntry, void, (InodeNum)(const std::string&)(bool)))              \
  ((RegisterInode, void, (InodeNum)))                                   \
  ((InvalidateInode, void, (InodeNum)(bool)))                           \
  ((InvalidateAll, void, ))                                             \
  ((CleanMgr, void, (int)))

class MockICacheMgr : public ICacheMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace client
}  // namespace cpfs
