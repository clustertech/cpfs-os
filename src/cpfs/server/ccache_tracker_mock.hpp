#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ccache_tracker.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((Size, std::size_t, , CONST))                                        \
  ((RemoveClient, void, (ClientNum)))                                   \
  ((SetCache, void, (InodeNum)(ClientNum)))                             \
  ((InvGetClients, void, (InodeNum)(ClientNum)(CCacheExpiryFunc)))      \
  ((ExpireCache, void, (int)(CCacheExpiryFunc)))

class MockICCacheTracker : public ICCacheTracker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
