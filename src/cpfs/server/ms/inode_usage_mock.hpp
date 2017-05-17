#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/inode_usage.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((IsOpened, bool, (InodeNum)(bool), CONST))                           \
  ((IsSoleWriter, bool, (ClientNum)(InodeNum), CONST))                  \
  ((SetFCOpened, void, (ClientNum)(InodeNum)(InodeAccess)))             \
  ((SetFCClosed, bool, (ClientNum)(InodeNum)(bool)))                    \
  ((GetFCOpened, boost::unordered_set<InodeNum>, (ClientNum), CONST))   \
  ((SwapClientOpened, void, (ClientNum)(InodeAccessMap*)))              \
  ((client_opened, ClientInodeMap,, CONST))                             \
  ((AddPendingUnlink, void, (InodeNum)))                                \
  ((AddPendingUnlinks, void, (const InodeSet&)))                        \
  ((RemovePendingUnlink, void, (InodeNum)))                             \
  ((IsPendingUnlink, bool, (InodeNum), CONST))                          \
  ((ClearPendingUnlink, void, ))                                        \
  ((pending_unlink, InodeSet,, CONST))

class MockIInodeUsage : public IInodeUsage {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
