#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/reply_set.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((FindReply, FIM_PTR<IFim>, (ReqId), CONST))                          \
  ((AddReply, void, (const FIM_PTR<IFim>&)))                            \
  ((ExpireReplies, void, (int)))                                        \
  ((RemoveClient, void, (ClientNum)))                                   \
  ((IsEmpty, bool,, CONST))

class MockIReplySet : public IReplySet {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
