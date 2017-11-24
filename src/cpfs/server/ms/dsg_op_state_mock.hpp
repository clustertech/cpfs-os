#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <gmock/gmock.h>

#include "mock_helper.hpp"
#include "server/ms/dsg_op_state.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((RegisterInodeOp, void, (InodeNum)(const void*)))                    \
  ((CompleteInodeOp, void, (InodeNum)(const void*)))                    \
  ((OnInodesCompleteOp, void,                                           \
    (const std::vector<InodeNum>)(OpCompletionCallback)))               \
  ((ReadLock, void,                                                     \
    (GroupId)(boost::shared_lock<boost::shared_mutex>*)))               \
  ((SetDsgInodesResyncing, void,                                        \
    (GroupId)(const std::vector<InodeNum>&)))                           \
  ((is_dsg_inode_resyncing, bool, (GroupId)(InodeNum)))

class MockIDSGOpStateMgr : public IDSGOpStateMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
