#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/inode_removal_tracker.hpp"

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((RecordRemoved, void, (InodeNum)))                                   \
  ((SetPersistRemoved, void, (bool)))                                   \
  ((GetRemovedInodes, std::vector<InodeNum>,))                          \
  ((ExpireRemoved, void, (int)))

class MockIInodeRemovalTracker : public IInodeRemovalTracker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
