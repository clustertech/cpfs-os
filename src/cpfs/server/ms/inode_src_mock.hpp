#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/inode_src.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((SetupAllocation, void,))                                            \
  ((Allocate, InodeNum, (InodeNum)(bool)))                              \
  ((NotifyUsed, void, (InodeNum)))                                      \
  ((NotifyRemoved, void, (InodeNum)))                                   \
  ((GetLastUsed, std::vector<InodeNum>,))                               \
  ((SetLastUsed, void, (const std::vector<InodeNum>&)))

class MockIInodeSrc : public IInodeSrc {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
