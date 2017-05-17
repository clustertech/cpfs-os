#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <string>
#include <vector>

#include <gmock/gmock.h>

#include "mock_helper.hpp"
#include "server/ms/dsg_alloc.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Allocate, std::vector<GroupId>, (GroupId)))                         \
  ((Advise, void, (std::string)(GroupId)(int64_t)))                     \
  ((SetStatKeeper, void, (IStatKeeper*)))

class MockIDSGAllocator : public IDSGAllocator {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
