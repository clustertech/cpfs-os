#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/durable_range.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((Load, bool,))                                                       \
  ((SetConservative, void, (bool)))                                     \
  ((Add, void, (InodeNum)(InodeNum)(InodeNum)(InodeNum)))               \
  ((Latch, void,))                                                      \
  ((Clear, void,))                                                      \
  ((Get, std::vector<InodeNum>,))

class MockIDurableRange : public IDurableRange {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
