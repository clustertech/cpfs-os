#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ds/cleaner.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ds {

#define OBJ_METHODS                                                     \
  ((SetPeriodicTimerMaker, void, (PeriodicTimerMaker)))                 \
  ((SetMinInodeRemoveTime, void, (unsigned)))                           \
  ((Init, void,))                                                       \
  ((Shutdown, void,))                                                   \
  ((RemoveInodeLater, void, (InodeNum)))

class MockICleaner : public ICleaner {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ds
}  // namespace server
}  // namespace cpfs
