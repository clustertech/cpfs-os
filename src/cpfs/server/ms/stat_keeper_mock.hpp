#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/stat_keeper.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Run, void,))                                                        \
  ((OnAllStat, void, (SpaceStatCallback)))                              \
  ((GetLastStat, AllDSSpaceStat,))                                      \
  ((OnNewStat, void, (SpaceStatCallback)))

class MockIStatKeeper : public IStatKeeper {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
