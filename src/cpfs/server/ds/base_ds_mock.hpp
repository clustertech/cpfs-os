#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/base_server_mock.hpp"
#include "server/ds/base_ds.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ds {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((Run, int,))                                                         \
  ((Shutdown, void,))

class MockBaseDataServer : public BaseDataServer {
 public:
  MockBaseDataServer(ConfigMgr configs = MakeEmptyConfigMgr())
      : BaseDataServer(configs) {}
  MAKE_MOCK_METHODS(OBJ_METHODS)
};

#undef OBJ_METHODS

}  // namespace ds
}  // namespace server
}  // namespace cpfs
