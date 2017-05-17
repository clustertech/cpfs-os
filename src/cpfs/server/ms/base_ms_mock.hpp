#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/base_server_mock.hpp"
#include "server/ms/base_ms.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((Run, int,))                                                         \
  ((Shutdown, void,))                                                   \
  ((StartServerActivated, void,))                                       \
  ((PrepareActivate, void,))

class MockBaseMetaServer : public BaseMetaServer {
 public:
  MockBaseMetaServer(ConfigMgr configs = MakeEmptyConfigMgr())
      : BaseMetaServer(configs) {}
  MAKE_MOCK_METHODS(OBJ_METHODS)
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
