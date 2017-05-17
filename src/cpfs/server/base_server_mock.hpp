#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/base_server.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

inline ConfigMgr MakeEmptyConfigMgr() {
  ConfigMgr ret;
  ret.set_ds_port(0);
  ret.set_ms1_port(0);
  ret.set_ms2_port(0);
  return ret;
}

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((Run, int,))                                                         \
  ((Shutdown, void,))

class MockICpfsServer : public BaseCpfsServer {
 public:
  MockICpfsServer(ConfigMgr configs = MakeEmptyConfigMgr())
      : BaseCpfsServer(configs) {}
  MAKE_MOCK_METHODS(OBJ_METHODS)
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
