#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/server_info.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((List, std::vector<std::string>,, CONST))                            \
  ((Get, std::string, (const std::string&)(const std::string&), CONST)) \
  ((Set, void, (const std::string&)(const std::string&)))               \
  ((Remove, void, (const std::string&)))

class MockIServerInfo : public IServerInfo {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
