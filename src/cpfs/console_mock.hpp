#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include <string>
#include <vector>

#include "console.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((ReadLine, std::string,))                                            \
  ((PrintLine, void, (const std::string&)))                             \
  ((PrintTable, void, (const std::vector<std::vector<std::string> >&)))

class MockIConsole : public IConsole {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
