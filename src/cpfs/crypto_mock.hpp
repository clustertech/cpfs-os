#pragma once

/* Copyright 2014 ClusterTech Ltd */

#include <string>

#include "crypto.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((CreateNonce, Nonce,))                                               \
  ((CreateKey, std::string,))                                           \
  ((Sign, void, (const std::string&)(const std::string&)(char*)))

class MockICrypto : public ICrypto {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
