#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "req_limiter.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Send, bool,                                                         \
    (const boost::shared_ptr<IReqEntry>&)                               \
    (boost::unique_lock<MUTEX_TYPE>*)))

class MockIReqLimiter : public IReqLimiter {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
