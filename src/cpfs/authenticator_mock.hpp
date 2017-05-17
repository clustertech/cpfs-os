#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/shared_ptr.hpp>

#include "authenticator.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((SetPeriodicTimerMaker, void, (PeriodicTimerMaker)(unsigned)))       \
  ((AsyncAuth, void,                                                    \
    (const boost::shared_ptr<IFimSocket>&)(AuthHandler)(bool)))

class MockIAuthenticator : public IAuthenticator {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
