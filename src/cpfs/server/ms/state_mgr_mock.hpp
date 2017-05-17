#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/state_mgr.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Elect, bool, (uint64_t)(bool)))                                     \
  ((IsActive, bool,, CONST))                                            \
  ((SetActive, void,))                                                  \
  ((ConfirmActive, void,))                                              \
  ((GetCount, uint64_t,, CONST))                                        \
  ((PersistCount, void, (uint64_t), CONST))

class MockIHACounter : public IHACounter {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((SwitchState, bool, (MSState)))                                      \
  ((GetState, MSState,, CONST))                                         \
  ((OnState, void, (MSState)(MSStateChangeCallback)))

class MockIStateMgr : public IStateMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
