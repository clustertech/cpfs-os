#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <string>

#include "mock_helper.hpp"
#include "client/conn_mgr.hpp"  // IWYU pragma: export

namespace cpfs {
namespace client {

#define OBJ_METHODS                                                     \
  ((Init, void, (const std::vector<std::string>&)))                     \
  ((ForgetMS, void, (boost::shared_ptr<IFimSocket>)(bool)))             \
  ((GetMSRejectInfo, std::vector<bool>,, CONST))                        \
  ((ReconnectMS, void, (boost::shared_ptr<IFimSocket>)))                \
  ((IsReconnectingMS, bool,, CONST))                                    \
  ((SetReconnectingMS, void, (bool)))                                   \
  ((SetForceStartMS, void, (unsigned)))                                 \
  ((SetInitConnRetry, void, (unsigned)))                                \
  ((ConnectDS, void, (uint32_t)(uint16_t)(GroupId)(GroupRole)))

class MockIConnMgr : public IConnMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS


}  // namespace client
}  // namespace cpfs
