#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ds/conn_mgr.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ds {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((SetDsMsFimProcessor, void, (IFimProcessor*)))                       \
  ((DisconnectMS, void, (boost::shared_ptr<IFimSocket>)))               \
  ((ReconnectMS, void, (boost::shared_ptr<IFimSocket>)))                \
  ((ConnectDS, void, (uint32_t)(uint16_t)(GroupId)(GroupRole)))

class MockIConnMgr : public IConnMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ds
}  // namespace server
}  // namespace cpfs
