#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/conn_mgr.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((Init, void,))                                                       \
  ((InitMSConn, void,                                                   \
    (const boost::shared_ptr<IFimSocket>&)(uint64_t)(bool)))            \
  ((CleanupFC, void, (const boost::shared_ptr<IFimSocket>&)))           \
  ((CleanupDS, void, (GroupId)(GroupRole)))

class MockIConnMgr : public IConnMgr {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
