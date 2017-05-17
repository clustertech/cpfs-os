#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/replier.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((DoReply, void,                                                      \
    (const FIM_PTR<IFim>&)(const FIM_PTR<IFim>&)                        \
    (const boost::shared_ptr<IFimSocket>&)(const OpContext*)            \
    (ReplierCallback)))                                                 \
  ((ExpireCallbacks, void, (int)))                                      \
  ((RedoCallbacks, void,))

class MockIReplier : public IReplier {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
