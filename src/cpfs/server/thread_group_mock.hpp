#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/thread_group.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((SetThreadFimProcessorMaker, void, (ThreadFimProcessorMaker)))       \
  ((AddWorker, void, (IWorker*)))                                       \
  ((num_workers, unsigned,))                                            \
  ((Start, void,))                                                      \
  ((Stop, void, (int)))                                                 \
  ((Accept, bool, (const FIM_PTR<IFim>&), CONST))                       \
  ((Process, bool,                                                      \
    (const FIM_PTR<IFim>&)                                              \
    (const boost::shared_ptr<IFimSocket>&)))                            \
  ((EnqueueAll, void, (const FIM_PTR<IFim>&)))                          \
  ((SocketPending, bool, (const boost::shared_ptr<IFimSocket>&),        \
    CONST))

class MockIThreadGroup : public IThreadGroup {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
