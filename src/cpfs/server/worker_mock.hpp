#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/shared_ptr.hpp>

#include "mock_helper.hpp"
#include "server/worker.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {

#define OBJ_METHODS                                                     \
  ((Accept, bool, (const FIM_PTR<IFim>&), CONST))                       \
  ((Process, bool,                                                      \
    (const FIM_PTR<IFim>&)(const boost::shared_ptr<IFimSocket>&)))      \
  ((set_server, void, (BaseCpfsServer*)))                               \
  ((server, BaseCpfsServer*,))                                          \
  ((SetQueuer, void, (IFimProcessor*)))                                 \
  ((Enqueue, bool,                                                      \
    (const FIM_PTR<IFim>&)(const boost::shared_ptr<IFimSocket>&)))      \
  ((SetCacheInvalFunc, void, (CacheInvalFunc)))                         \
  ((GetMemoId, uint64_t,))                                              \
  ((PutMemo, void, (uint64_t)(boost::shared_ptr<WorkerMemo>)))          \
  ((GetMemo, boost::shared_ptr<WorkerMemo>, (uint64_t)))

class MockIWorker : public IWorker {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace server
}  // namespace cpfs
