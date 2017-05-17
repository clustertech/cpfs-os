#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/bind.hpp>

#include "mock_helper.hpp"
#include "thread_fim_processor.hpp"  // IWYU pragma: export

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Start, void,))                                                      \
  ((Stop, void,))                                                       \
  ((Join, void,))                                                       \
  ((Accept, bool, (const FIM_PTR<IFim>&), CONST))                       \
  ((Process, bool,                                                      \
    (const FIM_PTR<IFim>&)(const boost::shared_ptr<IFimSocket>&)))      \
  ((SocketPending, bool, (const boost::shared_ptr<IFimSocket>&),        \
    CONST))

class MockIThreadFimProcessor : public IThreadFimProcessor {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Make, IThreadFimProcessor*, (IFimProcessor*)))

class MockThreadFimProcessorMaker {
  MAKE_MOCK_METHODS(OBJ_METHODS);

  ThreadFimProcessorMaker GetMaker() {
    return boost::bind(&MockThreadFimProcessorMaker::Make, this, _1);
  }
};

#undef OBJ_METHODS

}  // namespace cpfs
