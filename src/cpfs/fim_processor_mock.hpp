#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/shared_ptr.hpp>

#include "fim_processor.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((Accept, bool, (const FIM_PTR<IFim>&), CONST))                       \
  ((Process, bool,                                                      \
    (const FIM_PTR<IFim>&)(const boost::shared_ptr<IFimSocket>&)))

class MockIFimProcessor : public IFimProcessor {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
