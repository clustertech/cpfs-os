#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include <boost/bind.hpp>

#include "mock_helper.hpp"
#include "shaped_sender.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((SendFim, void, (const FIM_PTR<IFim>)))                              \
  ((WaitAllReplied, void,))

class MockIShapedSender : public IShapedSender {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

#define OBJ_METHODS                                                     \
  ((Make, IShapedSender*, (boost::shared_ptr<IReqTracker>)(unsigned)))

class MockShapedSenderMaker {
  MAKE_MOCK_METHODS(OBJ_METHODS);

  ShapedSenderMaker GetMaker() {
    return boost::bind(&MockShapedSenderMaker::Make, this, _1, _2);
  }
};

#undef OBJ_METHODS

}  // namespace cpfs
