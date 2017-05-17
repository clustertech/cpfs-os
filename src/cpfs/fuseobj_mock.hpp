#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "fuseobj.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define MOCK_MAKE_FUSE_DESC(fname, mname, rtype, args) ((mname, rtype, args))
#define OBJ_METHODS FUSEOBJ_FOR_FUSEFUNC(MOCK_MAKE_FUSE_DESC)

class MockFuseMethodPolicy {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

class MockFuseMethodPolicyF {
  MAKE_MOCK_FORWARDER(MockFuseMethodPolicy, OBJ_METHODS);
};

#undef OBJ_METHODS
#undef MOCK_MAKE_FUSE_DESC

}  // namespace cpfs
