/* Copyright 2013 ClusterTech Ltd */
#include "intrusive_util.hpp"

#include <boost/intrusive_ptr.hpp>

#include <gtest/gtest.h>

namespace cpfs {
namespace {

class Obj {
 private:
  mutable SimpleRefCount<Obj> cnt_;
  friend void intrusive_ptr_add_ref(const Obj* obj);
  friend void intrusive_ptr_release(const Obj* obj);
};

void intrusive_ptr_add_ref(const Obj* obj) {
  obj->cnt_.AddRef();
}

void intrusive_ptr_release(const Obj* obj) {
  obj->cnt_.Release(obj);
}

}  // namespace

// Explicit instantiate class template to better detect missing coverage
template class SimpleRefCount<Obj>;

namespace {

TEST(IntrusiveUtilTest, Basic) {
  boost::intrusive_ptr<Obj> obj(new Obj);
  boost::intrusive_ptr<Obj> obj2(new Obj);
  obj2 = obj;
}

}  // namespace
}  //  namespace cpfs
