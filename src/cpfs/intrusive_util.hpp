#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the RefCount class for use with intrusive pointers.
 */

#include <boost/atomic.hpp>

namespace cpfs {

/**
 * Provide reference count for embedded into objects.  When the
 * referenc count reaches zero, the object is deleted.  This code is
 * adapted from the manual of Boost.Atomic for refcount.
 *
 * @tparam TObj The class of stored objects
 */
template <typename TObj>
class SimpleRefCount {
 public:
  SimpleRefCount() : refcount_(0) {}
  virtual ~SimpleRefCount() {}

  /**
   * Add a reference to the object.
   */
  void AddRef() {
    refcount_.fetch_add(1, boost::memory_order_relaxed);
  }

  /**
   * Remove a reference to the object.  If the last reference is
   * removed, the object is either freed or put back to the pool
   * allocating it.
   */
  void Release(const TObj* obj) {
    if (refcount_.fetch_sub(1, boost::memory_order_release) == 1) {
      boost::atomic_thread_fence(boost::memory_order_acquire);
      Delete(obj);
    }
  }

  /**
   * Called when the reference count reaches zero.  By default, the
   * object is deleted.  Note that the RefCount object is likely
   * embedded within the object, so no code should be put after
   * calling Delete (or otherwise deleting obj).
   *
   * @param obj The object to delete
   */
  virtual void Delete(const TObj* obj) {
    delete obj;
  }

 private:
  boost::atomic<int> refcount_; /**< The reference count */
};

}  // namespace cpfs
