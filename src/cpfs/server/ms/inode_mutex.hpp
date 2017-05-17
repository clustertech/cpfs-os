#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define and implement the InodeMutex class, as well as its guard.
 * This wraps over the boost::recursive_mutex interface, providing an
 * array of mutex, and facilities to lock up to INODE_MUTEX_MAX of
 * them at a time.
 */

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <new>
#include <vector>

#include <boost/bind.hpp>
#include <boost/noncopyable.hpp>
#include <boost/preprocessor/arithmetic/inc.hpp>
#include <boost/preprocessor/repetition/enum_params.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>
#include <boost/preprocessor/repetition/repeat_from_to.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/recursive_mutex.hpp>

#include "common.hpp"
#include "mutex_util.hpp"

/**
 * Maximum number of mutex to concurrently lock when generating
 * InodeMutexGuard constructors.
 */
#define INODE_MUTEX_MAX 3

namespace cpfs {

struct ReqContext;

namespace server {
namespace ms {

/**
 * Represent an array of inode mutex.  This class carefully manage the
 * location of the mutexes.  The default size of a Boost.Thread mutex
 * (came from pthread) is slightly smaller than the cache line,
 * probably because it expects that some additional data is attached
 * to the object having the mutex.  As a simple array of mutex, we
 * don't have any such data, which can cause false sharing of mutexes
 * if we just create an array of recursive_mutex.  Instead, we use
 * placement new (i.e., "new(ptr) type") and placement delete
 * ("obj->~type()") to manually create the recursive mutexes in the
 * location that is spaced by the cache line size (as determined by
 * sysconf).
 */
class InodeMutexArray : private boost::noncopyable {
 public:
  /**
   * Create an inode mutex array.
   *
   * @param num_mutex The number of mutexes in the array
   */
  explicit InodeMutexArray(unsigned num_mutex) : num_mutex_(num_mutex) {
    entry_size_ = std::max(sizeof(boost::recursive_mutex),
                           std::size_t(sysconf(_SC_LEVEL1_DCACHE_LINESIZE)));
    // Add one entry so that we can do manual alignment
    char* buf = new char[(num_mutex + 1) * entry_size_];
    buf_.reset(buf);
    data_ = buf + entry_size_;
    // Make the entries aligned
    data_ -= reinterpret_cast<intptr_t>(data_) % entry_size_;
    for (unsigned i = 0; i < num_mutex; ++i)
      new(reinterpret_cast<char*>(GetMutex(i))) boost::recursive_mutex;
  }

  ~InodeMutexArray() {
    for (unsigned i = 0; i < num_mutex_; ++i)
      GetMutex(i)->~recursive_mutex();
  }

  /**
   * Lock the mutex used for an inode.
   *
   * @param inode The inode
   */
  void Lock(InodeNum inode) {
    GetMutex(GetMutexIndex(inode))->lock();
  }

  /**
   * Unlock the mutex used for an inode.
   *
   * @param inode The inode
   */
  void Unlock(InodeNum inode) {
    GetMutex(GetMutexIndex(inode))->unlock();
  }

  /**
   * Check whether inode_a should be locked before or after inode_b.
   *
   * @param inode_a The first inode
   *
   * @param inode_b The second inode
   *
   * @return Whether inode_a should be locked before inode_b
   */
  int GetLockOrder(InodeNum inode_a, InodeNum inode_b) {
    return GetMutexIndex(inode_a) < GetMutexIndex(inode_b);
  }

 private:
  unsigned num_mutex_; /**< The number of mutexes */
  boost::scoped_array<char> buf_; /**< The buffer */
  char* data_; /**< The data area */
  std::size_t entry_size_; /**< Size of each entry */

  /**
   * Get a mutex given an index.
   */
  boost::recursive_mutex* GetMutex(unsigned idx) {
    return reinterpret_cast<boost::recursive_mutex*>(data_ + idx * entry_size_);
  }

  /**
   * Find the mutex used for an inode.
   *
   * @param inode The inode
   *
   * @return The index of the mutex to use
   */
  unsigned GetMutexIndex(InodeNum inode) {
    return inode % num_mutex_;
  }
};

/**
 * A guard to be created to lock one or two inode mutexes in an array,
 * so that when the guard is destroyed the mutexes are unlocked.
 */
class InodeMutexGuard : private boost::noncopyable {
 public:
  /**
   * Internal macro to generate a InodeMutexGuard initialization
   * statement.
   */
#define INODE_MUTEX_INIT_CNT(z, i, text) inodes_[i] = inode ## i;
  /**
   * Internal macro to generate a InodeMutexGuard constructor.
   */
#define INODE_MUTEX_CTOR(z, n, text)                                    \
  InodeMutexGuard(InodeMutexArray* array,                               \
                  BOOST_PP_ENUM_PARAMS(n, InodeNum inode))              \
      __attribute__((unused))                                           \
      : array_(array), num_inodes_(n), inodes_(new InodeNum[n]) {       \
    BOOST_PP_REPEAT(n, INODE_MUTEX_INIT_CNT, dummy);                    \
    Lock();                                                             \
  }
  /**
   * Constructors, takes a pointer to InodeMutexArray and 1 to
   * INODE_MUTEX_MAX InodeNum as arguments.
   */
  BOOST_PP_REPEAT_FROM_TO(1, BOOST_PP_INC(INODE_MUTEX_MAX),
                          INODE_MUTEX_CTOR, dummy);
#undef INODE_MUTEX_CTOR
#undef INODE_MUTEX_INIT_CNT

  ~InodeMutexGuard() {
    for (unsigned i = 0; i < num_inodes_; ++i)
      array_->Unlock(inodes_[i]);
  }

 private:
  InodeMutexArray* array_;
  unsigned num_inodes_;
  boost::scoped_array<InodeNum> inodes_;

  void Lock() {
    std::sort(inodes_.get(), inodes_.get() + num_inodes_,
              boost::bind(&InodeMutexArray::GetLockOrder, array_, _1, _2));
    MUTEX_LOCK_WRAP(for (unsigned i = 0; i < num_inodes_; ++i)
                      array_->Lock(inodes_[i]));
  }
};

/**
 * Operation context.  The request context is an input to the
 * operation, allowing it to find information like requesting user ID,
 * group ID and operation time that are common to many requests.  The
 * mutex guard allows the operation to lock some inodes while keeping
 * the locks when the operation returns, until the operation context
 * is destroyed (or the guard reset).  The inode changed and read
 * array allows the caller to know which inodes are changed and read,
 * so as to arrange replies to the caller at an appropriate time to
 * achieve consistent replication.
 */
struct OpContext {
  const ReqContext* req_context;  /**< Request context of FC */
  /** Acquire and release locks */
  boost::scoped_ptr<InodeMutexGuard> i_mutex_guard;
  std::vector<InodeNum> inodes_changed; /**< inodes changed by the operation */
  std::vector<InodeNum> inodes_read; /**< inodes read by the operation */
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
