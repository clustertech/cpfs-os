/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of ConcurrentQueue.
 */

#include "concurrent_queue.hpp"

#include <cstddef>

#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/lockfree/queue.hpp>

namespace cpfs {

/**
 * Representation of the base concurrent queue.  This is an opaque
 * class for the pimpl idiom.
 */
struct BaseConcurrentQueueRepr {
  BaseConcurrentQueueRepr() : sem(0), data(512) {}

  /**
   * Semaphore used to wait when there is no data in the queue
   */
  boost::interprocess::interprocess_semaphore sem;

  /**
   * Queue to keep the elements not yet dequeued
   */
  boost::lockfree::queue<void*>  // pragma: NOLINT(build/include_what_you_use)
    data;
};

BaseConcurrentQueue::BaseConcurrentQueue()
    : repr_(new BaseConcurrentQueueRepr) {}

BaseConcurrentQueue::~BaseConcurrentQueue() {}

void BaseConcurrentQueue::enqueue(void* val) {
  repr_->data.push(val);
  repr_->sem.post();
}

void* BaseConcurrentQueue::dequeue() {
  repr_->sem.wait();
  for (;;) {
    void* res;
    if (repr_->data.pop(res))
      return res;
  }
}

void* BaseConcurrentQueue::try_dequeue() {
  if (!repr_->sem.try_wait())
    return NULL;
  for (;;) {
    void* res;
    if (repr_->data.pop(res))
      return res;
  }
}

}  // namespace cpfs
