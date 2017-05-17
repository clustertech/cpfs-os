#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define of ConcurrentQueue.
 */

#include <boost/scoped_ptr.hpp>

namespace cpfs {

struct BaseConcurrentQueueRepr;

/**
 * Base concurrent queue.  This is an implementation of the concurrent
 * queue with fixed void* type.
 */
class BaseConcurrentQueue {
 public:
  BaseConcurrentQueue();
  ~BaseConcurrentQueue();  // Don't instantiate this outside implementation

  /**
   * Enqueue an element to the queue.  Never block.
   *
   * @param val The element to enqueue
   */
  void enqueue(void* val);

  /**
   * Dequeue an element from the queue.  Block if there has been no
   * element in the queue.
   *
   * @return The element to dequeued
   */
  void* dequeue();

  /**
   * Dequeue an element from the queue.  Return NULL if there is no
   * element in the queue.
   *
   * @return The element to dequeued
   */
  void* try_dequeue();

 private:
  boost::scoped_ptr<BaseConcurrentQueueRepr> repr_;
};

/**
 * Concurrent queue.  It is a thin template to support pointers of
 * different types.
 */
template <typename T>
class ConcurrentQueue {
 public:
  /**
   * Enqueue an element to the queue.  Never block.
   *
   * @param val The element to enqueue
   */
  void enqueue(T* val) {
    data_.enqueue(reinterpret_cast<void*>(val));
  }

  /**
   * Dequeue an element from the queue.  Block if there has been no
   * element in the queue.
   *
   * @return The element to dequeued
   */
  T* dequeue() {
    return reinterpret_cast<T*>(data_.dequeue());
  }

  /**
   * Dequeue an element from the queue.  Return NULL if there is no
   * element in the queue.
   *
   */
  T* try_dequeue() {
    return reinterpret_cast<T*>(data_.try_dequeue());
  }

 private:
  BaseConcurrentQueue data_;
};

}  // namespace cpfs
