#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define TimedList. A thread-safe structure for storing elements to std::list,
 * with add time tracked. Elements expired can be removed from the list.
 */

#include <sys/time.h>

#include <ctime>
#include <list>
#include <utility>
#include <vector>

#include "mutex_util.hpp"
#include "util.hpp"

namespace cpfs {

/**
 * Implement the TimedList
 *
 * @tparam TElem The type of element to add
 */
template <typename TElem>
class TimedList {
 public:
  /** Type for time, element pair */
  typedef std::pair<struct timespec, TElem> Entry;
  typedef std::list<Entry> Entries; /**< Type for list of time, element pair */
  typedef std::vector<TElem> Elems;  /**< Type for list of elements */

  TimedList() : data_mutex_(MUTEX_INIT) {}

  /**
   * Add an element to the list
   *
   * @param elem The element to add
   */
  void Add(TElem elem) {
    MUTEX_LOCK_GUARD(data_mutex_);
    struct timespec time_added;
    clock_gettime(CLOCK_MONOTONIC, &time_added);
    entries_.push_back(std::make_pair(time_added, elem));
  }

  /**
   * Expire elements in the list
   *
   * @param min_age The minimum age of element remain in the list
   *
   * @param expired_ret Where to put expired elements.  If 0, the
   * information is not returned
   */
  void Expire(int min_age, Elems* expired_ret = 0) {
    MUTEX_LOCK_GUARD(data_mutex_);
    CompareTime ct;
    struct timespec max_time_added;
    clock_gettime(CLOCK_MONOTONIC, &max_time_added);
    max_time_added.tv_sec -= min_age;
    typename Entries::iterator it = entries_.begin();
    while (it != entries_.end()) {
      if (!ct(it->first, max_time_added))
        break;
      if (expired_ret)
        expired_ret->push_back(it->second);
      it = entries_.erase(it);
    }
  }

  /**
   * Fetch and return the elements from the list, original items are removed
   * from this object.
   *
   * @return List elements
   */
  Elems FetchAndClear() {
    Elems ret;
    ret.resize(entries_.size());
    int curr = 0;
    {  // Prevent coverage false positive
      MUTEX_LOCK_GUARD(data_mutex_);
      typename Entries::iterator it = entries_.begin();
      for (; it != entries_.end(); ++it)
        ret[curr++] = it->second;
      entries_.clear();
    }
    return ret;
  }

  /**
   * Whether the list is empty
   */
  bool IsEmpty() const {
    MUTEX_LOCK_GUARD(data_mutex_);
    return entries_.empty();
  }

 private:
  mutable MUTEX_TYPE data_mutex_;  // Protect everything below
  Entries entries_;  // Elements stored
};

}  // namespace cpfs
