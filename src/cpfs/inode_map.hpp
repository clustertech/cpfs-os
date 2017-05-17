#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define a facility of a mapping from inode to some other structure.
 * It allows users to safely remove an entry after checking that it is
 * no longer used.  Internally, a Boost unordered map is used to store
 * the information.
 */

#include <boost/make_shared.hpp>  // IWYU pragma: keep
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "mutex_util.hpp"

namespace cpfs {

/**
 * A map which map inodes to a shared_ptr of type TElt.  The type must
 * be default-constructable.  Users must subclass the type to provide
 * an implementation of IsUnused().
 *
 * @tparam TElt The element type
 */
template <typename TElt>
class InodeMap {
 protected:
  /**
   * The actual type used for storing the data.
   */
  typedef boost::unordered_map<InodeNum, boost::shared_ptr<TElt> > MapType;

 public:
  InodeMap() : data_mutex_(MUTEX_INIT) {}
  virtual ~InodeMap() {}

  /**
   * Check whether an element exists.
   *
   * @param inode The inode number of the element
   */
  bool HasInode(InodeNum inode) const {
    MUTEX_LOCK_GUARD(data_mutex_);
    return data_.find(inode) != data_.end();
  }

  /**
   * Get an element.  If the inode has not been in the map, a null
   * shared pointer is returned.
   *
   * @param inode The inode for which the element is get
   */
  boost::shared_ptr<TElt> Get(InodeNum inode) const {
    MUTEX_LOCK_GUARD(data_mutex_);
    typename MapType::const_iterator it = data_.find(inode);
    if (it != data_.end())
      return it->second;
    return boost::shared_ptr<TElt>();
  }

  /**
   * Get an element.  If the element has not been in the map, it is
   * default-constructed.
   *
   * @param inode The inode for which the element is get
   */
  boost::shared_ptr<TElt> operator[](InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    typename MapType::const_iterator it = data_.find(inode);
    if (it != data_.end())
      return it->second;
    return data_[inode] = boost::make_shared<TElt>();
  }

  /**
   * Safely remove the entry for an inode if it is no longer used.
   *
   * @param inode The inode to clean
   */
  void Clean(InodeNum inode) {
    MUTEX_LOCK_GUARD(data_mutex_);
    typename MapType::const_iterator it = data_.find(inode);
    if (it == data_.end())
      return;
    if (!it->second.unique())  // Someone else may make it used
      return;
    if (IsUnused(it->second))
      data_.erase(inode);
  }

 protected:
  mutable MUTEX_TYPE data_mutex_; /**< Protect data below */
  MapType data_; /**< The data stored */

  /**
   * Provided by subclass, to check whether the element is currently
   * used (i.e., may be cleaned up).
   *
   * @param elt The element to check
   *
   * @return Whether the element is considered unused
   */
  virtual bool IsUnused(const boost::shared_ptr<TElt>& elt) const = 0;
};

}  // namespace cpfs
