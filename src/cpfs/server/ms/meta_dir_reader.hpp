#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the IMetaDirReader interface.
 */
#include <string>

#include <boost/intrusive_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "fim.hpp"
#include "fims.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Interface for reading meta-server data directory.
 */
class IMetaDirReader {
 public:
  virtual ~IMetaDirReader() {}

  /**
   * Create a fim for recreating the inode specified
   *
   * @param prefixed_inode_s Inode number prefixed with 1st dir
   */
  virtual FIM_PTR<ResyncInodeFim> ToInodeFim(
      const std::string& prefixed_inode_s) = 0;

  /**
   * Create a fim for recreating the dentry specified
   *
   * @param prefixed_parent_s Parent inode of the dentry prefixed with 1st dir
   *
   * @param name Dentry name
   *
   * @param type Type of the dentry
   */
  virtual FIM_PTR<ResyncDentryFim> ToDentryFim(
      const std::string& prefixed_parent_s,
      const std::string& name,
      char type) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
