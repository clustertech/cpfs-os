#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define interfaces for handling user and group IDs in meta-data
 * server.
 */

#include <sys/types.h>

#include <memory>

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ms {

/**
 * Guard to be held to maintain the result of IUgidHandler::SetFsIds()
 * calls.  Once the guard is destroyed, the effects are reverted.
 */
class IUgidSetterGuard {
 public:
  virtual ~IUgidSetterGuard() {}
};

/**
 * Handle user and group ids.
 */
class IUgidHandler {
 public:
  virtual ~IUgidHandler() {}

  /**
   * Set whether checks should be performed.
   */
  virtual void SetCheck(bool check) = 0;

  /**
   * Set the user and group ids for accessing the filesystem.  The
   * effect is only kept for as long as the returned object lifetime.
   *
   * @param uid The UID to access the FS
   *
   * @param gid The GID to access the FS
   */
  virtual UNIQUE_PTR<IUgidSetterGuard> SetFsIds(uid_t uid, gid_t gid) = 0;

  /**
   * Check whether a user id has a particular supplementary group.
   *
   * @param uid The UID to check
   *
   * @param gid The GID to check
   */
  virtual bool HasSupplementaryGroup(uid_t uid, gid_t gid) = 0;

  /**
   * Clean unusable cache entries.
   */
  virtual void Clean() = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
