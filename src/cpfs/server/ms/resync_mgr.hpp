#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the Resync Manager
 */
#include <stdint.h>

#include <vector>

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;

/**
 * Interface for Resync Manager.
 */
class IResyncMgr {
 public:
  virtual ~IResyncMgr() {}

  /**
   * Send a MS resync request to active MS
   */
  virtual void RequestResync() = 0;

  /**
   * Send all resync messages to the previously failed peer.  New
   * thread is spawned to resync the whole meta directory content and
   * in memory states, for example, opened and pending unlink inodes
   * etc.
   *
   * @param optimized Whether to use optimized resync
   *
   * @param removed Inodes removed before resync
   */
  virtual void SendAllResync(bool optimized,
                             const std::vector<InodeNum>& removed) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
