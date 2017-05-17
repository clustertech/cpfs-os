#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::server::ms::IConnMgr for managing connections to
 * primary MS, FC and DS.
 */

#include "common.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimSocket;

namespace server {
namespace ms {

class BaseMetaServer;

/**
 * Interface for ConnMgr
 */
class IConnMgr {
 public:
  virtual ~IConnMgr() {}

  /**
   * Start connection to primary MS
   */
  virtual void Init() = 0;

  /**
   * Initialize connection with MS
   *
   * @param socket The connected socket
   *
   * @param peer_ha_counter The HA counter value of the peer
   *
   * @param peer_active Whether peer has been active before (thus
   * always win election)
   */
  virtual void InitMSConn(const boost::shared_ptr<IFimSocket>& socket,
                          uint64_t peer_ha_counter, bool peer_active) = 0;

  /**
   * Clean up an FC which was previously connected.
   *
   * @param socket The socket of the FC
   */
  virtual void CleanupFC(const boost::shared_ptr<IFimSocket>& socket) = 0;

  /**
   * Clean up a DS which was previously connected.
   *
   * @param group_id The group ID of the disconnected DS
   *
   * @param role_id The role ID of the disconnected DS
   */
  virtual void CleanupDS(GroupId group_id, GroupRole role_id) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
