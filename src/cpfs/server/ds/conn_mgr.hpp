#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines cpfs::server::ds::IConnMgr for initializing connections to
 * MS and DSx for DS.
 */

#include <stdint.h>

#include "common.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimProcessor;
class IFimSocket;

namespace server {
namespace ds {

class BaseDataServer;

/**
 * Interface for ConnMgr
 */
class IConnMgr {
 public:
  /**
   * Destructor
  */
  virtual ~IConnMgr() {}

  /**
   * Start connection to MS
   */
  virtual void Init() = 0;

  /**
   * Disconnect an MS.  Useful when the MS connection is rejected.
   *
   * @param socket The Fim socket of the MS
   */
  virtual void DisconnectMS(boost::shared_ptr<IFimSocket> socket) = 0;

  /**
   * Start MS reconnection.  Useful when the MS connection failed.
   *
   * @param socket The Fim socket of the failed MS
   */
  virtual void ReconnectMS(boost::shared_ptr<IFimSocket> socket) = 0;

  /**
   * Connect to a DS.
   *
   * @param ip The IP address of the DS
   *
   * @param port The port number of the DS
   *
   * @param group The group ID of the DS
   *
   * @param role The group role of the DS
   */
  virtual void ConnectDS(uint32_t ip, uint16_t port,
                         GroupId group, GroupRole role) = 0;
};

}  // namespace ds
}  // namespace server
}  // namespace cpfs
