#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::client::IConnMgr for initializing connections in FC.
 */

#include <stdint.h>

#include <string>
#include <vector>

#include "common.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimSocket;

namespace client {

/**
 * Interface for ConnMgr
 */
class IConnMgr {
 public:
  virtual ~IConnMgr() {}

  /**
   * Connect to both MS1 and MS2 (if available). Only active MS will be
   * connected eventually. The connection is dropped if peer is a stand-by MS.
   * DS group will be connected once registration to MS is completed.
   *
   * @param servers The host:ip of the meta server(s)
   */
  virtual void Init(const std::vector<std::string>& servers) = 0;

  /**
   * Forget an MS.  Useful when the MS is to be disconnected.  This
   * should not be called if ReconnectMS() is needed.
   *
   * @param socket The Fim socket of the MS
   *
   * @param rejected Whether the connection is rejected
   */
  virtual void ForgetMS(boost::shared_ptr<IFimSocket> socket,
                        bool rejected) = 0;
  /**
   * Get rejected MS info
   *
   * @return Vector info of whether MS is rejected
   */
  virtual std::vector<bool> GetMSRejectInfo() const = 0;

  /**
   * Reconnect on MS disconnection.
   *
   * @param socket The Fim socket of the MS disconnected
   */
  virtual void ReconnectMS(boost::shared_ptr<IFimSocket> socket) = 0;

  /**
   * Whether reconnection to MS is in progress
   */
  virtual bool IsReconnectingMS() const = 0;

  /**
   * Set whether reconnection to MS is in progress
   *
   * @param reconnecting Whether reconnection is in progress
   */
  virtual void SetReconnectingMS(bool reconnecting) = 0;

  /**
   * Set whether MS should be started forcibly
   *
   * @param server_index The meta server index
   */
  virtual void SetForceStartMS(unsigned server_index) = 0;

  /**
   * Set max init connection retry
   *
   * @param init_conn_retry The init conn retry count
   */
  virtual void SetInitConnRetry(unsigned init_conn_retry) = 0;

  /**
   * Connect to a DS.
   *
   * @param ip The IP to connect to
   *
   * @param port The port to connect to
   *
   * @param group The group id of the DS being connected to
   *
   * @param role The group role of the DS being connected to
   */
  virtual void ConnectDS(uint32_t ip, uint16_t port,
                         GroupId group, GroupRole role) = 0;
};

}  // namespace client
}  // namespace cpfs
