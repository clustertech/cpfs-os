#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ITrackerMapper interface.
 */

#include <boost/intrusive_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"

namespace cpfs {

class IFim;
class IFimSocket;
class IReqTracker;

/**
 * A structure to find request trackers for communicating with
 * particular host.  The connection establishment functions in CPFS
 * will create an object implementing the interface, so that the other
 * parts of the system can use it to find request trackers for
 * communication with particular host.
 */
class ITrackerMapper {
 public:
  virtual ~ITrackerMapper() {}

  /**
   * Clean up all data structures.  It is legal to call only if the
   * calling thread does not hold any mutex.
   */
  virtual void Reset() = 0;

  /**
   * Set the client number of the caller.  This should be set before
   * other functions of the tracker mapper is called.
   *
   * @param client_num The client number
   */
  virtual void SetClientNum(ClientNum client_num) = 0;

  /**
   * Set the max_send to use when creating ReqLimiter's in
   * ReqTracker's created previously or in the future.  This only
   * affects ReqLimiter's created in the future, already created
   * ReqLimiter's are not affected.
   *
   * @param limiter_max_send The max_send to use
   */
  virtual void SetLimiterMaxSend(uint64_t limiter_max_send) = 0;

  /**
   * Get the request tracker for the MS.  The request tracker returned
   * is persistent.
   */
  virtual boost::shared_ptr<IReqTracker> GetMSTracker() = 0;

  /**
   * @return The Fim socket used for the MS request tracker.  A null
   * return value means no MS request tracker is ready for file data
   * communication.
   */
  virtual boost::shared_ptr<IFimSocket> GetMSFimSocket() = 0;

  /**
   * Set the Fim socket to be used for MS request tracker.
   *
   * @param fim_socket The Fim socket used for the MS request tracker.
   * The socket should be ready for communication.  A null value means
   * we have lost connection to the server.
   *
   * @param plug Whether to automatically do Plug(), in case
   * fim_socket is not empty.  If fim_socket is empty it is always
   * unplugged
   */
  virtual void SetMSFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                              bool plug = true) = 0;

  /**
   * Get the request tracker for a DS.  The request tracker returned
   * is persistent.
   *
   * @param group_id The group id of the DS.
   *
   * @param role_id The role id of the DS.
   */
  virtual boost::shared_ptr<IReqTracker> GetDSTracker(
      GroupId group_id, GroupRole role_id) = 0;

  /**
   * @param group_id The group id of the DS.
   *
   * @param role_id The role id of the DS.
   *
   * @return The Fim socket used for the DS request tracker.  A null
   * return value means no DS request tracker is ready for file data
   * communication for the group and role id.
   */
  virtual boost::shared_ptr<IFimSocket> GetDSFimSocket(GroupId group_id,
                                                       GroupRole role_id) = 0;

  /**
   * Set the Fim socket to be used for a DS request tracker.
   *
   * @param fim_socket The Fim socket used for the DS request tracker.
   * The socket should be ready for communication.  A null value means
   * we have lost connection to the server.
   *
   * @param group_id The group id of the DS.
   *
   * @param role_id The role id of the DS.
   *
   * @param plug Whether to automatically do Plug(), in case
   * fim_socket is not empty.  If fim_socket is empty it is always
   * unplugged
   */
  virtual void SetDSFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                              GroupId group_id, GroupRole role_id,
                              bool plug = true) = 0;

  /**
   * Find the DS group and role ids of a FimSocket.
   *
   * @param fim_socket The Fim socket to find DS group and role ids
   *
   * @param group_id_ret Where to put the found group id
   *
   * @param role_id_ret Where to put the found group role
   *
   * @return Whether the FimSocket is found is the DS socket list
   */
  virtual bool FindDSRole(boost::shared_ptr<IFimSocket> fim_socket,
                          GroupId* group_id_ret, GroupRole* role_id_ret) = 0;

  /**
   * Get the request tracker for a FC.  The request tracker returned
   * is non-persistent.
   *
   * @param client_num The client number of the FC.
   */
  virtual boost::shared_ptr<IReqTracker> GetFCTracker(ClientNum client_num) = 0;

  /**
   * @param client_num The client number of the FC.
   *
   * @return The Fim socket used for the FC request tracker.  A null
   * return value means no FC request tracker is ready for file data
   * communication for the client number.
   */
  virtual boost::shared_ptr<IFimSocket> GetFCFimSocket(
      ClientNum client_num) = 0;

  /**
   * Set the Fim socket to be used for a FC request tracker.
   *
   * @param fim_socket The Fim socket used for the DS request tracker.
   * The socket should be ready for communication.  A null value means
   * we have lost connection to the server.
   *
   * @param client_num The client number of the FC.
   *
   * @param plug Whether to automatically do Plug(), in case
   * fim_socket is not empty.  If fim_socket is empty it is always
   * unplugged
   */
  virtual void SetFCFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                              ClientNum client_num, bool plug = true) = 0;

  /**
   * Broadcast a Fim to all connected DS in a DS group, perhaps except
   * one particular DS.
   *
   * @param group The DS group to broadcast the Fim to
   *
   * @param fim The Fim to broadcast
   *
   * @param except The DS role to skip the broadcast
   */
  virtual void DSGBroadcast(GroupId group, FIM_PTR<IFim> fim,
                            GroupRole except = kNumDSPerGroup) = 0;

  /**
   * Broadcast a Fim to all connected FCs.
   *
   * @param fim The Fim to broadcast
   */
  virtual void FCBroadcast(FIM_PTR<IFim> fim) = 0;

  /**
   * Plug or unplug all trackers
   *
   * @param plug Whether to send requests on AddRequest()
   */
  virtual void Plug(bool plug) = 0;

  /**
   * Check whether there is tracker with pending fims to write
   *
   * @return True if there exists at least one tracker with pending fims
   */
  virtual bool HasPendingWrite() const = 0;

  /**
   * Dump all request trackers.
   */
  virtual void DumpPendingRequests() const = 0;

  /**
   * Shutdown all trackers
   */
  virtual void Shutdown() = 0;

  /**
   * Get the request tracker for a Admin.  The request tracker returned
   * is non-persistent.
   *
   * @param client_num The client ID.
   */
  virtual boost::shared_ptr<IReqTracker> GetAdminTracker(
      ClientNum client_num) = 0;

  /**
   * @param client_num The client ID.
   *
   * @return The Fim socket used for the Admin request tracker.  A null
   * return value means no Admin request tracker is ready.
   */
  virtual boost::shared_ptr<IFimSocket> GetAdminFimSocket(
      ClientNum client_num) = 0;

  /**
   * Set the Fim socket to be used for a Admin request tracker.
   *
   * @param fim_socket The Fim socket used for the request tracker.
   *
   * @param client_num The client ID.
   *
   * @param plug Whether to automatically do Plug(), in case
   * fim_socket is not empty.  If fim_socket is empty it is always
   * unplugged
   */
  virtual void SetAdminFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                                 ClientNum client_num, bool plug = true) = 0;
};

}  // namespace cpfs
