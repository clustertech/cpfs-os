#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the interface of a TopologyMgr managing the topology
 * information of the system.
 */

#include <stdint.h>

#include <string>
#include <vector>

#include "admin_info.hpp"
#include "common.hpp"
#include "dsg_state.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimSocket;

namespace server {
namespace ms {

class BaseMetaServer;

/**
 * Interface for topology maker.  The class is mostly concerned with
 * internal accounting of the topology information.  The users of the
 * class is expected to have made basic validation for the other data
 * structures in the server, e.g., the tracker mapper.
 */
class ITopologyMgr {
 public:
  virtual ~ITopologyMgr() {}

  /**
   * Initialize the topology manager.
   */
  virtual void Init() = 0;

  /**
   * @return The number of DS groups
   */
  virtual GroupId num_groups() const = 0;

  /**
   * @param num_groups The maximum number of DS groups
   */
  virtual void set_num_groups(GroupId num_groups) = 0;

  /**
   * Return expectation of presence of DS.
   *
   * @param group The group ID
   *
   * @param role The group role
   *
   * @return Whether the topology manager expects a DS to be present
   */
  virtual bool HasDS(GroupId group, GroupRole role) const = 0;

  /**
   * Get the current DS group state of a group.
   *
   * @param group The DS group ID
   *
   * @param failed_ret Where to put the failed role in case state is
   * kDSGDegraded
   *
   * @return The DS group state
   */
  virtual DSGroupState GetDSGState(GroupId group, GroupRole* failed_ret) = 0;

  /**
   * Set the DS group state.
   *
   * @param group The DS group ID
   *
   * @param failed Failed role in case state is kDSGDegraded
   *
   * @param state The DS group state
   *
   * @param state_change_id The state change id
   */
  virtual void SetDSGState(GroupId group, GroupRole failed,
                           DSGroupState state, uint64_t state_change_id) = 0;

  /**
   * Mark a DS as lost.  This only has an effect if the DS is still
   * open.  A DSG with one lost DS will be started in kDSGDegraded
   * state rather than kDSGReady state.
   *
   * @param group The DS group ID
   *
   * @param role The DS group role
   */
  virtual void SetDSLost(GroupId group, GroupRole role) = 0;

  /**
   * Add a DS into the topology.  The user is expected to have already
   * checked that the DS role is not already occupied in the tracker
   * mapper.
   *
   * @param group The DS group ID
   *
   * @param role The DS group role
   *
   * @param info The node info for other DSs to contact the DS
   *
   * @param opt_resync Whether the DS has desire to use optimized
   * resync, needed only on the active MS
   *
   * @param state_changed_ret Return whether DSG state is changed.
   * Modified only if the operation is successful
   *
   * @return Whether the add operation is successful
   */
  virtual bool AddDS(GroupId group, GroupRole role, NodeInfo info,
                     bool opt_resync, bool* state_changed_ret) = 0;

  /**
   * Remove a DS from the topology
   *
   * @param group The DS group ID
   *
   * @param state_changed_ret Return whether DSG state is changed
   *
   * @param role The DS group role
   */
  virtual void RemoveDS(GroupId group, GroupRole role,
                        bool* state_changed_ret) = 0;

  /**
   * Handle completion of DS recovery.
   *
   * @param group The DS group of the DS which has recovery completed
   *
   * @param role The DS group role of the DS which has recovery
   * completed
   *
   * @param end_type Either 0 (dir-only) or 1 (full)
   *
   * @return Whether state is changed as a result
   */
  virtual bool DSRecovered(GroupId group, GroupRole role, char end_type) = 0;

  /**
   * Check whether the specified DS group is ready to use
   *
   * @param group The DS group to check
   *
   * @return Whether the DS group is ready to use
   */
  virtual bool DSGReady(GroupId group) const = 0;

  /**
   * Check whether the system and all DSs are ready.  It returns true
   * only if all DSGs are either active or degraded, and all expected
   * DS are connected.
   *
   * @return Whether the system is ready
   */
  virtual bool AllDSReady() = 0;

  /**
   * Suggest a group ID and role for a new DS to use.
   *
   * @param group_ret Where to put the suggested DS group ID
   *
   * @param role_ret Where to put the suggested DS group role
   *
   * @return Whether a role is found
   */
  virtual bool SuggestDSRole(GroupId* group_ret, GroupRole* role_ret) = 0;

  /**
   * Send all DS information to a client.
   *
   * @param peer The peer to receive the DS information.
   */
  virtual void SendAllDSInfo(boost::shared_ptr<IFimSocket> peer) = 0;

  /**
   * Announce a DS change to other servers and clients as required.
   *
   * @param group The DS group ID
   *
   * @param role The DS group role
   *
   * @param is_added Whether the DS is added to the topology
   *
   * @param state_changed Whether the DSG state is changed
   */
  virtual void AnnounceDS(GroupId group, GroupRole role, bool is_added,
                          bool state_changed) = 0;

  /**
   * Announce current change of a DS group.
   *
   * @param group The group to announce
   */
  virtual void AnnounceDSGState(GroupId group) = 0;

  /**
   * Handle acknowledgement of reception of state change Fim.
   *
   * @param state_change_id The state change id handled
   *
   * @param peer The peer sending the state change acknowledgement
   *
   * @param group_ret The group acked
   *
   * @param state_ret The state acked
   *
   * @return Whether all DS in the group has ack'ed the change
   */
  virtual bool AckDSGStateChange(uint64_t state_change_id,
                                 boost::shared_ptr<IFimSocket> peer,
                                 GroupId* group_ret,
                                 DSGroupState* state_ret) = 0;

  /**
   * @return whether all DSG can be started (possibly degraded).
   */
  virtual bool AllDSGStartable() const = 0;

  /**
   * Force all DSG to start.
   *
   * @return Whether all DSG are ready now
   */
  virtual bool ForceStartDSG() = 0;

  /**
   * @return The list of FCs known to the topology manager
   */
  virtual std::vector<ClientNum> GetFCs() const = 0;

  /**
   * Get number of FC added to the topology
   */
  virtual unsigned GetNumFCs() const = 0;

  /**
   * Add an FC to the topology.  The user is expected to have already
   * checked that the FC id is not already occupied in the tracker
   * mapper.
   *
   * @param fc_id The FC id
   *
   * @param info The FC node info
   *
   * @return Whether the add operation is successful
   */
  virtual bool AddFC(ClientNum fc_id, NodeInfo info) = 0;

  /**
   * Remove a FC from the topology.
   *
   * @param fc_id The FC id
   */
  virtual void RemoveFC(ClientNum fc_id) = 0;

  /**
   * Notify the topology that the FC is about to be killed
   *
   * @param fc_id The FC id
   */
  virtual void SetFCTerminating(ClientNum fc_id) = 0;

  /**
   * Suggest an FC id to be used by a client.
   *
   * @param fc_id_ret Where to put the suggested FC id
   *
   * @return Whether a FC id is available
   */
  virtual bool SuggestFCId(ClientNum* fc_id_ret) = 0;

  /**
   * Set the next FC id to try.  The topology manager try FC ids in
   * cyclic order, and return the first available FC id to clients.
   */
  virtual void SetNextFCId(ClientNum fc_id) = 0;

  /**
   * Announce a FC change to the peer meta server
   *
   * @param fc_id The FC id
   *
   * @param is_added Whether the FC is added to the topology
   */
  virtual void AnnounceFC(ClientNum fc_id, bool is_added) = 0;

  /**
   * Send all FC information to the peer MS server
   */
  virtual void SendAllFCInfo() = 0;

  /**
   * Announce shutdown to connected FC, DS and MS
   */
  virtual void AnnounceShutdown() = 0;

  /**
   * Announce halt to connected FC, DS and MS. The system should be fully off.
   */
  virtual void AnnounceHalt() = 0;

  /**
   * Handle acknowledgement of shutdown broadcast from DS
   *
   * @param peer The peer DS sending shutdown acknowledgement
   *
   * @return Whether all DS in DS groups have ack'ed the shutdown
   */
  virtual bool AckDSShutdown(boost::shared_ptr<IFimSocket> peer) = 0;

  // TODO(Isaac): Don't use StartStopWorker for stopping workers.  Use
  // a defer manager instead.
  /**
   * Start or stop the meta worker to reflect the current state.  To
   * avoid deadlock, this wait for at most 2 seconds.
   */
  virtual void StartStopWorker() = 0;

  /**
   * Set the distressed status of the DS group
   *
   * @param group The group id
   *
   * @param distressed Whether the DS group is distressed
   */
  virtual void SetDSGDistressed(GroupId group, bool distressed) = 0;

  /**
   * List of DS info for all DS groups
   */
  virtual std::vector<NodeInfo> GetDSInfos() = 0;

  /**
   * List of FC info for all connected FC
   */
  virtual std::vector<NodeInfo> GetFCInfos() = 0;

  /**
   * Add the peer MS into the topology.
   *
   * @param info The node info of the peer MS
   *
   * @param new_node Whether the peer MS is newly created
   */
  virtual bool AddMS(NodeInfo info, bool new_node) = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
