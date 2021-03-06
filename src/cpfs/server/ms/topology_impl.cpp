/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * implementation of the interface of a TopologyMgr managing the
 * topology information of the system.
 */

#include "server/ms/topology_impl.hpp"

#include <stdint.h>

#include <cstring>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>

#include "admin_info.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/stat_keeper.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/store.hpp"
#include "server/ms/topology.hpp"
#include "server/server_info.hpp"
#include "server/thread_group.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Low-water of free space available in DS.  If one DS has free space
 * below it, its DSG is switched to distress mode.  This is hard-coded
 * to 20G.
 */
const uint64_t kFreeSpaceLowWater = uint64_t(20) * 1024 * 1024 * 1024;
/**
 * High-water of free space available in DS.  If all DSs in a DSG has
 * free space above it, its DSG is switched out of distress mode.
 * This is hard-coded to 40G.
 */
const uint64_t kFreeSpaceHighWater = 2 * kFreeSpaceLowWater;

/**
 * The information of one server group.
 */
struct DSGroup {
  GroupId group_id; /**< The group ID of this group */
  DSGroupState state; /**< The current state of the group */
  GroupRole failed; /**< The role of the failed DS if state == kDSGDegraded */
  uint64_t state_change_id; /**< Number of state changes so far */
  bool creating; /**< Whether the group creation is in progress */
  bool distress; /**< Whether the group is in distress mode */
  std::vector<bool> shutting_down; /**< Whether the role is shutting down */
  std::vector<bool> opt_resync; /**< Whether to optimize resync */
  /**
   * The ports to contact each DS.  The IP address has special
   * meaning: if non-zero, the DS role has been allocated before.
   * If the port is 0, it means the DS has been removed.  Such DS
   * role will be the last ones to be suggested by SuggestDSRole().
   */
  NodeInfo ds_infos[kNumDSPerGroup];

  DSGroup() : state(kDSGPending), failed(0), state_change_id(0),
              creating(false), distress(false),
              shutting_down(kNumDSPerGroup, false),
              opt_resync(kNumDSPerGroup, false) {}

  /**
   * Update the current state after a topology change event.
   */
  void UpdateState() {
    DoUpdateState(false);
  }

  /**
   * Set the DSG to be recovered.
   *
   * @param role The role triggering it
   *
   * @param end_type Either 0 (dir-only) or 1 (full)
   *
   * @return Whether the DSG is switched to ready
   */
  bool SetRecovered(GroupRole role, char end_type) {
    if (state == kDSGRecovering && end_type == 0) {
      state = kDSGResync;
    } else if (state == kDSGResync && end_type == 1) {
      state = kDSGReady;
      failed = kNumDSPerGroup;
    } else {
      return false;
    }
    ++state_change_id;
    return true;
  }

  /**
   * Set the DSG to be shutting down
   *
   * @param role The role replying shutdown ready
   */
  void SetShuttingDown(GroupRole role) {
    shutting_down[role] = true;
  }

  /**
   * Check whether all running DS in the DSG are shutting down
   */
  bool IsShuttingDown() const {
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      if (ds_infos[r].ip != 0 &&  ds_infos[r].port != 0 && !shutting_down[r])
        return false;
    }
    return true;
  }

  /**
   * Check whether the DSG is startable.
   */
  bool Startable() const {
    GroupRole num_missing = 0;
    if (state == kDSGFailed || state == kDSGShuttingDown)
      return false;
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
      if (ds_infos[r].port == 0)
        ++num_missing;
    return num_missing <= 1;
  }

  /**
   * Try to force start the group.
   *
   * @return Whether force start occurred (false implies that the
   * group has previously been started already)
   */
  bool ForceStart() {
    if (state != kDSGPending)
      return false;
    return DoUpdateState(true);
  }

 private:
  bool DoUpdateState(bool lenient) {
    GroupRole new_failed;
    DSGroupState new_state = NextState(lenient, &new_failed);
    if (state != new_state || (state == kDSGDegraded && failed != new_failed)) {
      state = new_state;
      if (state != kDSGRecovering)
        failed = new_failed;
      ++state_change_id;
      return true;
    }
    return false;
  }

  DSGroupState NextState(bool lenient_open, GroupRole* failed_ret) {
    int num_open = 0, num_missing = 0;
    *failed_ret = kNumDSPerGroup;
    if (state == kDSGShuttingDown)
      return state;  // Don't move out of kDSGShuttingDown
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      if (ds_infos[r].ip == 0) {
        ++num_open;
      } else if (ds_infos[r].port == 0) {
        *failed_ret = r;
        ++num_missing;
      }
    }
    if (state == kDSGFailed || num_missing > 1)
      return kDSGFailed;
    if (num_open > 1 || (!lenient_open && num_open > 0))
      return kDSGPending;
    if (num_missing == 0)
      return state == kDSGPending || state == kDSGReady
          ? kDSGReady : kDSGRecovering;
    // num_missing == 1
    return state == kDSGReady || *failed_ret == failed ?
        kDSGDegraded : kDSGFailed;
  }
};

/**
 * Record DSG state changes pending announcement
 */
struct DSGStateChange {
  uint64_t id;  /**< Change pending for FC announcement */
  std::set<GroupRole> acked_ds;  /**< DS acknowledged state change */
  DSGroupState state; /**< The new state */
  GroupRole except; /**< The role which needs not be sent announcement */
  bool ds_notified;  /**< The change has been sent to DSs */
};

/**
 * Implement the ITopologyMgr interface.
 */
class TopologyMgr : public ITopologyMgr {
 public:
  /**
   * @param server The meta server that the connector operate on
   */
  explicit TopologyMgr(BaseMetaServer* server);
  void Init();
  GroupId num_groups() const;
  void set_num_groups(GroupId num_groups);
  bool HasDS(GroupId group, GroupRole role) const;
  DSGroupState GetDSGState(GroupId group, GroupRole* failed_ret);
  void SetDSGState(GroupId group, GroupRole failed,
                   DSGroupState state, uint64_t state_change_id);
  void SetDSLost(GroupId group, GroupRole role);
  bool AddDS(GroupId group, GroupRole role, NodeInfo info,
             bool opt_resync, bool* state_changed_ret);
  void RemoveDS(GroupId group, GroupRole role, bool* state_changed_ret);
  bool DSRecovered(GroupId group, GroupRole role, char end_type);
  bool DSGReady(GroupId group) const;
  bool AllDSReady();
  bool SuggestDSRole(GroupId* group_ret, GroupRole* role_ret);
  void SendAllDSInfo(boost::shared_ptr<IFimSocket> peer);
  void AnnounceDS(GroupId group, GroupRole role, bool is_added,
                  bool state_changed);
  void AnnounceDSGState(GroupId group);
  bool IsDSGStateChanging();
  void AckDSGStateChangeWait(ClientNum client);
  bool AckDSGStateChange(uint64_t state_change_id,
                         boost::shared_ptr<IFimSocket> peer,
                         GroupId* group_ret,
                         DSGroupState* state_ret);
  bool AllDSGStartable() const;
  bool ForceStartDSG();
  bool AckDSShutdown(boost::shared_ptr<IFimSocket> peer);
  std::vector<ClientNum> GetFCs() const;
  unsigned GetNumFCs() const;
  bool AddFC(ClientNum fc_id, NodeInfo info);
  void RemoveFC(ClientNum fc_id);
  void SetFCTerminating(ClientNum fc_id);
  bool SuggestFCId(ClientNum* fc_id_ret);
  void SetNextFCId(ClientNum fc_id);
  void AnnounceFC(ClientNum fc_id, bool is_added);
  void SendAllFCInfo();
  void StartStopWorker();
  void AnnounceShutdown();
  void AnnounceHalt();
  void SetDSGDistressed(GroupId group, bool distressed);
  bool AddMS(NodeInfo info, bool new_node);
  std::vector<NodeInfo> GetFCInfos();
  std::vector<NodeInfo> GetDSInfos();

 private:
  BaseMetaServer* server_;

  mutable MUTEX_TYPE data_mutex_; /**< Protect fields below */
  std::vector<DSGroup> ds_groups_; /**< The groups */
  typedef std::map<ClientNum, NodeInfo> FCInfoMap;
  FCInfoMap fc_infos_; /**< The FC IP addresses */
  ClientNum next_fc_id_; /**< The next FC id to allocate */
  /** Whether DSG state change is in progress */
  boost::atomic<bool> dsc_in_progress_;
  /** FCs to reply before DSG state change */
  std::set<ClientNum> dsc_pending_fcs_;
  /** State changes awaiting completion */
  typedef std::map<GroupId, DSGStateChange> DSGStateChangeMap;
  DSGStateChangeMap pending_dscs_;
  GroupId max_group_id_; /**< The max ID of the group created successfully */
  /** Map the role (e.g. MS1, DS 0-1) to UUID */
  std::map<std::string, std::string> node_uuids_;

  DSGroupState GetDSGState_(GroupId group, GroupRole* failed_ret) {
    ValidateDSGroupRole_(group, 0);
    *failed_ret = ds_groups_[group].failed;
    return ds_groups_[group].state;
  }

  void ValidateDSGroupRole_(GroupId group, GroupRole role) {
    if (group >= ds_groups_.size() || role >= kNumDSPerGroup)
      throw std::invalid_argument(
          (boost::format("No such role: %d-%d") % group % role).str());
  }

  FIM_PTR<DSGStateChangeFim> StateChangeFim_(GroupId group) {
    FIM_PTR<DSGStateChangeFim> ret = DSGStateChangeFim::MakePtr();
    (*ret)->ds_group = group;
    DSGroup& dsg = ds_groups_[group];
    (*ret)->state_change_id = dsg.state_change_id;
    (*ret)->failed = dsg.failed;
    (*ret)->state = dsg.state;
    (*ret)->ready = AllDSReady_() ? 1 : 0;
    (*ret)->opt_resync = (dsg.state == kDSGRecovering &&
                          dsg.opt_resync[dsg.failed]);
    (*ret)->distress = dsg.distress ? 1 : 0;
    return ret;
  }

  void Replicate_(const FIM_PTR<IFim>& fim) {
    boost::shared_ptr<IFimSocket> ms_fim_socket =
        server_->tracker_mapper()->GetMSFimSocket();
    if (ms_fim_socket)
      ms_fim_socket->WriteMsg(fim);
  }

  bool AllDSGStartable_() const {
    for (GroupId g = 0; g < ds_groups_.size(); ++g)
      if (!ds_groups_[g].Startable())
        return false;
    return true;
  }

  bool AllDSReady_();
  void AnnounceDSGState_(GroupId group, GroupRole except = kNumDSPerGroup);
  void SetFCsDSCFrozen_(bool enable);
  void NotifyDSDSCs_();
  bool MSActive_();
  void HandleNewStat(const AllDSSpaceStat& stat);
  void AddUUID_(const std::string& uuid, const std::string& role);
  bool CheckUUID_(const std::string& uuid, const std::string& role);
};

TopologyMgr::TopologyMgr(BaseMetaServer* server)
    : server_(server), data_mutex_(MUTEX_INIT),
      next_fc_id_(0), dsc_in_progress_(false), max_group_id_(0) {}

void TopologyMgr::Init() {
  max_group_id_ = boost::lexical_cast<GroupId>(
      server_->server_info()->Get("max-dsg-id", "0"));
  GroupId num_groups =
      boost::lexical_cast<int>(server_->server_info()->Get("max-ndsg", "1"));
  ds_groups_.resize(num_groups);
  for (GroupId g = 0; g < num_groups; ++g)
    ds_groups_[g].group_id = g;
  server_->stat_keeper()->OnAllStat(
      boost::bind(&TopologyMgr::HandleNewStat, this, _1));
  node_uuids_ = server_->store()->LoadAllUUID();
  AddUUID_(server_->store()->GetUUID(), server_->configs().role());
}

void TopologyMgr::HandleNewStat(const AllDSSpaceStat& stat) {
  for (GroupId g = 0; g < stat.size(); ++g) {
    bool distress_low = false;
    bool distress_high = false;
    for (GroupRole r = 0; r < stat[g].size(); ++r) {
      if (!stat[g][r].online)
        continue;
      if (stat[g][r].free_space < kFreeSpaceLowWater) {
        distress_low = true;
        break;
      }
      if (stat[g][r].free_space < kFreeSpaceHighWater) {
        distress_high = true;
      }
    }
    if (distress_low)
      SetDSGDistressed(g, true);
    else if (!distress_high)
      SetDSGDistressed(g, false);
  }
}

void TopologyMgr::SetDSGDistressed(GroupId group, bool distressed) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (ds_groups_[group].distress != distressed) {
    if (distressed)
      LOG(informational, Server, "DSG ", PINT(group), " is distressed");
    else
      LOG(informational, Server,
          "DSG ", PINT(group), " is no longer distressed");
    ds_groups_[group].distress = distressed;
    FIM_PTR<DSGDistressModeChangeFim> fim =
        DSGDistressModeChangeFim::MakePtr();
    (*fim)->distress = ds_groups_[group].distress;
    server_->tracker_mapper()->DSGBroadcast(group, fim, kNumDSPerGroup);
  }
}

GroupId TopologyMgr::num_groups() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return ds_groups_.size();
}

void TopologyMgr::set_num_groups(GroupId ngroups) {
  GroupId old_ngroups = num_groups();
  int32_t ngroups_diff = ngroups - old_ngroups;
  if (ngroups_diff == 1) {
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      ds_groups_.resize(ngroups);
      GroupId new_group = ngroups - 1;
      ds_groups_[new_group].group_id = new_group;
      ds_groups_[new_group].creating = true;
    }
  } else if (ngroups_diff == -1 && (ngroups - 1) == max_group_id_) {
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      ds_groups_.resize(ngroups);
    }
    if (AllDSReady_()) {
      server_->stat_keeper()->Run();
      StartStopWorker();
      FIM_PTR<SysStateNotifyFim> fim = SysStateNotifyFim::MakePtr();
      (*fim)->type = 'M';
      (*fim)->ready = true;
      server_->tracker_mapper()->FCBroadcast(fim);
    }
  } else {
    LOG(error, Server, "Cannot set number of DS groups to ", PINT(ngroups));
    return;
  }
  boost::shared_ptr<IFimSocket> ms_fim_socket =
      server_->tracker_mapper()->GetMSFimSocket();
  if (server_->state_mgr()->GetState() == kStateActive && ms_fim_socket) {
    FIM_PTR<DSGResizeFim> dsg_fim = DSGResizeFim::MakePtr();
    (*dsg_fim)->num_groups = ngroups;
    ms_fim_socket->WriteMsg(dsg_fim);
  }
  server_->server_info()->Set("max-ndsg",
                              boost::lexical_cast<std::string>(ngroups));
  LOG(notice, Server, "The max number of DS groups is now ", PINT(ngroups));
}

bool TopologyMgr::HasDS(GroupId group, GroupRole role) const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return ds_groups_[group].ds_infos[role].port != 0;
}

DSGroupState TopologyMgr::GetDSGState(GroupId group, GroupRole* failed_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetDSGState_(group, failed_ret);
}

void TopologyMgr::SetDSGState(GroupId group, GroupRole failed,
                              DSGroupState state, uint64_t state_change_id) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ValidateDSGroupRole_(group, 0);
  ds_groups_[group].failed = failed;
  ds_groups_[group].state = state;
  ds_groups_[group].state_change_id = state_change_id;
}

void TopologyMgr::SetDSLost(GroupId group, GroupRole role) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (ds_groups_[group].ds_infos[role].ip == 0) {
    LOG(notice, Server, "Setting DS ", GroupRoleName(group, role),
        " as previously lost");
    ds_groups_[group].failed = role;
    ds_groups_[group].ds_infos[role].ip = 1;
  }
}

bool TopologyMgr::AddDS(GroupId group, GroupRole role, NodeInfo info,
                        bool opt_resync, bool* state_changed_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ValidateDSGroupRole_(group, role);
  std::string alias = "DS " + GroupRoleName(group, role);
  LOG(informational, Server, "Adding ", alias.c_str());
  DSGroup& dsg = ds_groups_[group];
  NodeInfo& dsinfo = dsg.ds_infos[role];
  bool sameinfo = dsinfo.ip == info.ip && dsinfo.port == info.port;
  if (dsinfo.port != 0 && !sameinfo)
    return false;
  dsg.opt_resync[role] = opt_resync;
  if (sameinfo)
    return true;
  if (opt_resync) {  // Old DS
    if (!CheckUUID_(info.uuid, alias)) {
      LOG(error, Server, "Invalid peer DS. UUID: ", info.uuid);
      return false;
    }
  } else {  // New DS
    LOG(notice, Server, "Registering DS with UUID: ", info.uuid);
    AddUUID_(info.uuid, alias);
  }
  dsinfo = info;
  dsinfo.SetAlias(group, role);
  uint64_t orig_id = dsg.state_change_id;
  dsg.UpdateState();
  if (dsg.state == kDSGReady && dsg.creating) {
    dsg.creating = false;
    if (max_group_id_ <= group) {
      max_group_id_ = group;
      server_->server_info()->Set(
          "max-dsg-id", boost::lexical_cast<std::string>(max_group_id_));
    }
  }
  *state_changed_ret = orig_id != dsg.state_change_id;
  return true;
}

void TopologyMgr::RemoveDS(GroupId group, GroupRole role,
                           bool* state_changed_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ValidateDSGroupRole_(group, role);
  LOG(informational, Server, "Removing DS ", GroupRoleName(group, role));
  DSGroup& dsg = ds_groups_[group];
  dsg.ds_infos[role].port = 0;
  uint64_t orig_id = dsg.state_change_id;
  dsg.UpdateState();
  *state_changed_ret = orig_id != dsg.state_change_id;
}

bool TopologyMgr::DSRecovered(GroupId group, GroupRole role, char end_type) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ValidateDSGroupRole_(group, role);
  LOG(informational, Server, "DS ", GroupRoleName(group, role),
      end_type ? " fully recovered" : " partially recovered");
  return ds_groups_[group].SetRecovered(role, end_type);
}

bool TopologyMgr::DSGReady(GroupId group) const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return ds_groups_[group].state == kDSGReady ||
         ds_groups_[group].state == kDSGDegraded;
}

bool TopologyMgr::AllDSReady() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return AllDSReady_();
}

bool TopologyMgr::AllDSReady_() {
  for (GroupId group = 0; group < ds_groups_.size(); ++group)
    if (ds_groups_[group].state != kDSGReady &&
        ds_groups_[group].state != kDSGDegraded &&
        !ds_groups_[group].creating)
      return false;
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  for (GroupId group = 0; group < ds_groups_.size(); ++group) {
    DSGroup& dsg = ds_groups_[group];
    if (dsg.creating)
      continue;
    for (GroupRole role = 0; role < kNumDSPerGroup; ++role) {
      if (dsg.state == kDSGDegraded && dsg.failed == role)
        continue;
      if (!tracker_mapper->GetDSFimSocket(group, role))
        return false;
    }
  }
  return true;
}

bool TopologyMgr::SuggestDSRole(GroupId* group_ret, GroupRole* role_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  // First look for a never-allocated role
  for (GroupId g = 0; g < ds_groups_.size(); ++g) {
    DSGroup& dsg = ds_groups_[g];
    if (dsg.state == kDSGPending) {
      for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
        if (dsg.ds_infos[r].ip == 0) {
          *group_ret = g;
          *role_ret = r;
          return true;
        }
    }
  }
  // Look for a degraded role
  for (GroupId g = 0; g < ds_groups_.size(); ++g) {
    DSGroup& dsg = ds_groups_[g];
    if (dsg.state == kDSGDegraded) {
      for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
        if (dsg.ds_infos[r].port == 0) {
          *group_ret = g;
          *role_ret = r;
          return true;
        }
    }
  }
  return false;
}

void TopologyMgr::SendAllDSInfo(boost::shared_ptr<IFimSocket> peer) {
  MUTEX_LOCK_GUARD(data_mutex_);
  for (GroupId g = 0; g < ds_groups_.size(); ++g) {
    DSGroup& dsg = ds_groups_[g];
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      if (!server_->tracker_mapper()->GetDSFimSocket(g, r))
        continue;  // Will send topo change fim on DS connection
      FIM_PTR<TopologyChangeFim> fim =
          TopologyChangeFim::MakePtr();
      (*fim)->type = 'D';
      (*fim)->ds_group = g;
      (*fim)->ds_role = r;
      (*fim)->ip = dsg.ds_infos[r].ip;
      (*fim)->port = dsg.ds_infos[r].port;
      (*fim)->pid = dsg.ds_infos[r].pid;
      std::memset((*fim)->uuid, '\0', 40U);
      std::strncpy((*fim)->uuid, dsg.ds_infos[r].uuid, 36U);
      (*fim)->joined = '\x01';
      peer->WriteMsg(fim);
    }
    peer->WriteMsg(StateChangeFim_(g));
  }
}

void TopologyMgr::AnnounceDS(GroupId group, GroupRole role, bool is_added,
                             bool state_changed) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ValidateDSGroupRole_(group, role);
  FIM_PTR<TopologyChangeFim> change_fim =
      TopologyChangeFim::MakePtr();
  DSGroup& dsg = ds_groups_[group];
  (*change_fim)->ds_group = group;
  (*change_fim)->ds_role = role;
  (*change_fim)->type = 'D';
  (*change_fim)->joined = is_added ? '\x01' : '\x00';
  (*change_fim)->ip = is_added ? dsg.ds_infos[role].ip : 0;
  (*change_fim)->port = is_added ? dsg.ds_infos[role].port : 0;
  (*change_fim)->pid = is_added ? dsg.ds_infos[role].pid : 0;
  std::memset((*change_fim)->uuid, '\0', 40U);
  std::strncpy((*change_fim)->uuid, dsg.ds_infos[role].uuid, 36U);
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  Replicate_(change_fim);
  tracker_mapper->DSGBroadcast(group, change_fim, role);
  tracker_mapper->FCBroadcast(change_fim);
  if (!state_changed)
    return;
  AnnounceDSGState_(group, is_added ? kNumDSPerGroup : role);
}

void TopologyMgr::AnnounceDSGState(GroupId group) {
  MUTEX_LOCK_GUARD(data_mutex_);
  AnnounceDSGState_(group);
}

void TopologyMgr::AnnounceDSGState_(GroupId group, GroupRole except) {
  // Create a DSC, and do one of the followings:
  //  * Wait, if DSC in progress and freezing FCs; otherwise
  //  * Announce to DSs, if (a) DSC in progress, or (b) no DSC in
  //    progress and (i) state is not changed to ready or (ii) there
  //    is no FC
  //  * Start FC wait, if otherwise
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  DSGroup& dsg = ds_groups_[group];
  pending_dscs_[group].id = dsg.state_change_id;
  pending_dscs_[group].acked_ds.clear();
  pending_dscs_[group].state = dsg.state;
  pending_dscs_[group].except = except;
  pending_dscs_[group].ds_notified = false;
  LOG(notice, Server, "DSG state change: group ", PINT(group),
      " to be changed to state: ", ToStr(dsg.state));
  if (dsc_in_progress_) {
    if (dsc_pending_fcs_.size() == 0)
      NotifyDSDSCs_();  // Announce to DS
    return;
  }
  dsc_pending_fcs_.clear();  // Just to be sure
  if (dsg.state == kDSGReady) {
    BOOST_FOREACH(const FCInfoMap::const_iterator::value_type& p, fc_infos_) {
      if (tracker_mapper->GetFCFimSocket(p.first))
        dsc_pending_fcs_.insert(p.first);
    }
  }
  if (dsc_pending_fcs_.size() == 0) {
    NotifyDSDSCs_();
  } else {
    LOG(notice, Server, "Setting FCs to wait for DSG state change");
    dsc_in_progress_ = true;
    SetFCsDSCFrozen_(true);
  }
}

void TopologyMgr::SetFCsDSCFrozen_(bool enable) {
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  FIM_PTR<DSGStateChangeWaitFim> fim = DSGStateChangeWaitFim::MakePtr();
  (*fim)->enable = enable;
  tracker_mapper->FCBroadcast(fim);
}

bool TopologyMgr::IsDSGStateChanging() {
  return dsc_in_progress_;
}

void TopologyMgr::AckDSGStateChangeWait(ClientNum client) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (dsc_pending_fcs_.find(client) == dsc_pending_fcs_.end())
    return;
  dsc_pending_fcs_.erase(client);
  if (dsc_pending_fcs_.size() == 0)
    NotifyDSDSCs_();
}

void TopologyMgr::NotifyDSDSCs_() {
  BOOST_FOREACH(DSGStateChangeMap::iterator::value_type&
                p,
                pending_dscs_) {
    GroupId group = p.first;
    DSGStateChange& dsc = p.second;
    if (dsc.ds_notified)
      continue;
    LOG(notice, Server, "Sending DSG state change to DSs: group ", PINT(group),
        " state: ", ToStr(dsc.state));
    FIM_PTR<IFim> state_change_fim = StateChangeFim_(group);
    Replicate_(state_change_fim);
    server_->tracker_mapper()->DSGBroadcast(
        group, state_change_fim, dsc.except);
    dsc.ds_notified = true;
  }
}

bool TopologyMgr::AckDSGStateChange(uint64_t state_change_id,
                                    boost::shared_ptr<IFimSocket> peer,
                                    GroupId* group_ret,
                                    DSGroupState* state_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  *state_ret = kDSGOutdated;
  GroupRole role;
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  if (!tracker_mapper->FindDSRole(peer, group_ret, &role)) {
    LOG(notice, Server,
        "Ignoring a DSG state change ack: DS group role not found");
    return false;
  }
  DSGStateChangeMap::iterator dsc_it = pending_dscs_.find(*group_ret);
  if (dsc_it == pending_dscs_.end() || dsc_it->second.id != state_change_id)
    return false;
  DSGStateChange& dsc = dsc_it->second;
  DSGroup& dsg = ds_groups_[*group_ret];
  *state_ret = dsg.state;
  if (dsc.acked_ds.find(role) != dsc.acked_ds.end())  // Defensive
    return false;
  dsc.acked_ds.insert(role);
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
    if (dsc.acked_ds.find(r) == dsc.acked_ds.end()
        && tracker_mapper->GetDSFimSocket(*group_ret, r))
      return false;
  pending_dscs_.erase(dsc_it);
  tracker_mapper->FCBroadcast(StateChangeFim_(*group_ret));
  if (dsc_in_progress_ && pending_dscs_.size() == 0) {
    dsc_in_progress_ = false;
    LOG(notice, Server, "Setting FCs to resume after DSG state change");
    SetFCsDSCFrozen_(false);
  }
  return true;
}

bool TopologyMgr::AllDSGStartable() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return AllDSGStartable_();
}

bool TopologyMgr::ForceStartDSG() {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (!AllDSGStartable_())
    return false;
  for (GroupId g = 0; g < ds_groups_.size(); ++g)
    if (ds_groups_[g].ForceStart())
      AnnounceDSGState_(g);
  return true;
}

std::vector<ClientNum> TopologyMgr::GetFCs() const {
  std::vector<ClientNum> ret;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    for (FCInfoMap::const_iterator it = fc_infos_.begin();
         it != fc_infos_.end();
         ++it)
      ret.push_back(it->first);
  }
  return ret;
}

unsigned TopologyMgr::GetNumFCs() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return fc_infos_.size();
}

bool TopologyMgr::AddFC(ClientNum fc_id, NodeInfo info) {
  MUTEX_LOCK_GUARD(data_mutex_);
  LOG(informational, Server, "Adding FC ", PINT(fc_id));
  FCInfoMap::iterator it = fc_infos_.find(fc_id);
  if (it != fc_infos_.end())
    return it->second.ip == info.ip;
  fc_infos_[fc_id] = info;
  return true;
}

void TopologyMgr::RemoveFC(ClientNum fc_id) {
  MUTEX_LOCK_GUARD(data_mutex_);
  LOG(informational, Server, "Removing FC ", PINT(fc_id));
  fc_infos_.erase(fc_id);
}

void TopologyMgr::SetFCTerminating(ClientNum fc_id) {
  MUTEX_LOCK_GUARD(data_mutex_);
  fc_infos_[fc_id].terminating = true;
}

bool TopologyMgr::SuggestFCId(ClientNum* fc_id_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ClientNum first_tried = next_fc_id_;
  bool found = false;
  do {
    ClientNum ret = next_fc_id_;
    next_fc_id_ = (next_fc_id_ + 1) % (kMaxClients + 1);
    if (fc_infos_.find(ret) == fc_infos_.end()) {
      *fc_id_ret = ret;
      found = true;
      break;
    }
  } while (next_fc_id_ != first_tried);
  return found;
}

void TopologyMgr::SetNextFCId(ClientNum fc_id) {
  MUTEX_LOCK_GUARD(data_mutex_);
  next_fc_id_ = fc_id;
}

void TopologyMgr::AnnounceFC(ClientNum fc_id, bool is_added) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  boost::shared_ptr<IFimSocket> ms_fim_socket =
      tracker_mapper->GetMSFimSocket();
  if (!ms_fim_socket)
    return;
  FIM_PTR<TopologyChangeFim> change_fim =
      TopologyChangeFim::MakePtr();
  (*change_fim)->ds_role = 0;
  (*change_fim)->client_num = fc_id;
  (*change_fim)->type = 'F';
  (*change_fim)->joined = is_added ? '\x01' : '\x00';
  (*change_fim)->ip = is_added ? fc_infos_[fc_id].ip : 0;
  (*change_fim)->port = is_added ? fc_infos_[fc_id].port : 0;
  (*change_fim)->pid = is_added ? fc_infos_[fc_id].pid : 0;
  std::memset((*change_fim)->uuid, '\0', 40U);  // TODO(Joseph): Support UUID?
  ms_fim_socket->WriteMsg(change_fim);
}

void TopologyMgr::SendAllFCInfo() {
  MUTEX_LOCK_GUARD(data_mutex_);
  boost::shared_ptr<IFimSocket> ms_fim_socket =
      server_->tracker_mapper()->GetMSFimSocket();
  if (!ms_fim_socket)
    return;
  for (FCInfoMap::const_iterator it = fc_infos_.begin();
       it != fc_infos_.end(); ++it) {
    FIM_PTR<TopologyChangeFim> change_fim =
        TopologyChangeFim::MakePtr();
    (*change_fim)->ds_role = 0;
    (*change_fim)->client_num = it->first;
    (*change_fim)->type = 'F';
    (*change_fim)->joined = '\x01';
    (*change_fim)->ip = it->second.ip;
    (*change_fim)->port = it->second.port;
    (*change_fim)->pid = it->second.pid;
    std::memset((*change_fim)->uuid, '\0', 40U);  // TODO(Joseph): Support UUID?
    ms_fim_socket->WriteMsg(change_fim);
  }
}

bool TopologyMgr::AckDSShutdown(boost::shared_ptr<IFimSocket> peer) {
  MUTEX_LOCK_GUARD(data_mutex_);
  GroupId group;
  GroupRole role;
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  if (!tracker_mapper->FindDSRole(peer, &group, &role)) {
    LOG(notice, Server,
        "Ignoring a DSG state change ack: DS group role not found");
    return false;
  }
  DSGroup& dsg = ds_groups_[group];
  dsg.SetShuttingDown(role);
  for (GroupId g = 0; g < ds_groups_.size(); ++g)
    if (!ds_groups_[g].IsShuttingDown())
      return false;
  return true;
}

void TopologyMgr::StartStopWorker() {
  IThreadGroup* thread_group;
  bool is_active;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    thread_group = server_->thread_group();
    is_active = MSActive_();
  }
  if (is_active) {
    LOG(informational, Server, "Starting MS workers");
    thread_group->Start();
  } else {
    LOG(informational, Server, "Stopping MS workers");
    thread_group->Stop(2000);
  }
}

bool TopologyMgr::MSActive_() {
  MSState state = server_->state_mgr()->GetState();
  if (state != kStateStandalone && state != kStateActive)
    return false;
  for (GroupId group = 0; group < ds_groups_.size(); ++group) {
    GroupRole failed;
    DSGroupState dsg_state = GetDSGState_(group, &failed);
    if (dsg_state != kDSGReady && dsg_state != kDSGDegraded &&
        dsg_state != kDSGResync)
      return false;
  }
  return true;
}

void TopologyMgr::AnnounceShutdown() {
  MUTEX_LOCK_GUARD(data_mutex_);
  ITrackerMapper* mapper = server_->tracker_mapper();
  FIM_PTR<SysShutdownReqFim> fim = SysShutdownReqFim::MakePtr();
  // Peer MS
  boost::shared_ptr<IFimSocket> ms_socket = mapper->GetMSFimSocket();
  if (ms_socket)
    ms_socket->WriteMsg(fim);
  // All FC
  mapper->FCBroadcast(fim);
  // All DS announcing by state change
  for (GroupId g = 0; g < ds_groups_.size(); ++g) {
    ds_groups_[g].state = kDSGShuttingDown;
    pending_dscs_[g].id = ++ds_groups_[g].state_change_id;
    pending_dscs_[g].acked_ds.clear();
    pending_dscs_[g].state = kDSGShuttingDown;
    pending_dscs_[g].except = kNumDSPerGroup;
    pending_dscs_[g].ds_notified = false;
  }
  dsc_in_progress_ = false;
  NotifyDSDSCs_();
}

void TopologyMgr::AnnounceHalt() {
  MUTEX_LOCK_GUARD(data_mutex_);
  ITrackerMapper* mapper = server_->tracker_mapper();
  FIM_PTR<SysHaltFim> fim = SysHaltFim::MakePtr();
  // Peer MS
  boost::shared_ptr<IFimSocket> ms_socket = mapper->GetMSFimSocket();
  if (ms_socket)
    ms_socket->WriteMsg(fim);
  // All FC
  mapper->FCBroadcast(fim);
  // All DS
  for (GroupId g = 0; g < ds_groups_.size(); ++g)
    mapper->DSGBroadcast(g, fim);
}

std::vector<NodeInfo> TopologyMgr::GetDSInfos() {
  MUTEX_LOCK_GUARD(data_mutex_);
  {
    std::vector<NodeInfo> ret;
    for (GroupId g = 0; g < ds_groups_.size(); ++g) {
      for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
        const NodeInfo& ds_info = ds_groups_[g].ds_infos[r];
        if (ds_info.port != 0)
          ret.push_back(ds_info);
      }
    }
    return ret;
  }
}

std::vector<NodeInfo> TopologyMgr::GetFCInfos() {
  MUTEX_LOCK_GUARD(data_mutex_);
  {
    std::vector<NodeInfo> ret;
    for (FCInfoMap::const_iterator it = fc_infos_.begin();
         it != fc_infos_.end(); ++it)
      if (!it->second.terminating)
        ret.push_back(it->second);
    return ret;
  }
}

bool TopologyMgr::AddMS(NodeInfo info, bool new_node) {
  MUTEX_LOCK_GUARD(data_mutex_);
  std::string alias = server_->configs().role() == "MS1" ? "MS2" : "MS1";
  bool self_is_new = server_->peer_time_keeper()->GetLastUpdate() == 0;
  if (self_is_new || new_node) {
    LOG(notice, Server, "Registering MS with UUID: ", info.uuid);
    AddUUID_(info.uuid, alias);
  } else if (!CheckUUID_(info.uuid, alias)) {
    LOG(error, Server, "Invalid peer MS. UUID: ", info.uuid);
    return false;
  }
  return true;
}

void TopologyMgr::AddUUID_(const std::string& uuid, const std::string& role) {
  node_uuids_[role] = uuid;
  server_->store()->PersistAllUUID(node_uuids_);
}

bool TopologyMgr::CheckUUID_(
    const std::string& uuid, const std::string& role) {
  return node_uuids_.find(role) != node_uuids_.end() &&
         node_uuids_[role] == uuid;
}

}  // namespace

ITopologyMgr* MakeTopologyMgr(BaseMetaServer* server) {
  return new TopologyMgr(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
