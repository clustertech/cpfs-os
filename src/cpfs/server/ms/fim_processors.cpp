/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define initial MS Fim processor.
 */

#include "server/ms/fim_processors.hpp"

#include <stdint.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>

#include "admin_info.hpp"
#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "ds_iface.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "shutdown_mgr.hpp"
#include "thread_fim_processor.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/conn_mgr.hpp"
#include "server/ms/dsg_op_state.hpp"
#include "server/ms/resync_mgr.hpp"
#include "server/ms/startup_mgr.hpp"
#include "server/ms/stat_keeper.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/store.hpp"
#include "server/ms/topology.hpp"
#include "server/thread_group.hpp"
#include "server/worker_util.hpp"

namespace cpfs {

class IFimProcessor;

namespace server {
namespace ms {
namespace {

/**
 * Initial Fim processor for the meta server.
 */
class InitFimProcessor : public MemberFimProcessor<InitFimProcessor> {
 public:
  /**
   * @param meta_server The meta server using the processor.
   */
  explicit InitFimProcessor(BaseMetaServer* meta_server)
      : server_(meta_server) {
    AddHandler(&InitFimProcessor::HandleFCMSReg);
    AddHandler(&InitFimProcessor::HandleDSMSReg);
    AddHandler(&InitFimProcessor::HandleMSMSReg);
  }

 private:
  BaseMetaServer* server_; /**< The meta server using the processor */

  bool HandleFCMSReg(const FIM_PTR<FCMSRegFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    if ((*fim)->type == 'A' && (*fim)->force_start
        && server_->state_mgr()->GetState() == kStateHANotReady) {
      LOG(notice, Server, "Server is becoming active as requested by admin");
      server_->state_mgr()->SwitchState(kStateActive);
      server_->ha_counter()->SetActive();
      server_->inode_removal_tracker()->SetPersistRemoved(true);
      server_->durable_range()->SetConservative(true);
    }
    if (!AcceptConnect(socket)) {
      socket->WriteMsg(MSRegRejectedFim::MakePtr());
      return true;
    }
    ClientNum client_num;
    if ((*fim)->is_reconnect) {
      client_num = (*fim)->client_num;
    } else {
      // TODO(Joseph): Allocate node ID (future?)
      if (!GetFCId(&client_num)) {
        socket->WriteMsg(MSRegRejectedFim::MakePtr());
        LOG(error, Server, "Failed to accept FC connection. "
            "Cannot allocate Id for ", PVal(*socket));
        return true;
      }
    }
    if ((*fim)->type == 'F')
      return InitFCSocket(socket, client_num, (*fim)->pid);
    else
      return InitAdminSocket(socket, client_num);
  }

  /**
   * Allocate a client number to a new FC socket.
   *
   * @param client_num_ret Where to write the client number
   */
  bool GetFCId(ClientNum* client_num_ret) {
    if (!server_->topology_mgr()->SuggestFCId(client_num_ret))
      return false;
    if (server_->tracker_mapper()->GetFCFimSocket(*client_num_ret))
      return false;
    return true;
  }

  /**
   * Initialize a client socket after connection and client number
   * assignment.
   *
   * @param socket The sender
   *
   * @param client_num The client number assigned
   *
   * @param pid The process ID
   */
  bool InitFCSocket(const boost::shared_ptr<IFimSocket>& socket,
                    ClientNum client_num, uint16_t pid) {
    IAsioPolicy* policy = server_->asio_policy();
    TcpEndpoint endpoint = policy->GetRemoteEndpoint(socket->socket());
    NodeInfo info;
    info.ip = IPToInt(boost::lexical_cast<std::string>(endpoint.address()));
    info.port = endpoint.port();
    info.pid = pid;
    info.SetAlias(client_num);
    if (!server_->topology_mgr()->AddFC(client_num, info)) {
      socket->WriteMsg(MSRegRejectedFim::MakePtr());
      LOG(error, Server, "Failed to add FC ", PINT(client_num), " to topology"
          " for ", PVal(*socket));
      return true;
    }
    socket->Migrate(server_->fc_asio_policy());
    // Set processor and tracker before reply
    server_->tracker_mapper()->SetFCFimSocket(socket, client_num);
    IFimProcessor* processor = server_->fc_fim_processor();
    if (server_->state_mgr()->GetState() == kStateFailover)
      processor = server_->failover_processor();
    socket->SetFimProcessor(processor);
    // Reply after processor and tracker are set
    FIM_PTR<FCMSRegSuccessFim> success_fim =
        FCMSRegSuccessFim::MakePtr();
    (*success_fim)->client_num = client_num;
    socket->WriteMsg(success_fim);
    server_->topology_mgr()->SendAllDSInfo(socket);
    server_->topology_mgr()->AnnounceFC(client_num, true);
    socket->OnCleanup(
        boost::bind(&InitFimProcessor::CleanupFCSocket, this, socket,
                    boost::shared_ptr<DeadlineTimer>()));
    LOG(notice, Server, "Connection from ",
        socket->name(), " ", socket->remote_info(), " accepted");
    return true;
  }

  bool InitAdminSocket(const boost::shared_ptr<IFimSocket>& socket,
                       ClientNum client_num) {
    server_->tracker_mapper()->SetAdminFimSocket(socket, client_num);
    socket->SetFimProcessor(server_->admin_fim_processor());
   // Reply after processor and tracker are set
    FIM_PTR<FCMSRegSuccessFim> success_fim =
        FCMSRegSuccessFim::MakePtr();
    (*success_fim)->client_num = client_num;
    socket->WriteMsg(success_fim);
    server_->topology_mgr()->SendAllDSInfo(socket);
    LOG(notice, Server, "Connection from Admin ", socket->remote_info(),
        " accepted");
    return true;
  }

  void CleanupFCSocket(const boost::shared_ptr<IFimSocket>& socket,
                       boost::shared_ptr<DeadlineTimer> timer) {
    // If some work of the socket is still there, we wait so that the
    // remaining work will not undo the cleanup done in CleanupFC()
    if (!server_->thread_group()->SocketPending(socket)) {
      server_->conn_mgr()->CleanupFC(socket);
      return;
    }
    // Schedule another invocation in the FC communication thread
    timer.reset(server_->fc_asio_policy()->MakeDeadlineTimer());
    socket->asio_policy()->SetDeadlineTimer(
        timer.get(),
        5.0,
        boost::bind(&InitFimProcessor::CleanupFCSocket, this, socket, timer));
  }

  bool HandleDSMSReg(const FIM_PTR<DSMSRegFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    if (!AcceptConnect(socket)) {
      socket->WriteMsg(MSRegRejectedFim::MakePtr());
      return true;
    }
    LOG(informational, Server, "DS info received: ", IntToIP((*fim)->ip),
        ":", PINT((*fim)->port));
    NodeInfo info;
    info.ip = (*fim)->ip;
    info.port = (*fim)->port;
    info.pid = (*fim)->pid;
    info.SetUUID((*fim)->uuid);
    GroupId group_id = (*fim)->ds_group;
    GroupRole role_id = (*fim)->ds_role;
    bool state_changed;
    if (!TryDSJoin((*fim)->is_grouped, &group_id, &role_id, info,
                   (*fim)->opt_resync, &state_changed)) {
      socket->WriteMsg(MSRegRejectedFim::MakePtr());
      return true;
    }
    if ((*fim)->distressed)
      server_->topology_mgr()->SetDSGDistressed(group_id, true);
    server_->tracker_mapper()->SetDSFimSocket(socket, group_id, role_id);
    socket->SetFimProcessor(server_->ds_fim_processor());
    socket->OnCleanup(boost::bind(
        &IConnMgr::CleanupDS, server_->conn_mgr(),
        group_id, role_id));
    LOG(notice, Server, "Connection from ",
        socket->name(), " ", socket->remote_info(), " accepted");
    // Reply DS the group ID and role assigned
    FIM_PTR<DSMSRegSuccessFim> success_fim =
        DSMSRegSuccessFim::MakePtr();
    (*success_fim)->ds_group = group_id;
    (*success_fim)->ds_role = role_id;
    socket->WriteMsg(success_fim);
    bool all_ds_ready = server_->topology_mgr()->AllDSReady();
    server_->topology_mgr()->
        AnnounceDS(group_id, role_id, true, state_changed || all_ds_ready);
    if (all_ds_ready)
      server_->stat_keeper()->Run();
    return true;
  }

  bool TryDSJoin(bool is_grouped, GroupId* group_id, GroupRole* role_id,
                 const NodeInfo& info, bool opt_resync,
                 bool* state_changed_ret) {
    if (!is_grouped) {
      if (!server_->topology_mgr()->SuggestDSRole(group_id, role_id)) {
        LOG(error, Server,
            "Failed to join DS group. All DS roles are occupied");
        return false;
      }
    } else {
      GroupRole failed;
      DSGroupState state = server_->topology_mgr()->
          GetDSGState(*group_id, &failed);
      if (state == kDSGPending) {
        bool prev_degraded =
            server_->startup_mgr()->dsg_degraded(*group_id, &failed);
        if (prev_degraded && failed == *role_id) {
          LOG(warning, Server, "Cannot accept previously failed DS ",
              GroupRoleName(*group_id, *role_id), " until DSG is started");
          return false;
        }
      }
    }
    if (server_->tracker_mapper()->GetDSFimSocket(*group_id, *role_id)) {
      LOG(warning, Server, "DS ", GroupRoleName(*group_id, *role_id),
          " already occupied");
      return false;
    }
    if (!server_->topology_mgr()->AddDS(*group_id, *role_id, info,
                                        opt_resync, state_changed_ret)) {
      LOG(warning, Server, "Internal inconsistency with DS ",
          GroupRoleName(*group_id, *role_id));
      return false;
    }
    if (*state_changed_ret)
      server_->topology_mgr()->StartStopWorker();
    return true;
  }

  bool HandleMSMSReg(const FIM_PTR<MSMSRegFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    LOG(notice, Server, "MS connection received");
    NodeInfo ni;
    ni.SetUUID((*fim)->uuid);
    if (!server_->topology_mgr()->AddMS(ni, (*fim)->new_node)) {
      socket->Shutdown();
      return true;
    }
    MSState state = server_->state_mgr()->GetState();
    if (state == kStateHANotReady
        || (state == kStateActive && !(*fim)->active)) {
      FIM_PTR<MSMSRegSuccessFim> success_fim = MSMSRegSuccessFim::MakePtr();
      std::strncpy(
          (*success_fim)->uuid, server_->store()->GetUUID().c_str(), 36U);
      (*success_fim)->uuid[36] = '\0';
      (*success_fim)->ha_counter = server_->ha_counter()->GetCount();
      (*success_fim)->active = (state == kStateActive);
      (*success_fim)->new_node =
          server_->peer_time_keeper()->GetLastUpdate() == 0;
      socket->WriteMsg(success_fim);
      server_->conn_mgr()->InitMSConn(socket, (*fim)->ha_counter,
                                      (*fim)->active);
      LOG(notice, Server, "Connection from ", PVal(*socket), " accepted");
      return true;
    } else {
      LOG(notice, Server,
          "Cannot accept MS connection. Server state=", ToStr(state));
      socket->WriteMsg(MSRegRejectedFim::MakePtr());
      return true;
    }
  }

  /**
   * Test whether connection from FC / DS can be accepted
   *
   * @param socket The socket to accept
   */
  bool AcceptConnect(const boost::shared_ptr<IFimSocket>& socket) const {
    MSState state = server_->state_mgr()->GetState();
    bool active = server_->ha_counter()->IsActive();
    bool accept = !server_->IsHAMode() || active || state == kStateFailover;
    accept = accept && state != kStateShuttingDown;
    if (!accept) {
      LOG(notice, Server,
          "Cannot accept connection from ", PVal(*socket), ". ",
          "Server state=", ToStr(state), ", active=", PINT(active));
    }
    return accept;
  }
};

/**
 * Inter-MS Fim processor for the meta server.
 */
class MSCtrlFimProcessor : public MemberFimProcessor<MSCtrlFimProcessor> {
 public:
  /**
   * @param meta_server The meta server using the processor.
   */
  explicit MSCtrlFimProcessor(BaseMetaServer* meta_server)
      : server_(meta_server) {
    AddHandler(&MSCtrlFimProcessor::HandleTopologyChange);
    AddHandler(&MSCtrlFimProcessor::HandleMSResyncReq);
    AddHandler(&MSCtrlFimProcessor::HandleDSGStateChange);
    AddHandler(&MSCtrlFimProcessor::HandleMSResyncEnd);
    AddHandler(&MSCtrlFimProcessor::HandleShutdownReq);
    AddHandler(&MSCtrlFimProcessor::HandleSysHalt);
    AddHandler(&MSCtrlFimProcessor::HandleDSGResize);
    AddHandler(&MSCtrlFimProcessor::HandleConfigChange);
    AddHandler(&MSCtrlFimProcessor::HandleConfigList);
    AddHandler(&MSCtrlFimProcessor::HandleDSResyncPhaseInodeList);
  }

 private:
  BaseMetaServer* server_; /**< The meta server using the processor */
  /** Inodes removed by peer before resync */
  std::vector<InodeNum> resync_removed_;

  /**
   * Handle topology change Fims from active MS.
   *
   * @param fim The topology change Fim
   *
   * @param socket The sender
   */
  bool HandleTopologyChange(const FIM_PTR<TopologyChangeFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    bool state_changed;
    switch ((*fim)->type) {
      case 'D':
        if ((*fim)->joined) {
          NodeInfo info;
          info.ip = (*fim)->ip;
          info.port = (*fim)->port;
          info.pid = (*fim)->pid;
          info.SetUUID((*fim)->uuid);
          info.SetAlias((*fim)->ds_group, (*fim)->ds_role);
          server_->topology_mgr()->
              AddDS((*fim)->ds_group, (*fim)->ds_role, info,
                    false, &state_changed);
        } else {
          server_->topology_mgr()->
              RemoveDS((*fim)->ds_group, (*fim)->ds_role, &state_changed);
        }
        break;
      case 'F':
        if ((*fim)->joined) {
          NodeInfo info;
          info.ip = (*fim)->ip;
          info.port = (*fim)->port;
          info.pid = (*fim)->pid;
          info.SetAlias((*fim)->client_num);
          server_->topology_mgr()->AddFC((*fim)->client_num, info);
        } else {
          server_->topology_mgr()->RemoveFC((*fim)->client_num);
        }
        break;
    }
    return true;
  }

  /**
   * Handle resync request. Inodes to be resynced is selected based on the
   * last seen time for peer MS. On account of heartbeat loss, replication
   * and disk write delay, the time value is moved backward
   */
  bool HandleMSResyncReq(const FIM_PTR<MSResyncReqFim>& fim,
                         const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    server_->state_mgr()->OnState(kStateResync,
        boost::bind(&MSCtrlFimProcessor::DoMSResync, this, fim));
    return true;
  }

  /**
   * Handle resync end request from active MS.
   */
  bool HandleMSResyncEnd(const FIM_PTR<MSResyncEndFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer) {
    ReplyOnExit replier(fim, peer);
    LOG(notice, Server, "MS Resync completed");
    server_->peer_time_keeper()->Start();
    server_->state_mgr()->SwitchState(kStateStandby);
    server_->ha_counter()->PersistCount();  // Can use me again
    server_->inode_removal_tracker()->SetPersistRemoved(false);
    server_->durable_range()->SetConservative(false);
    int err_no = server_->store()->SetLastResyncDirTimes();
    replier.SetResult(err_no);
    return true;
  }

  /**
   * Handle DS group state change Fims from active MS.
   *
   * @param fim The DS group state change Fim
   *
   * @param socket The sender
   */
  bool HandleDSGStateChange(const FIM_PTR<DSGStateChangeFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    GroupId group = (*fim)->ds_group;
    GroupRole failed = (*fim)->failed;
    DSGroupState state = DSGroupState((*fim)->state);
    server_->topology_mgr()->SetDSGState(group, failed, state,
                                         (*fim)->state_change_id);
    switch (state) {
      case kDSGDegraded:
        server_->startup_mgr()->set_dsg_degraded(group, true, failed);
        break;
      case kDSGReady:
        server_->startup_mgr()->set_dsg_degraded(group, false, failed);
        break;
      default:
        {}  // Do nothing
    }
    return true;
  }

  /**
   * Perform MS resync
   */
  void DoMSResync(const FIM_PTR<MSResyncReqFim>& fim) {
    if ((*fim)->first)
      resync_removed_.clear();
    std::size_t cnt = fim->tail_buf_size() / sizeof(InodeNum);
    std::size_t before = resync_removed_.size();
    resync_removed_.resize(before + cnt);
    std::memcpy(resync_removed_.data() + before, fim->tail_buf(),
                cnt * sizeof(InodeNum));
    if ((*fim)->last) {
      server_->resync_mgr()->SendAllResync(
          (*fim)->last == 'O', resync_removed_);
      resync_removed_.clear();
    }
  }

  /**
   * Handle system shutdown request from active MS.
   */
  bool HandleShutdownReq(const FIM_PTR<SysShutdownReqFim>& fim,
                         const boost::shared_ptr<IFimSocket>& socket) {
    (void)fim;
    (void)socket;
    server_->shutdown_mgr()->Init(kShutdownTimeout);
    return true;
  }

  /**
   * Handle system halt request from active MS
   *
   * @param fim The Fim
   *
   * @param socket The sender
   */
  bool HandleSysHalt(const FIM_PTR<SysHaltFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    (void)fim;
    (void)socket;
    server_->shutdown_mgr()->Shutdown();
    return true;
  }

  /**
   * Handle config change request form MS
   *
   * @param fim The Fim
   *
   * @param socket The sender
   */
  bool HandleConfigChange(const FIM_PTR<PeerConfigChangeReqFim>& fim,
                          const boost::shared_ptr<IFimSocket>& socket) {
    ReplyOnExit replier(fim, socket);
    if ((*fim)->name == std::string("log_severity")) {
      server_->configs().set_log_severity((*fim)->value);
      replier.SetResult(0);
    } else if ((*fim)->name == std::string("log_path")) {
      server_->configs().set_log_path((*fim)->value);
      replier.SetResult(0);
    } else if ((*fim)->name == std::string("num_ds_groups") &&
               server_->state_mgr()->GetState() == kStateActive) {
      // For consistency, only active MS may set the num groups
      server_->topology_mgr()->set_num_groups(
          boost::lexical_cast<int>((*fim)->value));
      replier.SetResult(0);
    }
    return true;
  }

  /**
   * Handle config list request form MS
   *
   * @param fim The Fim
   *
   * @param socket The sender
   */
  bool HandleDSGResize(const FIM_PTR<DSGResizeFim>& fim,
                       const boost::shared_ptr<IFimSocket>& socket) {
    (void)fim;
    (void)socket;
    server_->topology_mgr()->set_num_groups((*fim)->num_groups);
    return true;
  }

  bool HandleConfigList(const FIM_PTR<ClusterConfigListReqFim>& fim,
                        const boost::shared_ptr<IFimSocket>& socket) {
    ReplyOnExit r(fim, socket);
    std::vector<ConfigItem> ret = server_->configs().List();
    std::size_t config_info_size = ret.size() * sizeof(ConfigItem);
    FIM_PTR<ClusterConfigListReplyFim> reply
        = ClusterConfigListReplyFim::MakePtr(config_info_size);
    std::memcpy(reply->tail_buf(), ret.data(), config_info_size);
    r.SetNormalReply(reply);
    return true;
  }

  bool HandleDSResyncPhaseInodeList(
      const FIM_PTR<DSResyncPhaseInodeListFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer) {
    InodeNum* inodes = reinterpret_cast<InodeNum*>(fim->tail_buf());
    std::vector<InodeNum> resyncing;
    std::size_t size = fim->tail_buf_size() / sizeof(InodeNum);
    std::copy(inodes, inodes + size, std::back_inserter(resyncing));
    server_->dsg_op_state_mgr()->SetDsgInodesResyncing(
        (*fim)->ds_group, resyncing);
    FIM_PTR<ResultCodeReplyFim> reply = ResultCodeReplyFim::MakePtr();
    reply->set_req_id(fim->req_id());
    (*reply)->err_no = 0;
    peer->WriteMsg(reply);
    return true;
  }
};

/**`
 * FimProcessor for processing Fims sent from DS to MS.
 */
class DSCtrlFimProcessor : public MemberFimProcessor<DSCtrlFimProcessor> {
 public:
  /**
   * @param meta_server The meta server using the processor.
   */
  explicit DSCtrlFimProcessor(BaseMetaServer* meta_server)
      : server_(meta_server) {
    AddHandler(&DSCtrlFimProcessor::HandleDSGStateChangeAck);
    AddHandler(&DSCtrlFimProcessor::HandleDSResyncEnd);
    AddHandler(&DSCtrlFimProcessor::HandleDSResyncPhaseInodeList);
  }

 private:
  BaseMetaServer* server_; /**< The meta server using the processor */

  bool HandleDSGStateChangeAck(
      const FIM_PTR<DSGStateChangeAckFim>& fim,
      const boost::shared_ptr<IFimSocket>& socket) {
    ITopologyMgr* topology_mgr = server_->topology_mgr();
    GroupId group;
    DSGroupState state;
    if (topology_mgr->AckDSGStateChange((*fim)->state_change_id, socket, &group,
                                        &state)) {
      ITrackerMapper* tracker_mapper = server_->tracker_mapper();
      if (state == kDSGDegraded) {
        GroupRole failed_role;
        topology_mgr->GetDSGState(group, &failed_role);
        tracker_mapper->GetDSTracker(group, failed_role)->
            RedirectRequests(boost::bind(&DSFimRedirector, _1, group,
                                         tracker_mapper));
      } else if (state == kDSGRecovering) {
        for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
          boost::shared_ptr<IFimSocket> ds_socket =
              tracker_mapper->GetDSFimSocket(group, r);
          if (ds_socket)
            ds_socket->WriteMsg(DSResyncReqFim::MakePtr());
        }
      } else if (state == kDSGShuttingDown) {
        server_->shutdown_mgr()->Shutdown();
      }
    }
    return true;
  }

  bool HandleDSResyncEnd(const FIM_PTR<DSResyncEndFim>& fim,
                         const boost::shared_ptr<IFimSocket>& peer) {
    DSResyncEndFim& rfim = static_cast<DSResyncEndFim&>(*fim);
    GroupId group;
    GroupRole role;
    if (server_->tracker_mapper()->FindDSRole(peer, &group, &role)) {
      ITopologyMgr* topology_mgr = server_->topology_mgr();
      if (topology_mgr->DSRecovered(group, role, rfim->end_type)) {
        server_->startup_mgr()->set_dsg_degraded(group, false, role);
        topology_mgr->StartStopWorker();
        topology_mgr->AnnounceDSGState(group);
      }
    }
    return true;
  }

  bool HandleDSResyncPhaseInodeList(
      const FIM_PTR<DSResyncPhaseInodeListFim>& fim,
      const boost::shared_ptr<IFimSocket>& peer) {
    GroupId group;
    GroupRole role;
    if (server_->tracker_mapper()->FindDSRole(peer, &group, &role)) {
      GroupRole failed;
      DSGroupState state = server_->topology_mgr()->GetDSGState(group, &failed);
      if (state != kDSGResync)
        return true;
      std::size_t size = fim->tail_buf_size() / sizeof(InodeNum);
      InodeNum* inodes = reinterpret_cast<InodeNum*>(fim->tail_buf());
      std::vector<InodeNum> resyncing;
      std::copy(inodes, inodes + size, std::back_inserter(resyncing));
      server_->dsg_op_state_mgr()->SetDsgInodesResyncing(group, resyncing);
      MSState ms_state = server_->state_mgr()->GetState();
      if (ms_state == kStateActive) {
        boost::shared_ptr<IReqTracker> tracker = server_->tracker_mapper()->
            GetMSTracker();
        boost::shared_ptr<IReqEntry> entry = MakeReplReqEntry(tracker, fim);
        boost::unique_lock<MUTEX_TYPE> repl_lock;
        if (tracker->AddRequestEntry(entry, &repl_lock)) {
          entry->OnAck(boost::bind(&DSCtrlFimProcessor::WaitInodesCompleteOp,
                                   this, resyncing, fim, peer));
          return true;
        }
      }
      WaitInodesCompleteOp(resyncing, fim, peer);
    }
    return true;
  }

  void WaitInodesCompleteOp(std::vector<InodeNum> resyncing,
                            const FIM_PTR<IFim>& fim,
                            const boost::shared_ptr<IFimSocket>& peer) {
    server_->dsg_op_state_mgr()->OnInodesCompleteOp(
        resyncing,
        boost::bind(&DSCtrlFimProcessor::ReplyPhaseInodeList, this, fim, peer));
  }

  void ReplyPhaseInodeList(const FIM_PTR<IFim>& fim,
                           const boost::shared_ptr<IFimSocket>& peer) {
    FIM_PTR<ResultCodeReplyFim> reply = ResultCodeReplyFim::MakePtr();
    reply->set_req_id(fim->req_id());
    reply->set_final();
    (*reply)->err_no = 0;
    peer->WriteMsg(reply);
  }
};

}  // namespace

IFimProcessor* MakeInitFimProcessor(BaseMetaServer* meta_server) {
  return new InitFimProcessor(meta_server);
}

IFimProcessor* MakeMSCtrlFimProcessor(BaseMetaServer* meta_server) {
  return new MSCtrlFimProcessor(meta_server);
}

IFimProcessor* MakeDSCtrlFimProcessor(BaseMetaServer* meta_server) {
  return new DSCtrlFimProcessor(meta_server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
