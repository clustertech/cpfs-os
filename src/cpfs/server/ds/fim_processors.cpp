/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define initial DS Fim processor.
 */

#include "server/ds/fim_processors.hpp"

#include <stdint.h>

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "admin_info.hpp"
#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "mutex_util.hpp"
#include "req_completion.hpp"
#include "req_tracker.hpp"
#include "shutdown_mgr.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/conn_mgr.hpp"
#include "server/ds/degrade.hpp"
#include "server/ds/resync.hpp"
#include "server/ds/store.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/thread_group.hpp"
#include "server/worker_util.hpp"

namespace cpfs {

class IFimProcessor;

namespace server {
namespace ds {
namespace {

/**
 * Number of seconds for the expecting new FimSocket state to last
 * when a peer is recovering.
 */
const int kExpectingTimeout = 15;

/**
 * Handle control Fims for connection to MS.
 */
class MSCtrlFimProcessor : public MemberFimProcessor<MSCtrlFimProcessor> {
 public:
  /**
   * @param server The server having the processor
   */
  explicit MSCtrlFimProcessor(BaseDataServer* server) : server_(server) {
    AddHandler(&MSCtrlFimProcessor::HandleDSMSRegSuccess);
    AddHandler(&MSCtrlFimProcessor::HandleMSRegRejected);
    AddHandler(&MSCtrlFimProcessor::HandleTopologyChange);
    AddHandler(&MSCtrlFimProcessor::HandleDSGStateChange);
    AddHandler(&MSCtrlFimProcessor::HandleDSGDistressModeChange);
    AddHandler(&MSCtrlFimProcessor::HandleDSResyncReq);
    AddHandler(&MSCtrlFimProcessor::HandleSysHalt);
    AddHandler(&MSCtrlFimProcessor::HandleMSStatFS);
    AddHandler(&MSCtrlFimProcessor::HandleConfigChange);
    AddHandler(&MSCtrlFimProcessor::HandleConfigList);
  }

 private:
  BaseDataServer* server_; /**< The server having the processor */
  /** Timer to limit the time when request trackers are expecting FimSockets */
  boost::scoped_ptr<DeadlineTimer> expecting_timer_[kNumDSPerGroup];

  bool HandleDSMSRegSuccess(const FIM_PTR<DSMSRegSuccessFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    LOG(notice, Server, "Connected to MS ", PVal(socket));
    server_->store()->SetRole((*fim)->ds_group, (*fim)->ds_role);
    LOG(notice, Server, "DS ",
        GroupRoleName((*fim)->ds_group, (*fim)->ds_role), " is running");
    server_->tracker_mapper()->SetMSFimSocket(socket);
    socket->OnCleanup(
        boost::bind(&IConnMgr::ReconnectMS, server_->conn_mgr(), socket));
    return true;
  }

  bool HandleMSRegRejected(const FIM_PTR<MSRegRejectedFim>& fim,
                           const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    LOG(informational, Server,
        "Connection to MS ", PVal(socket), " is rejected");
    server_->conn_mgr()->DisconnectMS(socket);
    return true;
  }

  bool HandleTopologyChange(const FIM_PTR<TopologyChangeFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    // Connect to DSx
    if ((*fim)->type == 'D') {
      GroupId group = (*fim)->ds_group;
      GroupRole role = (*fim)->ds_role;
      if ((*fim)->joined) {
        server_->conn_mgr()->ConnectDS((*fim)->ip, (*fim)->port,
                                       group, role);
      } else {
        boost::shared_ptr<IFimSocket> ds_socket =
            server_->tracker_mapper()->GetDSFimSocket(group, role);
        if (ds_socket) {
          LOG(notice, Server,
              "Disconnecting from DS ", GroupRoleName(group, role));
          ds_socket->Shutdown();
        }
      }
    }
    return true;
  }

  bool HandleDSGStateChange(const FIM_PTR<DSGStateChangeFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    {
      server_->set_distressed((*fim)->distress);
      // Let workers know current distress mode
      FIM_PTR<DSGDistressModeChangeFim> distress_fim =
          DSGDistressModeChangeFim::MakePtr();
      (*distress_fim)->distress = (*fim)->distress;
      LOG(informational, Server, "Distress mode is now ",
          PINT((*fim)->distress));
      server_->thread_group()->EnqueueAll(distress_fim);
      boost::unique_lock<boost::shared_mutex> lock;
      uint64_t old_state_change;
      GroupRole old_failed;
      DSGroupState old_state = server_->dsg_state(
          &old_state_change, &old_failed);
      server_->set_dsg_state((*fim)->state_change_id,
                             DSGroupState((*fim)->state),
                             (*fim)->failed, &lock);
      MUTEX_LOCK_SCOPE_LOG();
      // Reset Fim deferring, but only after DSG state unique lock is acquired
      if (old_state == kDSGRecovering && (*fim)->state != kDSGRecovering)
        server_->thread_group()->EnqueueAll(DeferResetFim::MakePtr());
      if ((*fim)->state == kDSGReady) {
        server_->inode_removal_tracker()->SetPersistRemoved(false);
        server_->durable_range()->SetConservative(false);
        server_->dsg_ready_time_keeper()->Start();
      } else {
        server_->dsg_ready_time_keeper()->Stop();
      }
      LOG(notice, Server, "DSG state change, state is now: ",
          ToStr(DSGroupState((*fim)->state)));
      if ((*fim)->state == kDSGDegraded) {
        server_->inode_removal_tracker()->SetPersistRemoved(true);
        server_->durable_range()->SetConservative(true);
        server_->degraded_cache()->SetActive(true);
      } else if ((*fim)->state == kDSGRecovering) {
        server_->set_opt_resync((*fim)->opt_resync);
        server_->req_completion_checker_set()->OnCompleteAllInodes(
            boost::bind(&MSCtrlFimProcessor::SetDSGRecovering, this,
                        (*fim)->state_change_id, socket));
        return true;
      } else if ((*fim)->state == kDSGShuttingDown) {
        server_->shutdown_mgr()->Init(kShutdownTimeout);
        server_->req_completion_checker_set()->OnCompleteAllInodes(
            boost::bind(&MSCtrlFimProcessor::AckDSGStateChange, this,
                        (*fim)->state_change_id, socket));
        return true;
      }
    }
    AckDSGStateChange((*fim)->state_change_id, socket);
    return true;
  }

  void SetDSGRecovering(uint64_t state_change_id,
                        boost::shared_ptr<IFimSocket> socket) {
    server_->degraded_cache()->SetActive(false);
    uint64_t curr_state_change_id;
    GroupRole failed;
    if (server_->dsg_state(&curr_state_change_id, &failed) != kDSGRecovering ||
        curr_state_change_id != state_change_id)
      return;
    IStore* store = server_->store();
    GroupRole role = store->ds_role();
    if (role == failed) {
      server_->resync_fim_processor()->AsyncResync(
          boost::bind(&MSCtrlFimProcessor::ResyncRecvComplete, this, _1));
    } else {
      GroupId group = store->ds_group();
      SetExpecting(group, failed, true);
      IAsioPolicy* policy = server_->asio_policy();
      if (!expecting_timer_[failed])
        expecting_timer_[failed].reset(policy->MakeDeadlineTimer());
      policy->SetDeadlineTimer(
          expecting_timer_[failed].get(),
          kExpectingTimeout,
          boost::bind(&MSCtrlFimProcessor::OnExpectExpiry,
                      this, group, failed, false, _1));
    }
    AckDSGStateChange(state_change_id, socket);
  }

  void ResyncRecvComplete(bool success) {
    if (success) {
      server_->tracker_mapper()->GetMSFimSocket()->WriteMsg(
          DSResyncEndFim::MakePtr());
    } else {
      // TODO(Isaac): Should terminate the DS (rare)
      LOG(error, Server, "DS Resync failed");
      return;
    }
  }

  void OnExpectExpiry(GroupId group, GroupRole role, bool expecting,
                      const boost::system::error_code& error) {
    if (!error)
      SetExpecting(group, role, expecting);
    // Otherwise, it is supposedly a timer cancellation
  }

  void SetExpecting(GroupId group, GroupRole role, bool expecting) {
    server_->tracker_mapper()->GetDSTracker(
        group, role)->SetExpectingFimSocket(expecting);
  }

  void AckDSGStateChange(uint64_t state_change_id,
                         const boost::shared_ptr<IFimSocket>& socket) {
    FIM_PTR<DSGStateChangeAckFim> ack_fim =
        DSGStateChangeAckFim::MakePtr();
    (*ack_fim)->state_change_id = state_change_id;
    socket->WriteMsg(ack_fim);
  }

  bool HandleDSGDistressModeChange(
      const FIM_PTR<DSGDistressModeChangeFim>& fim,
      const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    LOG(informational, Server, "Distress mode is now ", PINT((*fim)->distress));
    server_->thread_group()->EnqueueAll(fim);
    return true;
  }

  bool HandleDSResyncReq(const FIM_PTR<DSResyncReqFim>& fim,
                         const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    (void) socket;
    LOG(notice, Server, "DS Resync requested");
    uint64_t curr_state_change_id;
    GroupRole failed;
    if (server_->dsg_state(&curr_state_change_id, &failed) != kDSGRecovering)
      return true;
    GroupRole role = server_->store()->ds_role();
    if (failed == role)
      return true;
    server_->resync_mgr()->Start(failed);
    return true;
  }

  bool HandleSysHalt(const FIM_PTR<SysHaltFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    (void) socket;
    server_->shutdown_mgr()->Shutdown();
    return true;
  }

  bool HandleMSStatFS(const FIM_PTR<MSStatFSFim>& fim,
                      const boost::shared_ptr<IFimSocket>& socket) {
    ReplyOnExit r(fim, socket);
    uint64_t total_space;
    uint64_t free_space;
    int ret = server_->store()->Stat(&total_space, &free_space);
    if (ret != 0) {
      LOG(error, Server, "Cannot obtain free space statistics");
      r.SetResult(ret);
      return true;
    }
    FIM_PTR<MSStatFSReplyFim> reply = MSStatFSReplyFim::MakePtr();
    (*reply)->total_space = total_space;
    (*reply)->free_space = free_space;
    r.SetNormalReply(reply);
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
};

/**
 * Initial Fim processor for the data server.
 */
class InitFimProcessor : public MemberFimProcessor<InitFimProcessor> {
 public:
  /**
   * @param data_server The data server using the processor.
   */
  explicit InitFimProcessor(BaseDataServer* data_server)
      : data_server_(data_server) {
    AddHandler(&InitFimProcessor::HandleDSDSReg);
    AddHandler(&InitFimProcessor::HandleFCDSReg);
  }

 private:
  BaseDataServer* data_server_; /**< The data server using the processor */

  /**
   * Handle registration from DS.
   *
   * @param fim The registration Fim.
   *
   * @param socket The sender.
   */
  bool HandleDSDSReg(const FIM_PTR<DSDSRegFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    LOG(notice, Server, "Connection from DS ",
        GroupRoleName((*fim)->ds_group, (*fim)->ds_role), " ",
        socket->remote_info(), " accepted");
    socket->Migrate(data_server_->ds_asio_policy());
    data_server_->tracker_mapper()->SetDSFimSocket(
        socket, (*fim)->ds_group, (*fim)->ds_role);
    socket->SetFimProcessor(data_server_->ds_fim_processor());
    return true;
  }

  /**
   * Handle registration from FC.
   *
   * @param fim The registration Fim.
   *
   * @param socket The sender.
   */
  bool HandleFCDSReg(const FIM_PTR<FCDSRegFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    ClientNum client_num = (*fim)->client_num;
    LOG(notice, Server, "Connection from FC ", PINT(client_num), " ",
        socket->remote_info(), " accepted");
    socket->Migrate(data_server_->fc_asio_policy());
    data_server_->tracker_mapper()->SetFCFimSocket(socket, client_num);
    socket->SetFimProcessor(data_server_->fc_fim_processor());
    return true;
  }
};

}  // namespace

IFimProcessor* MakeMSCtrlFimProcessor(BaseDataServer* data_server) {
  return new MSCtrlFimProcessor(data_server);
}

IFimProcessor* MakeInitFimProcessor(BaseDataServer* data_server) {
  return new InitFimProcessor(data_server);
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
