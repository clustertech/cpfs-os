/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of cpfs::server::ms::IConnMgr.
 */

#include "server/ms/conn_mgr_impl.hpp"

#include <stdint.h>

#include <cstring>
#include <string>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_set.hpp>

#include "admin_info.hpp"
#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "connector.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_processor.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "req_tracker.hpp"
#include "shutdown_mgr.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/conn_mgr.hpp"
#include "server/ms/failover_mgr.hpp"
#include "server/ms/inode_usage.hpp"
#include "server/ms/resync_mgr.hpp"
#include "server/ms/startup_mgr.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/store.hpp"
#include "server/ms/topology.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Connect to primary MS.  Once the connections is ready, the
 * FimSocket is be set to the TrackerMapper.
 */
class ConnMgr : public IConnMgr {
 public:
  /**
   * Construct the MS connector
   *
   * @param server The meta server using the connector.
   */
  explicit ConnMgr(BaseMetaServer* server)
      : server_(server) {}

  void Init() {
    if (server_->configs().role() == "MS2") {
      init_msms_fim_processor_.reset(
          new InitMSMSFimProcessor(this, server_->topology_mgr()));
      ConnectMS();
    }
  }

  void InitMSConn(const boost::shared_ptr<IFimSocket>& socket,
                  uint64_t peer_ha_counter, bool peer_active) {
    server_->state_mgr()->SwitchState(kStateResync);
    server_->topology_mgr()->StartStopWorker();  // Stop
    server_->tracker_mapper()->SetMSFimSocket(socket);
    socket->SetFimProcessor(server_->ms_fim_processor());
    socket->OnCleanup(boost::bind(&ConnMgr::CleanupMSFimSocket, this));
    // Active / Standby election
    bool oldActive = server_->ha_counter()->IsActive();
    bool isActive = server_->ha_counter()->Elect(peer_ha_counter, peer_active);
    if (isActive) {
      if (!oldActive)
        server_->StartServerActivated();
    } else {
      server_->resync_mgr()->RequestResync();
    }
  }

  void CleanupDS(GroupId group_id, GroupRole role_id) {
    bool state_changed;
    ITopologyMgr* topology_mgr = server_->topology_mgr();
    topology_mgr->RemoveDS(group_id, role_id, &state_changed);
    GroupRole failed;
    DSGroupState group_state = topology_mgr->GetDSGState(group_id, &failed);
    if (group_state == kDSGDegraded) {
      server_->startup_mgr()->set_dsg_degraded(group_id, true, role_id);
    } else if (group_state == kDSGFailed) {
      LOG(critical, Server, "DSG failed, MS will terminate itself in 10s");
      boost::shared_ptr<DeadlineTimer> timer(
          server_->asio_policy()->MakeDeadlineTimer());
      // Suicide after 10s upon any DSG failing
      server_->asio_policy()->SetDeadlineTimer(
          timer.get(), 10.0,
          boost::bind(&ConnMgr::Suicide, this, timer));
    }
    topology_mgr->AnnounceDS(group_id, role_id, false, state_changed);
  }

  void CleanupFC(const boost::shared_ptr<IFimSocket>& socket) {
    ClientNum client_num = socket->GetReqTracker()->peer_client_num();
    // Notify that the FC is about to be killed
    server_->topology_mgr()->SetFCTerminating(client_num);
    // Delay FC removal until cleaner completes an iteration. This avoids
    // old cached results being sent to the new FC reusing old FC ID
    boost::shared_ptr<DeadlineTimer> timer(
        server_->asio_policy()->MakeDeadlineTimer());
    server_->asio_policy()->SetDeadlineTimer(
        timer.get(), kMSCleanerIterationTime + kMSReplySetMaxAge,
        boost::bind(&ConnMgr::RemoveFC, this, timer, client_num));
    // Close opened inodes, by faking Release requests
    boost::unordered_set<InodeNum> opened_inodes
        = server_->inode_usage()->GetFCOpened(client_num);
    boost::unordered_set<InodeNum>::iterator itr = opened_inodes.begin();
    ReqId req_id = ReqId(client_num + 1) << (64 - kClientBits);
    req_id -= ReqId(1) << 32;  // Use the last 4G request ID space
    for (; itr != opened_inodes.end(); ++itr) {
      LOG(debug, FS, "Dereferencing file open count for client ",
          PINT(client_num), " inode ", PHex(*itr));
      FIM_PTR<ReleaseFim> rfim = ReleaseFim::MakePtr();
      (*rfim)->inode = *itr;
      (*rfim)->keep_read = false;
      (*rfim)->clean = false;
      rfim->set_req_id(req_id++);
      server_->fc_fim_processor()->Process(rfim, socket);
    }
  }

 private:
  BaseMetaServer* server_; /**< The meta server using the connector */
  boost::scoped_ptr<IFimProcessor> init_msms_fim_processor_;

  /**
   * Connect to MS1.
   *
   * @param timer The timer used to trigger the connection.  Not
   * really used, but keep the timer from dying
   */
  void ConnectMS(boost::shared_ptr<DeadlineTimer> timer =
                 boost::shared_ptr<DeadlineTimer>()) {
    (void) timer;
    if (server_->state_mgr()->GetState() == kStateFailover) {
      LOG(notice, Server, "Failover in progress, deferring MS reconnection");
      ScheduleReconnMS();
    } else {
      server_->connector()->AsyncConnect(
          server_->configs().ms1_host(), server_->configs().ms1_port(),
          init_msms_fim_processor_.get(),
          boost::bind(&ConnMgr::MSConnected, this, _1));
    }
  }

  /**
   * Schedule for MS1 reconnection.
   */
  void ScheduleReconnMS() {
    boost::shared_ptr<DeadlineTimer> timer(
        server_->asio_policy()->MakeDeadlineTimer());
    server_->asio_policy()->SetDeadlineTimer(
        timer.get(), 5,
        boost::bind(&ConnMgr::ConnectMS, this, timer));
  }

  /**
   * Handle initial connection to MS1.
   */
  void MSConnected(boost::shared_ptr<IFimSocket> fim_socket) {
    // Delay setting Fim processor until MSMSRegSuccess is received
    FIM_PTR<MSMSRegFim> fim = MSMSRegFim::MakePtr();
    std::strncpy((*fim)->uuid, server_->store()->GetUUID().c_str(), 36U);
    (*fim)->uuid[36] = '\0';
    (*fim)->ha_counter = server_->ha_counter()->GetCount();
    (*fim)->active = server_->ha_counter()->IsActive();
    (*fim)->new_node = server_->peer_time_keeper()->GetLastUpdate() == 0;
    fim_socket->WriteMsg(fim);
  }

  void RemoveFC(boost::shared_ptr<DeadlineTimer> timer, ClientNum client_num) {
    (void) timer;  // Just to keep the timer from dying
    server_->topology_mgr()->RemoveFC(client_num);
    server_->topology_mgr()->AnnounceFC(client_num, false);
  }

  /**
   * Clean up the broken MSFimSocket, start failover and reconnect to MS1.
   */
  void CleanupMSFimSocket() {
    server_->peer_time_keeper()->Stop();
    server_->inode_removal_tracker()->SetPersistRemoved(true);
    server_->durable_range()->SetConservative(true);
    if (server_->state_mgr()->GetState() == kStateStandby)
      server_->failover_mgr()->Start(kReconfirmTimeout);
    // Reconnect
    if (server_->configs().role() == "MS2")
      ScheduleReconnMS();
  }

  /**
   * Kill the server.  This is called once we know that the system can
   * no longer function, so that other parties (most likely, FCs on
   * the same machine as the server) will not mistaken to send
   * requests here.
   */
  void Suicide(boost::shared_ptr<DeadlineTimer> timer) {
    (void) timer;  // Just to keep the timer from dying
    server_->shutdown_mgr()->Shutdown();
  }

  /**
   * Handle initial Fims for connection to MS1.
   */
  class InitMSMSFimProcessor : public MemberFimProcessor<InitMSMSFimProcessor> {
   public:
    /**
     * @param conn_mgr The MS connection manager using the processor.
     */
    explicit InitMSMSFimProcessor(ConnMgr* conn_mgr, ITopologyMgr* topology)
        : conn_mgr_(conn_mgr), topology_(topology) {
      AddHandler(&InitMSMSFimProcessor::HandleMSMSRegSuccess);
      AddHandler(&InitMSMSFimProcessor::HandleMSRegRejected);
    }

   private:
    ConnMgr* conn_mgr_; /**< The DS conn manager using the processor */
    ITopologyMgr* topology_;

    bool HandleMSMSRegSuccess(const FIM_PTR<MSMSRegSuccessFim>& fim,
                              const boost::shared_ptr<IFimSocket>& socket) {
      NodeInfo ni;
      ni.SetUUID((*fim)->uuid);
      if (!topology_->AddMS(ni, (*fim)->new_node)) {
        socket->Shutdown();
        conn_mgr_->ScheduleReconnMS();
        return true;
      }
      LOG(notice, Server, "Connected to MS ", socket->remote_info());
      conn_mgr_->InitMSConn(socket, (*fim)->ha_counter, (*fim)->active);
      return true;
    }

    bool HandleMSRegRejected(const FIM_PTR<MSRegRejectedFim>& fim,
                             const boost::shared_ptr<IFimSocket>& socket) {
      (void) fim;
      LOG(notice, Server, "Connection to ", socket->remote_info(), " rejected");
      socket->Shutdown();
      conn_mgr_->ScheduleReconnMS();
      return true;
    }
  };
};

}  // namespace

IConnMgr* MakeConnMgr(BaseMetaServer* server) {
  return new ConnMgr(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
