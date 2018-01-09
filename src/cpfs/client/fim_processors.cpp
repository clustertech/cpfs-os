/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement the ClientFimProcessor.
 */
#include "client/fim_processors.hpp"

#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "ds_iface.hpp"
#include "dsg_state.hpp"
#include "event.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "op_completion.hpp"
#include "req_entry.hpp"
#include "req_tracker.hpp"
#include "shutdown_mgr.hpp"
#include "tracker_mapper.hpp"
#include "client/base_client.hpp"
#include "client/cache.hpp"
#include "client/conn_mgr.hpp"

namespace cpfs {

class IFimProcessor;

namespace client {
namespace {

/**
 * Handle initial Fims for connection to MS.
 */
class MSCtrlFimProcessor : public MemberFimProcessor<MSCtrlFimProcessor> {
 public:
  /**
   * @param client The client using the processor
   */
  explicit MSCtrlFimProcessor(BaseFSClient* client)
      : client_(client), ready_seen_(false) {
    AddHandler(&MSCtrlFimProcessor::HandleFCMSRegSuccess);
    AddHandler(&MSCtrlFimProcessor::HandleDSGStateChangeWait);
    AddHandler(&MSCtrlFimProcessor::HandleTopologyChange);
    AddHandler(&MSCtrlFimProcessor::HandleMSRegRejected);
    AddHandler(&MSCtrlFimProcessor::HandleDSGStateChange);
    AddHandler(&MSCtrlFimProcessor::HandleSysShutdownReq);
    AddHandler(&MSCtrlFimProcessor::HandleSysHalt);
  }

 private:
  BaseFSClient* client_; /**< The client using the manager */
  bool ready_seen_; /**< Whether MS shows ready in DSGStateChange before */

  bool HandleFCMSRegSuccess(const FIM_PTR<FCMSRegSuccessFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    LOG(notice, Server, "Connected to MS ", PVal(socket));
    // Set to inode allocator
    client_->set_client_num((*fim)->client_num);
    ITrackerMapper* tracker_mapper = client_->tracker_mapper();
    tracker_mapper->SetClientNum((*fim)->client_num);
    tracker_mapper->SetMSFimSocket(socket, false);
    socket->OnCleanup(
        boost::bind(&MSCtrlFimProcessor::CleanupMSFimSocket, this));
    return true;
  }

  bool HandleMSRegRejected(const FIM_PTR<MSRegRejectedFim>& fim,
                           const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    LOG(informational, Server,
        "Connection to MS ", PVal(socket), " is rejected");
    socket->Shutdown();
    client_->conn_mgr()->ForgetMS(socket, true);
    return true;
  }

  bool HandleDSGStateChangeWait(const FIM_PTR<DSGStateChangeWaitFim>& fim,
                                const boost::shared_ptr<IFimSocket>& socket) {
    LOG(notice, Server, "Setting request freeze to ",
        PVal(int((*fim)->enable)));
    client_->SetReqFreeze((*fim)->enable);
    if ((*fim)->enable) {
      Event ev;
      client_->op_completion_checker_set()->OnCompleteAllGlobal(
          boost::bind(&Event::Invoke, &ev));
      ev.Wait();
      LOG(informational, Server, "Acknowledging request freeze request");
      socket->WriteMsg(DSGStateChangeReadyFim::MakePtr());
    }
    return true;
  }

  bool HandleTopologyChange(const FIM_PTR<TopologyChangeFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    if ((*fim)->type == 'D') {
      GroupId group = (*fim)->ds_group;
      GroupRole role = (*fim)->ds_role;
      if ((*fim)->joined) {
        client_->conn_mgr()->ConnectDS((*fim)->ip, (*fim)->port,
                                              group, role);
      } else {
        boost::shared_ptr<IFimSocket> ds_socket =
            client_->tracker_mapper()->GetDSFimSocket(group, role);
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
    (void) socket;
    GroupId group = (*fim)->ds_group;
    GroupRole failed = (*fim)->failed;
    DSGroupState old_state = client_->
        set_dsg_state(group, DSGroupState((*fim)->state), failed);
    LOG(notice, Server, "DSG state change, group ", PINT((*fim)->ds_group),
        " state is now: ", ToStr(DSGroupState((*fim)->state)));
    client_->cache_mgr()->InvalidateAll();
    // TODO(Isaac): should wait until all threads knows about the change (race?)
    if ((*fim)->ready) {
      boost::shared_ptr<IReqTracker> ms_tracker =
          client_->tracker_mapper()->GetMSTracker();
      if (client_->conn_mgr()->IsReconnectingMS()) {
        // In reconnecting (failover), FC to MS is plugged when
        // DS group ready AND reconfirmation phase ends
        if (ms_tracker->ResendReplied())
          LOG(informational, Server, "Unconfirmed requests sent");
        ms_tracker->GetFimSocket()->
            WriteMsg(ReconfirmEndFim::MakePtr());
        client_->conn_mgr()->SetReconnectingMS(false);
      } else if (!ready_seen_) {
        ms_tracker->Plug();
        // Never plug an MS tracker again with DSGStateChange: all
        // future plugging should be due to SysStateNotify after
        // reconfirmation Fim processing
        ready_seen_ = true;
      }
    }
    if (old_state != kDSGDegraded && (*fim)->state == kDSGDegraded) {
      boost::shared_ptr<IReqTracker> ds_tracker =
          client_->tracker_mapper()->GetDSTracker(group, failed);
      if (ds_tracker->RedirectRequests(
              boost::bind(&DSFimRedirector, _1, group,
                          client_->tracker_mapper())))
        LOG(informational, Fim, "Fims to failed DS ",
            GroupRoleName(group, failed), " redirected to other DSs");
    }
    return true;
  }

  bool HandleSysShutdownReq(const FIM_PTR<SysShutdownReqFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    (void) socket;
    client_->shutdown_mgr()->Init();
    return true;
  }

  bool HandleSysHalt(const FIM_PTR<SysHaltFim>& fim,
                     const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    (void) socket;
    client_->shutdown_mgr()->Shutdown();
    return true;
  }

  /**
   * Clean up MSFimSocket created in HandleFCMSRegSuccess() on disconnected.
   * Try connect to stand-by meta server.
   */
  void CleanupMSFimSocket() {
    boost::shared_ptr<IFimSocket> socket =
        client_->tracker_mapper()->GetMSFimSocket();
    if (client_->IsShuttingDown()) {
      client_->conn_mgr()->ForgetMS(socket, false);
    } else {
      client_->cache_mgr()->InvalidateAll();
      client_->conn_mgr()->ReconnectMS(socket);
    }
    // TOOD(Joseph): Stop client if in non-HA mode
  }
};

/**
 * Handle control Fims from DS.
 */
class DSCtrlFimProcessor : public MemberFimProcessor<DSCtrlFimProcessor> {
 public:
  /**
   * @param client The client using the processor
   */
  explicit DSCtrlFimProcessor(BaseFSClient* client) : client_(client) {
    AddHandler(&DSCtrlFimProcessor::HandleNotDegraded);
  }

 private:
  BaseFSClient* client_; /**< The client using the manager */

  bool HandleNotDegraded(const FIM_PTR<NotDegradedFim>& fim,
                         const boost::shared_ptr<IFimSocket>& socket) {
    IReqTracker* tracker = socket->GetReqTracker();
    boost::shared_ptr<IReqEntry> entry =
        tracker->GetRequestEntry((*fim)->redirect_req);
    if (!entry)
      return true;
    FIM_PTR<IFim> req = entry->request();
    if (!req)
      return true;
    GroupId group;
    GroupRole redirect_target;
    if (req->type() == kReadFim) {
      ReadFim& rfim = static_cast<ReadFim&>(*req);
      group = rfim->target_group;
      redirect_target = rfim->target_role;
    } else if (req->type() == kWriteFim) {
      WriteFim& wfim = static_cast<WriteFim&>(*req);
      group = wfim->target_group;
      redirect_target = wfim->target_role;
    } else {
      return true;
    }
    tracker->RedirectRequest(
        req->req_id(),
        client_->tracker_mapper()->GetDSTracker(group, redirect_target));
    return true;
  }
};

/**
 * Implement the GenCtrlFimProcessor interface.
 */
class GenCtrlFimProcessor : public MemberFimProcessor<GenCtrlFimProcessor> {
 public:
  /**
   * @param client The client
   */
  explicit GenCtrlFimProcessor(BaseFSClient* client) : client_(client) {
    AddHandler(&GenCtrlFimProcessor::HandleInvInode);
    AddHandler(&GenCtrlFimProcessor::HandleSysStateNotify);
  }

 private:
  BaseFSClient* client_;

  bool HandleInvInode(const FIM_PTR<InvInodeFim>& fim,
                      const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    LOG(debug, Cache, "To invalidate ", PHex((*fim)->inode));
    client_->cache_mgr()->InvalidateInode((*fim)->inode, (*fim)->clear_page);
    return true;
  }

  bool HandleSysStateNotify(const FIM_PTR<SysStateNotifyFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    if ((*fim)->type == 'M') {
      boost::shared_ptr<IReqTracker> ms_tracker =
          client_->tracker_mapper()->GetMSTracker();
      // Meta server state changed. Plug or unplug the socket
      if ((*fim)->ready)
        ms_tracker->Plug();
    }
    return true;
  }
};

}  // namespace

IFimProcessor* MakeMSCtrlFimProcessor(BaseFSClient* client) {
  return new MSCtrlFimProcessor(client);
}

IFimProcessor* MakeDSCtrlFimProcessor(BaseFSClient* client) {
  return new DSCtrlFimProcessor(client);
}

IFimProcessor* MakeGenCtrlFimProcessor(BaseFSClient* client) {
  return new GenCtrlFimProcessor(client);
}

}  // namespace client
}  // namespace cpfs
