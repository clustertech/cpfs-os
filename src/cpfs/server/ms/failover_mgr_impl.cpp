/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of FailoverMgr for switching stand-by server to
 * active, notifying connected FCs when ready.
 */

#include "server/ms/failover_mgr_impl.hpp"

#include <stdexcept>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/reverse_lock.hpp>
#include <boost/unordered_set.hpp>

#include "asio_policy.hpp"
#include "common.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fim_socket_impl.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "periodic_timer.hpp"
#include "req_tracker.hpp"
#include "shutdown_mgr.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/conn_mgr.hpp"
#include "server/ms/dirty_inode.hpp"
#include "server/ms/failover_mgr.hpp"
#include "server/ms/inode_src.hpp"
#include "server/ms/replier.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/topology.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {
/**
 * Implementation for FailoverMgr.
 */
class FailoverMgr : public IFailoverMgr {
  // Note that a FailoverMgr can be started at most once by the same
  // program, since after a failover, the program is active and will
  // never become standby again until it dies.
 public:
  /**
   * Construct the Failover manager
   *
   * @param server The meta server
   */
  explicit FailoverMgr(BaseMetaServer* server)
      : server_(server), data_mutex_(MUTEX_INIT),
        failover_pending_(false), cleanup_pending_(false) {}

  void SetPeriodicTimerMaker(PeriodicTimerMaker maker) {
    periodic_timer_maker_ = maker;
  }

  void Start(double timeout) {
    MUTEX_LOCK_GUARD(data_mutex_);
    failover_pending_ = true;
    cleanup_pending_ = true;
    GroupId num_groups = server_->topology_mgr()->num_groups();
    for (GroupId g = 0; g < num_groups; ++g)
      for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
        if (server_->topology_mgr()->HasDS(g, r))
          server_->tracker_mapper()->GetDSTracker(g, r)->
              SetExpectingFimSocket(true);
    if (!server_->state_mgr()->SwitchState(kStateFailover)) {
      LOG(error, Server, "Failover is aborted. Invalid server state: ",
          ToStr(server_->state_mgr()->GetState()));
      server_->shutdown_mgr()->Shutdown();  // Suicide
      return;
    }
    LOG(notice, Server, "Failover started");
    server_->ha_counter()->SetActive();
    // Stop time keeper until peer is restarted and resync completed
    server_->peer_time_keeper()->Stop();
    timer_ = periodic_timer_maker_(server_->asio_policy()->io_service(),
                                   timeout);
    timer_->OnTimeout(boost::bind(&FailoverMgr::Timeout, this));
    if (server_->topology_mgr()->GetNumFCs() == 0)
      SwitchActive_();
  }

  void AddReconfirmDone(ClientNum client_num) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (!timer_)
      throw std::logic_error((boost::format(
          "Unexpected reconfirm completion from FC %d during failover")
           % client_num).str());
    done_fcs_.insert(client_num);
    TrySwitchActive_();
  }

 private:
  BaseMetaServer* server_; /**< Server using the manager */
  PeriodicTimerMaker periodic_timer_maker_; /**< How to make PeriodicTimer */
  MUTEX_TYPE data_mutex_; /**< Protect data below */
  boost::shared_ptr<IPeriodicTimer> timer_; /**< Check for completion */
  bool failover_pending_; /**< Whether a failover is in progress */
  bool cleanup_pending_; /**< Whether a cleanup is needed */
  boost::unordered_set<ClientNum> done_fcs_; /**< FCs finished resending */

  bool Timeout() {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    if (cleanup_pending_) {
      GroupId num_groups = server_->topology_mgr()->num_groups();
      for (GroupId g = 0; g < num_groups; ++g)
        for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
          server_->tracker_mapper()->GetDSTracker(g, r)->
              SetExpectingFimSocket(false);
          if (server_->topology_mgr()->HasDS(g, r)
              && !server_->tracker_mapper()->GetDSFimSocket(g, r)) {
            LOG(notice, Server, "Failed to wait for DS ", GroupRoleName(g, r),
                " to re-connect during failover. Timeout occurred");
            boost::reverse_lock<boost::unique_lock<MUTEX_TYPE> > unl(lock);
            server_->conn_mgr()->CleanupDS(g, r);
          }
        }
      std::vector<ClientNum> fcs = server_->topology_mgr()->GetFCs();
      for (unsigned i = 0; i < fcs.size(); ++i)
        if (!server_->tracker_mapper()->GetFCFimSocket(fcs[i])) {
          LOG(notice, Server, "Failed to wait for FC ", PINT(fcs[i]),
              " to re-connect during failover. Timeout occurred");
          boost::reverse_lock<boost::unique_lock<MUTEX_TYPE> > unl(lock);
          boost::shared_ptr<IFimSocket> fake_sock =
              kFimSocketMaker(0, server_->asio_policy());
          server_->tracker_mapper()->SetFCFimSocket(fake_sock, fcs[i]);
          fake_sock->SetReqTracker(
              server_->tracker_mapper()->GetFCTracker(fcs[i]));
          server_->tracker_mapper()->SetFCFimSocket(
              boost::shared_ptr<IFimSocket>(), fcs[i]);
          server_->conn_mgr()->CleanupFC(fake_sock);
        }
      cleanup_pending_ = false;
    }
    TrySwitchActive_();
    return failover_pending_;
  }

  void TrySwitchActive_() {
    if (!failover_pending_)
      return;
    std::vector<ClientNum> fcs = server_->topology_mgr()->GetFCs();
    std::vector<ClientNum> waiting;
    for (unsigned i = 0; i < fcs.size(); ++i)
      if (done_fcs_.find(fcs[i]) == done_fcs_.end())
        waiting.push_back(i);
    if (!waiting.empty()) {
      std::vector<std::string> w_str;
      for (unsigned i = 0; i < waiting.size(); ++i)
        w_str.push_back((boost::format("%s") % waiting[i]).str());
      std::string msg = boost::algorithm::join(w_str, ", ");
      LOG(informational, Server,
          "Not switching to active: Still waiting for FCs [", msg.c_str(), "]");
      return;
    }
    SwitchActive_();
  }

  void SwitchActive_() {
    LOG(notice, Server, "Failover completed");
    server_->ha_counter()->ConfirmActive();
    server_->asio_policy()->Post(
        boost::bind(&IReplier::RedoCallbacks, server_->replier()));
    server_->inode_removal_tracker()->SetPersistRemoved(true);
    server_->durable_range()->SetConservative(true);
    // Setup inode allocation only after reconfirmation to avoid inode
    // number clash
    server_->inode_src()->SetupAllocation();
    server_->state_mgr()->SwitchState(kStateActive);
    IDirtyInodeMgr* dirty_inode_mgr = server_->dirty_inode_mgr();
    dirty_inode_mgr->ClearCleaning();
    server_->topology_mgr()->StartStopWorker();
    // Notify re-connected FCs
    FIM_PTR<SysStateNotifyFim> reply = SysStateNotifyFim::MakePtr();
    (*reply)->type = 'M';
    (*reply)->ready = 1;
    std::vector<ClientNum> fcs = server_->topology_mgr()->GetFCs();
    for (unsigned i = 0; i < fcs.size(); ++i)
      if (boost::shared_ptr<IFimSocket> socket =
          server_->tracker_mapper()->GetFCFimSocket(fcs[i])) {
        socket->SetFimProcessor(server_->fc_fim_processor());
        socket->WriteMsg(reply);
      }
    done_fcs_.clear();
    failover_pending_ = false;
    server_->PrepareActivate();
  }
};

}  // namespace

IFailoverMgr* MakeFailoverMgr(BaseMetaServer* server) {
  return new FailoverMgr(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
