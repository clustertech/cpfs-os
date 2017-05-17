/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of cpfs::client::IConnMgr for initializing
 * connections in FC.
 */

#include "client/conn_mgr_impl.hpp"

#include <stdint.h>
#include <unistd.h>

#include <limits>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "asio_policy_impl.hpp"
#include "common.hpp"
#include "connector.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "req_tracker.hpp"
#include "service.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "client/base_client.hpp"
#include "client/conn_mgr.hpp"

namespace cpfs {
namespace client {
namespace {

/**
 * Simple structure holding MS addresses to connect
 */
struct HostPort {
  std::string full;  /**< The host-port specification in full */
  std::string ip;  /**< The host part */
  int port;  /**< The port part */

  /**
   * @param full The full host:port specification
   *
   * @param policy The policy to resolve the host to IP
   */
  HostPort(std::string full, IAsioPolicy* policy) {
    this->full = full;
    policy->ParseHostPort(full, &ip, &port);
  }
};

/**
 * Connect to all servers (MS1, DS1, DS2, etc) and perform connection
 * initialization. The target addresses of DSx are retrieved from MS1.
 * Once the connections are ready, the FimSockets will be set to ReqMapper.
 */
class ConnMgr : public IConnMgr {
 public:
  /**
   * @param client The client using the manager
   *
   * @param type The type of connection to make: 'F' for FC and 'A' for Admin
   */
  explicit ConnMgr(BaseClient* client, char type)
      : client_(client),
        reconnecting_(false),
        force_start_index_(std::numeric_limits<unsigned>::max()),
        type_(type),
        init_completed_(false),
        init_conn_retry_(kDefaultInitConnRetry),
        init_conn_retry_cnt_(0) {}

  void Init(const std::vector<std::string>& meta_servers) {
    boost::scoped_ptr<IAsioPolicy> policy(MakeAsioPolicy());
    for (unsigned i = 0; i < meta_servers.size(); ++i)
      meta_servers_.push_back(HostPort(meta_servers[i], policy.get()));
    for (unsigned i = 0; i < meta_servers_.size(); ++i) {
      LOG(notice, Server, "Connecting to MS ", meta_servers_[i].full);
      active_.push_back(true);
      rejected_.push_back(false);
      ConnectMS(i);
      reconn_timers_.push_back(client_->asio_policy()->MakeDeadlineTimer());
    }
  }

  void ForgetMS(boost::shared_ptr<IFimSocket> socket, bool rejected) {
    unsigned index = ms_socket_map_[socket];
    rejected_[index] = rejected;
    ms_socket_map_.erase(socket);
    if (ShouldReconnectMS()) {
      client_->asio_policy()->SetDeadlineTimer(
          &reconn_timers_[index],
          5.0,
          boost::bind(&ConnMgr::ConnectMS, this, index));
    } else {
      active_[index] = false;
    }
  }

  std::vector<bool> GetMSRejectInfo() const {
    return rejected_;
  }

  void ReconnectMS(boost::shared_ptr<IFimSocket> socket) {
    unsigned index = ms_socket_map_[socket];
    ms_socket_map_.clear();
    unsigned num_servers = meta_servers_.size();
    if (num_servers > 1) {
      active_[index] = false;
      unsigned standby = (index + 1) % num_servers;
      LOG(notice, Server, "Connecting to standby MS ",
          meta_servers_[standby].full);
      reconnecting_ = true;
      if (!active_[standby]) {
        active_[standby] = true;
        ConnectMS(standby);
      } else {
        LOG(informational, Server, "Already attempting MS conn, not repeated");
      }
    }
  }

  bool IsReconnectingMS() const {
    return reconnecting_;
  }

  void SetReconnectingMS(bool reconnecting) {
    reconnecting_ = reconnecting;
  }

  void SetForceStartMS(unsigned server_index) {
    force_start_index_ = server_index;
  }

  void SetInitConnRetry(unsigned init_conn_retry) {
    init_conn_retry_ = init_conn_retry;
  }

  void ConnectDS(uint32_t ip, uint16_t port, GroupId group, GroupRole role) {
    // Connect to DSx
    if (client_->tracker_mapper()->GetDSFimSocket(group, role)) {
      LOG(informational, Server,
          "Already connected to DS ", GroupRoleName(group, role));
    } else {
      LOG(informational, Server,
          "Connecting to DS ", GroupRoleName(group, role));
      // Connect to DSx
      client_->connector()->AsyncConnect(
          IntToIP(ip), port, client_->ds_fim_processor(),
          boost::bind(&ConnMgr::DSConnected, this, _1, group, role), false);
    }
  }

 protected:
  BaseClient* client_; /**< The client using the manager */
  std::vector<HostPort> meta_servers_; /**< Meta servers */
  /** Mapping meta server FimSocket to index (index to meta_servers_) */
  boost::unordered_map<boost::shared_ptr<IFimSocket>, unsigned> ms_socket_map_;
  bool reconnecting_; /**< Whether failover occured */
  unsigned force_start_index_; /**< Index of meta server to force start */
  /** Whether each MS server is rejected */
  std::vector<bool> rejected_;
  /** Whether each server is waiting for connection or already connected */
  std::vector<bool> active_;
  /** Timers for MS reconnections */
  boost::ptr_vector<DeadlineTimer> reconn_timers_;
  char type_; /**< The client type */
  bool init_completed_; /**< Whether init conn to MS has been done */
  unsigned init_conn_retry_; /**< Max # of init conn retry */
  unsigned init_conn_retry_cnt_; /**< # of init conn retry to MS(s) attempted */

  /**
   * Callback to invoke when connection to MS is made
   */
  void MSConnected(boost::shared_ptr<IFimSocket> fim_socket, unsigned index) {
    if (!init_completed_)
      init_completed_ = true;
    ms_socket_map_[fim_socket] = index;
    FIM_PTR<FCMSRegFim> fim = FCMSRegFim::MakePtr();
    (*fim)->is_reconnect = reconnecting_ ? '\x1' : '\x0';
    (*fim)->client_num = client_->client_num();
    (*fim)->type = type_;
    (*fim)->pid = getpid();
    (*fim)->force_start = (force_start_index_ == index) ? '\x1' : '\x0';
    fim_socket->WriteMsg(fim);
  }

  /**
   * Callback to invoke when connection to DSx is made
   */
  void DSConnected(boost::shared_ptr<IFimSocket> socket,
                   GroupId group, GroupRole role) {
    LOG(notice, Server, "Connected to DS ", GroupRoleName(group, role));
    ITrackerMapper* tracker_mapper = client_->tracker_mapper();
    tracker_mapper->SetDSFimSocket(socket, group, role, false);
    FIM_PTR<FCDSRegFim> fim = FCDSRegFim::MakePtr();
    (*fim)->client_num = client_->client_num();
    socket->WriteMsg(fim);
    tracker_mapper->GetDSTracker(group, role)->Plug();
  }

  /**
   * Connect to meta server at specific index
   *
   * @param index Index to meta servers
   */
  void ConnectMS(unsigned index) {
    client_->connector()->AsyncConnect(
        meta_servers_[index].ip, meta_servers_[index].port,
        client_->ms_fim_processor(),
        boost::bind(&ConnMgr::MSConnected, this, _1, index),
        true, 5.0,
        boost::bind(&ConnMgr::MSConnectFailed, this, index));
  }

  /**
   * Connect error handler.
   *
   * @param index Index to meta servers
   *
   * @return The delay before reconnecting
   */
  double MSConnectFailed(unsigned index) {
    if (ShouldReconnectMS()) {
      if (init_completed_ ||
          ++init_conn_retry_cnt_ <= init_conn_retry_ * meta_servers_.size())
        return 5.0;
      LOG(error, Server, "Failed connecting to MS. Aborting");
      // TODO(Joseph): Alternative for unblocking ReqIdGenerator::GenReqId()
      client_->tracker_mapper()->SetClientNum(0);
      client_->tracker_mapper()->Shutdown();
      client_->runner()->Shutdown();
    }
    active_[index] = false;
    return -1.0;
  }

  /**
   * Whether MS reconnection should be done again.  This returns true
   * if the client is not already shutting down, and an active MS
   * connection is not yet obtained.
   */
  bool ShouldReconnectMS() {
    return !client_->IsShuttingDown() &&
        !client_->tracker_mapper()->GetMSFimSocket();
  }
};

}  // namespace

IConnMgr* MakeConnMgr(BaseClient* client, char type) {
  return new ConnMgr(client, type);
}

}  // namespace client
}  // namespace cpfs
