/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of cpfs::server::ds::IConnMgr for initializing
 * connections to MS and DSx for DS.
 */

#include "server/ds/conn_mgr_impl.hpp"

#include <stdint.h>
#include <unistd.h>

#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "connecting_socket.hpp"
#include "connector.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "time_keeper.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/conn_mgr.hpp"
#include "server/ds/store.hpp"
#include "server/thread_group.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * Connect to MS and DSx and perform connection initialization for DS.
 * Once the connections are ready, the FimSockets will be set to
 * TrackerMapper.
 */
class ConnMgr : public IConnMgr {
 public:
  /**
   * Construct the DS connection manager
   *
   * @param server The data server using the connection manager.
   */
  explicit ConnMgr(BaseDataServer* server)
      : server_(server),
        is_reconnect_(false) {}

  void Init() {
    ConfigMgr& configs = server_->configs();
    meta_servers_.push_back(
        std::make_pair(configs.ms1_host(), configs.ms1_port()));
    if (!configs.ms2_host().empty())
      meta_servers_.push_back(
          std::make_pair(configs.ms2_host(), configs.ms2_port()));
    for (unsigned i = 0; i < meta_servers_.size(); ++i) {
      active_.push_back(true);
      ConnectMS(i);
      reconn_timers_.push_back(server_->asio_policy()->MakeDeadlineTimer());
    }
  }

  void DisconnectMS(boost::shared_ptr<IFimSocket> socket) {
    unsigned index = ms_socket_map_[socket];
    ms_socket_map_.erase(socket);
    socket->Shutdown();
    if (ShouldReconnectMS()) {
      server_->asio_policy()->SetDeadlineTimer(
          &reconn_timers_[index],
          5.0,
          boost::bind(&ConnMgr::ConnectMS, this, index));
    } else {
      active_[index] = false;
    }
  }

  void ConnectDS(uint32_t ip, uint16_t port, GroupId group, GroupRole role) {
    if (server_->tracker_mapper()->GetDSFimSocket(group, role)) {
      LOG(informational, Server,
          "Already connected to DS ", GroupRoleName(group, role));
    } else {
      LOG(notice, Server, "Connecting to DS ", GroupRoleName(group, role));
      server_->connector()->AsyncConnect(
          IntToIP(ip), port,
          server_->ds_fim_processor(),
          boost::bind(&ConnMgr::DSConnected, this, _1, group, role), false,
          0, DefaultConnectErrorHandler, server_->ds_asio_policy());
    }
  }

  void ReconnectMS(boost::shared_ptr<IFimSocket> socket) {
    LOG(warning, Server, "MS disconnected");
    unsigned index = ms_socket_map_[socket];
    active_[index] = false;
    ms_socket_map_.clear();
    // Unlock all inodes
    FIM_PTR<DSInodeLockFim> fim = DSInodeLockFim::MakePtr();
    (*fim)->inode = 0;
    (*fim)->lock = 2;
    server_->thread_group()->EnqueueAll(fim);
    unsigned num_servers = meta_servers_.size();
    if (num_servers > 1) {
      unsigned standby = (index + 1) % num_servers;
      LOG(notice, Server, "Connecting to stand-by MS: ", server_info(standby));
      is_reconnect_ = true;
      if (!active_[standby]) {
        active_[standby] = true;
        ConnectMS(standby);
      } else {
        LOG(informational, Server, "Already attempting MS conn, not repeated");
      }
    }
  }

 private:
  BaseDataServer* server_; /**< The data server */
  /** MS servers */
  std::vector<std::pair<std::string, int> > meta_servers_;
  /** Mapping meta server FimSocket to index (index to meta_servers_) */
  boost::unordered_map<boost::shared_ptr<IFimSocket>, unsigned> ms_socket_map_;
  /** Whether manager has re-connected to MS */
  bool is_reconnect_;
  /** Whether each server is waiting for connection or already connected */
  std::vector<bool> active_;
  /** Timers for MS reconnections */
  boost::ptr_vector<DeadlineTimer> reconn_timers_;

  /**
   * Callback to invoke when connection to MS1 is made.
   */
  void MSConnected(boost::shared_ptr<IFimSocket> fim_socket, unsigned index) {
    ms_socket_map_[fim_socket] = index;
    FIM_PTR<DSMSRegFim> fim = DSMSRegFim::MakePtr();
    std::strncpy((*fim)->uuid, server_->store()->GetUUID().c_str(), 36U);
    (*fim)->uuid[36] = '\0';
    (*fim)->is_grouped = server_->store()->is_role_set();
    (*fim)->ds_group = server_->store()->ds_group();
    (*fim)->ds_role = server_->store()->ds_role();
    (*fim)->opt_resync =
        bool(server_->dsg_ready_time_keeper()->GetLastUpdate());
    (*fim)->distressed = server_->distressed();
    (*fim)->ip = IPToInt(server_->configs().ds_host());
    (*fim)->port = server_->configs().ds_port();
    (*fim)->pid = getpid();
    (*fim)->is_reconnect = is_reconnect_;
    fim_socket->WriteMsg(fim);
  }

  /**
   * Callback to invoke when connection to DSx is made.
   *
   * @param socket The socket to the other DS
   *
   * @praam group The group ID of the other DS
   *
   * @param role The role of the other DS
   */
  void DSConnected(boost::shared_ptr<IFimSocket> socket,
                   GroupId group, GroupRole role) {
    LOG(notice, Server, "Connected to DS ", GroupRoleName(group, role));
    ITrackerMapper* tracker_mapper = server_->tracker_mapper();
    FIM_PTR<DSDSRegFim> fim = DSDSRegFim::MakePtr();
    // Send current DS info
    (*fim)->ds_group = server_->store()->ds_group();
    (*fim)->ds_role = server_->store()->ds_role();
    socket->WriteMsg(fim);
    // Must set after WriteMsg to ensure that the tracker won't get
    // plugged too early: the other end will only set the DS tracker
    // after processing the DSDSRegFim above, and any request sent
    // before that would be dropped.
    tracker_mapper->SetDSFimSocket(socket, group, role);
  }

  /**
   * Connect to meta server at specific index
   *
   * @param index Index to meta servers
   */
  void ConnectMS(unsigned index) {
    std::string host = meta_servers_[index].first;
    int port = meta_servers_[index].second;
    server_->connector()->AsyncConnect(
        host, port, server_->ms_fim_processor(),
        boost::bind(&ConnMgr::MSConnected, this, _1, index),
        true,
        5.0,
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
    if (ShouldReconnectMS())
      return 5.0;
    active_[index] = false;
    return -1.0;
  }

  /**
   * Get server ip:port
   */
  std::string server_info(unsigned index) const {
    return (boost::format("%s:%d") % meta_servers_[index].first %
            meta_servers_[index].second).str();
  }

  bool ShouldReconnectMS() {
    return !server_->tracker_mapper()->GetMSFimSocket() &&
           !server_->IsShuttingDown();
  }
};

}  // namespace

IConnMgr* MakeConnMgr(BaseDataServer* server) {
  return new ConnMgr(server);
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
