/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of AdminFimProcessor.
 */
#include "admin_fim_processor.hpp"

#include <stdint.h>
#include <stdlib.h>

#include <cstddef>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/iterator/iterator_traits.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>

#include "admin_info.hpp"
#include "base_ms.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "ds_query_mgr.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "req_entry.hpp"
#include "req_tracker.hpp"
#include "shutdown_mgr.hpp"
#include "state_mgr.hpp"
#include "store.hpp"
#include "topology.hpp"
#include "tracker_mapper.hpp"
#include "server/worker_util.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implement the AdminFimProcessor
 */
class AdminFimProcessor : public MemberFimProcessor<AdminFimProcessor> {
 public:
  /**
   * Create the AdminFimProcessor
   *
   * @param meta_server The meta server
   */
  explicit AdminFimProcessor(BaseMetaServer* meta_server)
      : server_(meta_server) {
    AddHandler(&AdminFimProcessor::HandleStatFS);
    AddHandler(&AdminFimProcessor::HandleDSGStateChangeReady);
    AddHandler(&AdminFimProcessor::HandleClusterStatus);
    AddHandler(&AdminFimProcessor::HandleClusterDiskInfo);
    AddHandler(&AdminFimProcessor::HandleConfigList);
    AddHandler(&AdminFimProcessor::HandleConfigChange);
    AddHandler(&AdminFimProcessor::HandleSystemShutdown);
  }

 private:
  BaseMetaServer* server_; /**< The server with the DSQueryMgr */

  bool HandleStatFS(const FIM_PTR<FCStatFSFim>& fim,
                    const boost::shared_ptr<IFimSocket>& sender);

  bool HandleDSGStateChangeReady(const FIM_PTR<DSGStateChangeReadyFim>& fim,
                                 const boost::shared_ptr<IFimSocket>& sender);

  bool HandleClusterStatus(const FIM_PTR<ClusterStatusReqFim>& fim,
                           const boost::shared_ptr<IFimSocket>& sender);

  bool HandleClusterDiskInfo(const FIM_PTR<ClusterDiskInfoReqFim>& fim,
                             const boost::shared_ptr<IFimSocket>& sender);

  bool HandleConfigList(const FIM_PTR<ClusterConfigListReqFim>& fim,
                        const boost::shared_ptr<IFimSocket>& sender);

  bool HandleConfigChange(const FIM_PTR<ClusterConfigChangeReqFim>& fim,
                          const boost::shared_ptr<IFimSocket>& sender);

  bool HandleSystemShutdown(const FIM_PTR<ClusterShutdownReqFim>& fim,
                            const boost::shared_ptr<IFimSocket>& sender);


  void AddConfigsFromFim(std::vector<ConfigItem>* ret,
                         const FIM_PTR<IFim>& fim);

  bool ChangePeerConfig(const std::string& target,
                        const std::string& cfg_name,
                        const std::string& new_value);
};

bool AdminFimProcessor::HandleStatFS(
    const FIM_PTR<FCStatFSFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  ReplyOnExit r(fim, sender);
  std::vector<GroupId> group_ids;
  for (GroupId g = 0; g < server_->topology_mgr()->num_groups(); ++g)
    group_ids.push_back(g);
  DSReplies all_replies = server_->ds_query_mgr()->Request(
      MSStatFSFim::MakePtr(), group_ids);
  uint64_t total_space = 0;
  uint64_t free_space = 0;
  for (DSReplies::iterator itr = all_replies.begin();
       itr != all_replies.end(); ++itr) {
    std::map<GroupRole, FIM_PTR<IFim> >::const_iterator
        ds_itr = itr->second.begin();
    uint64_t dsg_total_space = 0;
    uint64_t dsg_free_space = 0;
    for (; ds_itr != itr->second.end(); ++ds_itr) {
      const FCStatFSReplyFim& rfim =
          static_cast<FCStatFSReplyFim&>(*(ds_itr->second));
      dsg_total_space += rfim->total_space;
      dsg_free_space += rfim->free_space;
    }
    dsg_total_space *= ((kNumDSPerGroup - 1) / double(itr->second.size()));
    dsg_free_space *= ((kNumDSPerGroup - 1) / double(itr->second.size()));
    total_space += dsg_total_space;
    free_space += dsg_free_space;
  }
  uint64_t total_inodes = 0;
  uint64_t free_inodes = 0;
  int ret = server_->store()->Stat(&total_inodes, &free_inodes);
  if (ret != 0) {
    LOG(error, Server, "Failed to get inode statistics while handling statfs");
    r.SetResult(ret);
    return true;
  }
  FIM_PTR<FCStatFSReplyFim> reply = FCStatFSReplyFim::MakePtr();
  (*reply)->total_space = total_space;
  (*reply)->free_space  = free_space;
  (*reply)->total_inodes = total_inodes;
  (*reply)->free_inodes = free_inodes;
  r.SetNormalReply(reply);
  return true;
}

bool AdminFimProcessor::HandleDSGStateChangeReady(
    const FIM_PTR<DSGStateChangeReadyFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  ClientNum peer_num = sender->GetReqTracker()->peer_client_num();
  server_->topology_mgr()->AckDSGStateChangeWait(peer_num);
  return true;
}

bool AdminFimProcessor::HandleClusterStatus(
    const FIM_PTR<ClusterStatusReqFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  ReplyOnExit r(fim, sender);
  // Combined FC and DS node infos
  std::vector<NodeInfo> ret;
  std::vector<NodeInfo> ds_infos = server_->topology_mgr()->GetDSInfos();
  std::vector<NodeInfo> fc_infos = server_->topology_mgr()->GetFCInfos();
  ret.insert(ret.end(), ds_infos.begin(), ds_infos.end());
  ret.insert(ret.end(), fc_infos.begin(), fc_infos.end());
  // DS group states
  std::size_t num_dsg = server_->topology_mgr()->num_groups();
  std::vector<uint8_t> dsg_states;
  for (GroupId group = 0; group < num_dsg; ++group) {
    GroupRole failed;
    dsg_states.push_back(server_->topology_mgr()->GetDSGState(group, &failed));
  }
  std::size_t dsg_state_size = num_dsg * sizeof(dsg_states[0]);
  std::size_t node_info_size = ret.size() * sizeof(NodeInfo);
  FIM_PTR<ClusterStatusReplyFim> reply =
      ClusterStatusReplyFim::MakePtr(dsg_state_size + node_info_size);
  (*reply)->ms_state = server_->state_mgr()->GetState();
  if ((*reply)->ms_state == kStateActive &&
      !server_->tracker_mapper()->GetMSFimSocket())
    (*reply)->ms_state = kStateDegraded;
  (*reply)->ms_role =
      atoi(server_->configs().role().substr(2).c_str());
  (*reply)->num_dsg = num_dsg;
  (*reply)->num_node_info = ret.size();
  std::memcpy(reply->tail_buf(), dsg_states.data(), dsg_state_size);
  std::memcpy(reply->tail_buf() + dsg_state_size, ret.data(), node_info_size);
  r.SetNormalReply(reply);
  return true;
}

bool AdminFimProcessor::HandleClusterDiskInfo(
    const FIM_PTR<ClusterDiskInfoReqFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  ReplyOnExit r(fim, sender);
  std::vector<GroupId> group_ids;
  for (GroupId g = 0; g < server_->topology_mgr()->num_groups(); ++g)
    group_ids.push_back(g);
  DSReplies all_replies = server_->ds_query_mgr()->Request(
      MSStatFSFim::MakePtr(), group_ids);
  std::vector<DSDiskInfo> cluster_disk_infos;
  for (DSReplies::iterator itr = all_replies.begin();
       itr != all_replies.end(); ++itr) {
    GroupId group_id = itr->first;
    std::map<GroupRole, FIM_PTR<IFim> >& replies = itr->second;
    for (GroupRole role = 0; role < kNumDSPerGroup; ++role) {
      if (replies.find(role) == replies.end())
        continue;
      DSDiskInfo disk_info;
      const FCStatFSReplyFim& rfim =
          static_cast<FCStatFSReplyFim&>(*replies[role]);
      disk_info.SetAlias(group_id, role);
      disk_info.total = rfim->total_space;
      disk_info.free = rfim->free_space;
      cluster_disk_infos.push_back(disk_info);
    }
  }
  std::size_t disk_info_size = cluster_disk_infos.size() * sizeof(DSDiskInfo);
  FIM_PTR<ClusterDiskInfoReplyFim> reply
      = ClusterDiskInfoReplyFim::MakePtr(disk_info_size);
  std::memcpy(reply->tail_buf(), cluster_disk_infos.data(), disk_info_size);
  r.SetNormalReply(reply);
  return true;
}

void AdminFimProcessor::AddConfigsFromFim(
    std::vector<ConfigItem>* ret,
    const FIM_PTR<IFim>& fim) {
  const ClusterConfigListReplyFim& rreply =
      static_cast<ClusterConfigListReplyFim&>(*fim);
  std::size_t num_configs = rreply.tail_buf_size() / sizeof(ConfigItem);
  const ConfigItem* configs_raw =
      reinterpret_cast<const ConfigItem*>(rreply.tail_buf());
  ret->insert(ret->end(), configs_raw, configs_raw + num_configs);
}

bool AdminFimProcessor::HandleConfigList(
    const FIM_PTR<ClusterConfigListReqFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  ReplyOnExit r(fim, sender);
  GroupId num_groups = server_->topology_mgr()->num_groups();
  // List configs from self
  std::vector<ConfigItem> ret;
  ret.push_back(ConfigItem(kNodeCfg, server_->configs().role()));
  ret.push_back(ConfigItem("log_severity", server_->configs().log_severity()));
  ret.push_back(ConfigItem("log_path", server_->configs().log_path()));
  if (server_->configs().role().substr(0, 2) == "MS")
    ret.push_back(ConfigItem("num_ds_groups",
        boost::lexical_cast<std::string>(num_groups)));
  // List configs from peer MS
  boost::shared_ptr<IReqTracker> ms_tracker =
      server_->tracker_mapper()->GetMSTracker();
  if (ms_tracker->GetFimSocket()) {
    FIM_PTR<IFim> ms_reply = ms_tracker->AddRequest(
        ClusterConfigListReqFim::MakePtr())->WaitReply();
    std::string peer_role =
        (server_->configs().role() == "MS1") ? "MS2" : "MS1";
    ret.push_back(ConfigItem(kNodeCfg, peer_role));
    AddConfigsFromFim(&ret, ms_reply);
  }
  // List configs from peer DS
  std::vector<GroupId> group_ids;
  for (GroupId g = 0; g < num_groups; ++g)
    group_ids.push_back(g);
  DSReplies all_replies = server_->ds_query_mgr()->Request(
      ClusterConfigListReqFim::MakePtr(), group_ids);
  for (DSReplies::iterator itr = all_replies.begin();
       itr != all_replies.end(); ++itr) {
    std::map<GroupRole, FIM_PTR<IFim> >::const_iterator
        ds_itr = itr->second.begin();
    for (; ds_itr != itr->second.end(); ++ds_itr) {
      GroupId group = itr->first;
      GroupRole role = ds_itr->first;
      ret.push_back(ConfigItem(kNodeCfg, "DS " + GroupRoleName(group, role)));
      AddConfigsFromFim(&ret, ds_itr->second);
    }
  }
  std::size_t config_info_size = ret.size() * sizeof(ConfigItem);
  FIM_PTR<ClusterConfigListReplyFim> reply
      = ClusterConfigListReplyFim::MakePtr(config_info_size);
  std::memcpy(reply->tail_buf(), ret.data(), config_info_size);
  r.SetNormalReply(reply);
  return true;
}

bool AdminFimProcessor::HandleConfigChange(
    const FIM_PTR<ClusterConfigChangeReqFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  ReplyOnExit r(fim, sender);
  std::string target = (*fim)->target;
  std::string name = (*fim)->name;
  std::string value = (*fim)->value;
  if (target == server_->configs().role()) {
    if (name == "log_severity") {
      server_->configs().set_log_severity(value);
    } else if (name == "num_ds_groups") {
      server_->topology_mgr()->set_num_groups(boost::lexical_cast<int>(value));
    } else if (name == "log_path") {
      server_->configs().set_log_path(value);
    } else {
      LOG(warning, Server, "Unknown configuration: ", name, " change failed");
      return true;
    }
  } else {
    if (!ChangePeerConfig(target, name, value)) {
      LOG(warning, Server, "Failed to change configuration: ", name);
      return true;
    }
  }
  r.SetNormalReply(ClusterConfigChangeReplyFim::MakePtr());
  return true;
}

bool AdminFimProcessor::ChangePeerConfig(
    const std::string& target,
    const std::string& cfg_name,
    const std::string& new_value) {
  boost::shared_ptr<IReqTracker> tracker;
  // TODO(Isaac): Refactor this parsing code outside the processor
  if (target.substr(0, 2) == "MS") {
    tracker = server_->tracker_mapper()->GetMSTracker();
  } else if (target.substr(0, 2) == "DS") {
    std::string group_role_str = target.substr(2);
    boost::erase_all(group_role_str, " ");
    std::vector<std::string> group_role;
    boost::split(group_role, group_role_str, boost::is_any_of("-"));
    GroupId group = boost::lexical_cast<GroupId>(group_role[0]);
    GroupRole role = boost::lexical_cast<GroupRole>(group_role[1]);
    tracker = server_->tracker_mapper()->GetDSTracker(group, role);
  }
  if (tracker) {
    FIM_PTR<PeerConfigChangeReqFim> cc_fim =
        PeerConfigChangeReqFim::MakePtr();
    strncpy((*cc_fim)->name, cfg_name.c_str(), cfg_name.length() + 1);
    strncpy((*cc_fim)->value, new_value.c_str(), new_value.length() + 1);
    boost::shared_ptr<IReqEntry> entry = tracker->AddRequest(cc_fim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    ResultCodeReplyFim& r_reply = static_cast<ResultCodeReplyFim&>(*reply);
    return r_reply->err_no == 0;
  } else {
    return false;
  }
}

bool AdminFimProcessor::HandleSystemShutdown(
    const FIM_PTR<ClusterShutdownReqFim>& fim,
    const boost::shared_ptr<IFimSocket>& sender) {
  ReplyOnExit r(fim, sender);
  server_->shutdown_mgr()->Init();
  r.SetNormalReply(ClusterShutdownReplyFim::MakePtr());
  return true;
}

}  // namespace

IFimProcessor* MakeAdminFimProcessor(BaseMetaServer* meta_server) {
  return new AdminFimProcessor(meta_server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
