/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of CpfsAdmin for cluster administration.
 */
#include "client/cpfs_admin.hpp"

#include <stdint.h>

#include <cstddef>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>

#include "admin_info.hpp"
#include "common.hpp"
#include "conn_mgr.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "io_service_runner.hpp"
#include "req_entry.hpp"
#include "req_tracker.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "client/base_client.hpp"

namespace cpfs {
namespace client {
namespace {

/**
 * Define the CpfsAdmin
 */
class CpfsAdmin : public ICpfsAdmin {
 public:
  /**
   * Create the CpfsAdmin
   *
   * @param client The base admin client
   */
  explicit CpfsAdmin(BaseAdminClient* client)
      : client_(client), inited_(false) {}

  bool Init(double timeout, std::vector<bool>* reject_info) {
    if (!inited_) {
      client_->service_runner()->Run();
      inited_ = true;
    }
    bool is_ready = client_->WaitReady(timeout);
    *reject_info = client_->conn_mgr()->GetMSRejectInfo();
    return is_ready;
  }

  ClusterInfo QueryStatus() {
    ClusterInfo cluster_info;
    {  // Avoid coverage false positive
      FIM_PTR<ClusterStatusReqFim> rfim = ClusterStatusReqFim::MakePtr();
      boost::shared_ptr<IReqEntry> entry
          = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
      const FIM_PTR<IFim>& reply = entry->WaitReply();
      const ClusterStatusReplyFim& rreply =
          static_cast<ClusterStatusReplyFim&>(*reply);
      cluster_info.ms_state = ToStr(MSState(rreply->ms_state));
      if (rreply->state_changing)
        cluster_info.ms_state += " (DSG state changing)";
      cluster_info.ms_role =
          (boost::format("MS%d") % int(rreply->ms_role)).str();
      const uint8_t* dsg_states_raw =
        reinterpret_cast<const uint8_t*>(rreply.tail_buf());
      std::vector<uint8_t> dsg_states(
          dsg_states_raw, dsg_states_raw + rreply->num_dsg);
      for (std::size_t n = 0; n < rreply->num_dsg; ++n)
        cluster_info.dsg_states.push_back(ToStr(DSGroupState(dsg_states[n])));
      std::size_t node_info_size = rreply->num_node_info * sizeof(NodeInfo);
      std::vector<NodeInfo> infos_vec(rreply->num_node_info);
      std::memcpy(&infos_vec.data()[0],
                  rreply.tail_buf() + rreply->num_dsg, node_info_size);
      for (std::size_t n = 0; n < infos_vec.size(); ++n) {
        const NodeInfo& info = infos_vec[n];
        std::map<std::string, std::string> info_attrs;
        info_attrs["IP"] = IntToIP(info.ip);
        info_attrs["Port"] = boost::lexical_cast<std::string>(info.port);
        info_attrs["PID"] = boost::lexical_cast<std::string>(info.pid);
        info_attrs["Alias"] = info.alias;
        cluster_info.node_infos.push_back(info_attrs);
      }
    }
    return cluster_info;
  }

  DiskInfoList QueryDiskInfo() {
    DiskInfoList ret;
    {  // Avoid coverage false positive
      FIM_PTR<ClusterDiskInfoReqFim> rfim = ClusterDiskInfoReqFim::MakePtr();
      boost::shared_ptr<IReqEntry> entry
          = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
      const FIM_PTR<IFim>& reply = entry->WaitReply();
      const ClusterDiskInfoReplyFim& rreply =
          static_cast<ClusterDiskInfoReplyFim&>(*reply);
      std::size_t num_ds_disk_info =
          rreply.tail_buf_size() / sizeof(DSDiskInfo);
      const DSDiskInfo* ds_disk_infos_raw =
          reinterpret_cast<const DSDiskInfo*>(rreply.tail_buf());
      std::vector<DSDiskInfo> ds_disk_infos(
          ds_disk_infos_raw, ds_disk_infos_raw + num_ds_disk_info);
      for (std::size_t n = 0; n < ds_disk_infos.size(); ++n) {
        std::map<std::string, std::string> info_attrs;
        info_attrs["Alias"] = ds_disk_infos[n].alias;
        info_attrs["Total"] = FormatDiskSize(ds_disk_infos[n].total);
        info_attrs["Free"] = FormatDiskSize(ds_disk_infos[n].free);
        ret.push_back(info_attrs);
      }
    }
    return ret;
  }

  ConfigList ListConfig() {
    ConfigList ret;
    {  // Avoid coverage false positive
      FIM_PTR<ClusterConfigListReqFim> rfim =
          ClusterConfigListReqFim::MakePtr();
      boost::shared_ptr<IReqEntry> entry
          = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
      const FIM_PTR<IFim>& reply = entry->WaitReply();
      const ClusterConfigListReplyFim& rreply =
          static_cast<ClusterConfigListReplyFim&>(*reply);
      std::size_t num_configs = rreply.tail_buf_size() / sizeof(ConfigItem);
      const ConfigItem* configs_raw =
          reinterpret_cast<const ConfigItem*>(rreply.tail_buf());
      std::vector<ConfigItem> configs(configs_raw, configs_raw + num_configs);
      for (std::size_t n = 0; n < configs.size(); ++n)
        ret.push_back(std::make_pair(configs[n].name, configs[n].value));
    }
    return ret;
  }

  bool ChangeConfig(const std::string& target,
                    const std::string& name,
                    const std::string& value) {
    // TODO(Joseph): Check string length boundary
    FIM_PTR<ClusterConfigChangeReqFim> rfim =
        ClusterConfigChangeReqFim::MakePtr();
    strncpy((*rfim)->target, target.c_str(), target.length() + 1);
    strncpy((*rfim)->name, name.c_str(), name.length() + 1);
    strncpy((*rfim)->value, value.c_str(), value.length() + 1);
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    return (reply->type() == kClusterConfigChangeReplyFim);
  }

  bool SystemShutdown() {
    FIM_PTR<ClusterShutdownReqFim> rfim = ClusterShutdownReqFim::MakePtr();
    boost::shared_ptr<IReqEntry> entry
        = client_->tracker_mapper()->GetMSTracker()->AddRequest(rfim);
    const FIM_PTR<IFim>& reply = entry->WaitReply();
    return (reply->type() == kClusterShutdownReplyFim);
  }

  bool ForceStart(unsigned server_index) {
    client_->conn_mgr()->SetForceStartMS(server_index);
    return true;
  }

 private:
  BaseAdminClient* client_;
  bool inited_;
};

}  // namespace

ICpfsAdmin* MakeCpfsAdmin(BaseAdminClient* client) {
  return new CpfsAdmin(client);
}

}  // namespace client
}  // namespace cpfs
