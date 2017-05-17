/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IStartupMgr.
 */

#include "server/ms/startup_mgr_impl.hpp"

#include <stdexcept>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_policy.hpp"
#include "common.hpp"
#include "mutex_util.hpp"
#include "periodic_timer.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/startup_mgr.hpp"
#include "server/ms/stat_keeper.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/topology.hpp"
#include "server/server_info.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Amount of time between forced start checks.
 */
const double kForceStartCheckInterval = 300.0;

/**
 * Record of a DS group.
 */
struct DSGRecord {
  bool degraded; /**< Whether the DSG should be started degraded */
  GroupRole failed; /**< The DS role that has failed */
};

/**
 * Name of xattr used for storing data.
 */
const std::string kXattrName = "swait";

/**
 * Implementation of IStartupMgr.
 */
class StartupMgr : public IStartupMgr {
 public:
  /**
   * @param server The server with the manager
   */
  explicit StartupMgr(BaseMetaServer* server)
      : server_(server), data_mutex_(MUTEX_INIT), force_startable_(false) {}

  void SetPeriodicTimerMaker(PeriodicTimerMaker maker) {
    periodic_timer_maker_ = maker;
  }

  void Init();

  void Reset(const char* data);

  bool dsg_degraded(GroupId group, GroupRole* failed) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (group >= dsg_records_.size())
      return false;
    *failed = dsg_records_[group].failed;
    return dsg_records_[group].degraded;
  }

  void set_dsg_degraded(GroupId group, bool degraded, GroupRole failed) {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (group >= dsg_records_.size())
      dsg_records_.resize(group + 1);
    dsg_records_[group].degraded = degraded;
    dsg_records_[group].failed = failed;
    Save_();
  }

 private:
  BaseMetaServer* server_; /**< The server with the manager */
  PeriodicTimerMaker periodic_timer_maker_; /**< How to make periodic timers */
  MUTEX_TYPE data_mutex_; /**< Protect fields below */
  /** Initiate checks for forced startup */
  boost::shared_ptr<IPeriodicTimer> force_start_timer_;
  bool force_startable_; /**< Whether the system can be force-started */
  std::vector<DSGRecord> dsg_records_; /**< Info for each ds group */

  void Load_();
  void Save_();
  void InitCheck_();
  bool CheckForceStart();
  bool ForceStartable_();
  void ForceStart_();
};

void StartupMgr::Init() {
  MUTEX_LOCK_GUARD(data_mutex_);
  Load_();
  InitCheck_();
}

void StartupMgr::Reset(const char* data) {
  MUTEX_LOCK_GUARD(data_mutex_);
  server_->server_info()->Set(kXattrName, data);
  Load_();
  InitCheck_();
}

void StartupMgr::Load_() {
  std::string xattr = server_->server_info()->Get(kXattrName);
  if (xattr == "")
    return;
  dsg_records_.clear();
  std::vector<std::string> info;
  boost::split(info, xattr, boost::is_any_of(","));
  for (unsigned i = 0; i < info.size(); ++i) {
    std::vector<std::string> ds_info;
    boost::split(ds_info, info[i], boost::is_any_of("-"));
    if (ds_info.size() != 2)
      throw std::runtime_error("Cannot parse swait component: " + info[i]);
    try {
      GroupId group = boost::lexical_cast<GroupId>(ds_info[0]);
      if (group >= dsg_records_.size())
        dsg_records_.resize(group + 1);
      dsg_records_[group].degraded = true;
      GroupRole failed = boost::lexical_cast<GroupRole>(ds_info[1]);
      dsg_records_[group].failed = failed;
      server_->topology_mgr()->SetDSLost(group, failed);
    } catch (const boost::bad_lexical_cast&) {
      throw std::runtime_error("Cannot parse swait component: " + info[i]);
    }
  }
}

void StartupMgr::InitCheck_() {
  force_start_timer_ =
      periodic_timer_maker_(server_->asio_policy()->io_service(),
                            kForceStartCheckInterval);
  force_start_timer_->OnTimeout(
      boost::bind(&StartupMgr::CheckForceStart, this));
}

bool StartupMgr::CheckForceStart() {
  MUTEX_LOCK_GUARD(data_mutex_);
  MSState state = server_->state_mgr()->GetState();
  if (state != kStateHANotReady && state != kStateActive)
    return false;
  if (state != kStateActive)
    return true;
  if (server_->topology_mgr()->AllDSReady())
    return false;
  if (!server_->topology_mgr()->AllDSGStartable())
    return true;
  if (!force_startable_) {
    force_startable_ = true;
    return true;
  }
  if (!server_->topology_mgr()->ForceStartDSG())
    return true;
  server_->topology_mgr()->StartStopWorker();
  server_->stat_keeper()->Run();
  return false;
}

void StartupMgr::Save_() {
  std::vector<std::string> info;
  for (unsigned i = 0; i < dsg_records_.size(); ++i)
    if (dsg_records_[i].degraded)
      info.push_back((boost::format("%s-%s")
                      % i % dsg_records_[i].failed).str());
  server_->server_info()->Set(kXattrName, boost::join(info, ","));
}

}  // namespace

IStartupMgr* MakeStartupMgr(BaseMetaServer* server) {
  return new StartupMgr(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
