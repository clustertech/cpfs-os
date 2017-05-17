/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the ITrackerMapper interface.
 */

#include "tracker_mapper_impl.hpp"

#include <stdint.h>

#include <list>
#include <string>
#include <utility>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_tracker.hpp"
#include "req_tracker_impl.hpp"
#include "tracker_mapper.hpp"

namespace cpfs {
namespace {

/**
 * Type used for DS request trackers
 */
typedef boost::unordered_map<std::pair<GroupId, GroupRole>,
                             boost::shared_ptr<IReqTracker> > DSReqTrackerList;
/**
 * Type used for mapping DS Fim sockets to group ids and roles
 */
typedef boost::unordered_map<boost::shared_ptr<IFimSocket>,
                             std::pair<GroupId, GroupRole> > DSFimSocketList;

/**
 * Type used for FC request trackers
 */
typedef boost::unordered_map<ClientNum,
                             boost::shared_ptr<IReqTracker> > FCReqTrackerList;

/**
 * Type used for Admin request trackers
 */
typedef boost::unordered_map<
    ClientNum, boost::shared_ptr<IReqTracker> > AdminReqTrackerList;

/**
 * Type used for trackers
 */
typedef std::list<boost::shared_ptr<IReqTracker> > TrackerList;

/**
 * Implement the ITrackerMapper interface.
 */
class TrackerMapper : public ITrackerMapper {
 public:
  /**
   * @param policy The Asio policy to be used for tracker creation
   */
  explicit TrackerMapper(IAsioPolicy* policy)
      : asio_policy_(policy), data_mutex_(MUTEX_INIT),
        client_num_(kUnknownClient),
        ms_tracker_(MakeReqTracker("MS", policy, &req_id_gen_)) {}
  ~TrackerMapper();
  void Reset();
  void SetClientNum(ClientNum client_num);
  void SetLimiterMaxSend(uint64_t limiter_max_send);
  boost::shared_ptr<IReqTracker> GetMSTracker();
  boost::shared_ptr<IFimSocket> GetMSFimSocket();
  void SetMSFimSocket(boost::shared_ptr<IFimSocket> fim_socket, bool plug);
  boost::shared_ptr<IReqTracker> GetDSTracker(
      GroupId group_id, GroupRole role_id);
  boost::shared_ptr<IFimSocket> GetDSFimSocket(GroupId group_id,
                                               GroupRole role_id);
  void SetDSFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                      GroupId group_id, GroupRole role_id, bool plug);
  bool FindDSRole(boost::shared_ptr<IFimSocket> fim_socket,
                  GroupId* group_id_ret, GroupRole* role_id_ret);
  boost::shared_ptr<IReqTracker> GetFCTracker(ClientNum client_num);
  boost::shared_ptr<IFimSocket> GetFCFimSocket(ClientNum client_num);
  void SetFCFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                      ClientNum client_num, bool plug);
  void DSGBroadcast(GroupId group, FIM_PTR<IFim> fim,
                    GroupRole except);
  void FCBroadcast(FIM_PTR<IFim> fim);
  void Plug(bool plug);
  bool HasPendingWrite() const;
  void DumpPendingRequests() const;
  void Shutdown();
  boost::shared_ptr<IReqTracker> GetAdminTracker(ClientNum client_num);
  boost::shared_ptr<IFimSocket> GetAdminFimSocket(ClientNum client_num);
  void SetAdminFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                         ClientNum client_num, bool plug);

 private:
  IAsioPolicy* asio_policy_; /**< For creating trackers */
  mutable MUTEX_TYPE data_mutex_; /**< Protect all structures below */
  ClientNum client_num_; /**< The client number of the user */
  ReqIdGenerator req_id_gen_; /**< For generating request IDs */
  /** max_send to use for ReqLimiter's */
  uint64_t limiter_max_send_;
  /**
   * The MS tracker.  It gains an IReqTracker during construction, and
   * it will remain there until destruction of the tracker mapper.
   */
  boost::shared_ptr<IReqTracker> ms_tracker_;
  /**
   * The DS trackers.  It gains an IReqTracker only when first
   * accessed, and once created it will remain there until destruction
   * of the tracker mapper.
   */
  DSReqTrackerList ds_trackers_;
  /**
   * The DS sockets.  It is sort of the reverse mapping of
   * ds_trackers_, although it maps from sockets rather than request
   * trackers.
   */
  DSFimSocketList ds_sockets_;
  /**
   * The FC trackers.  It gains an IReqTracker only when first set
   * with an IFimSocket, and will remain there until it is set with a
   * null IFimSocket with the failed flag.
   */
  FCReqTrackerList fc_trackers_;
  /**
   * The Admin trackers.  It gains an IReqTracker only when first set
   * with an IFimSocket, and will remain there until it is set with a
   * null IFimSocket with the failed flag.
   */
  AdminReqTrackerList admin_trackers_;

  static boost::shared_ptr<IFimSocket> GetFimSocket_(
      boost::shared_ptr<IReqTracker> tracker) {
    if (!tracker)
      return boost::shared_ptr<IFimSocket>();
    return tracker->GetFimSocket();
  }

  static void DetachTracker_(boost::shared_ptr<IReqTracker> tracker,
                             std::list<boost::shared_ptr<IFimSocket> >*
                             socket_ret) {
    boost::shared_ptr<IFimSocket> fim_socket = tracker->GetFimSocket();
    if (fim_socket) {
      socket_ret->push_back(fim_socket);
      tracker->SetFimSocket(boost::shared_ptr<IFimSocket>());
    }
  }

  static void PlugTracker_(boost::shared_ptr<IReqTracker> tracker, bool plug) {
    tracker->Plug(plug);
  }

  static bool CheckPendingWrite_(boost::shared_ptr<IReqTracker> tracker) {
    boost::shared_ptr<IFimSocket> fim_socket = tracker->GetFimSocket();
    if (!fim_socket)
      return false;
    return fim_socket->pending_write();
  }

  static void ShutdownTracker_(boost::shared_ptr<IReqTracker> tracker) {
    tracker->Shutdown();
  }

  boost::shared_ptr<IReqTracker> GetMSTracker_() {
    return ms_tracker_;
  }

  boost::shared_ptr<IReqTracker> GetDSTracker_(
      GroupId group_id, GroupRole role_id) {
    boost::shared_ptr<IReqTracker> ret =
        ds_trackers_[std::make_pair(group_id, role_id)];
    if (ret)
      return ret;
    return ds_trackers_[std::make_pair(group_id, role_id)]
        = MakeReqTracker("DS " + GroupRoleName(group_id, role_id),
                         asio_policy_, &req_id_gen_);
  }

  boost::shared_ptr<IReqTracker> GetFCTracker_(ClientNum client_num) const {
    FCReqTrackerList::const_iterator it = fc_trackers_.find(client_num);
    if (it == fc_trackers_.end())
      return boost::shared_ptr<IReqTracker>();
    return it->second;
  }

  boost::shared_ptr<IReqTracker> GetAdminTracker_(ClientNum client_num) const {
    AdminReqTrackerList::const_iterator it = admin_trackers_.find(client_num);
    if (it == admin_trackers_.end())
      return boost::shared_ptr<IReqTracker>();
    return it->second;
  }

  void ForAllTrackers(
      boost::function<void(boost::shared_ptr<IReqTracker>)> func) {
    func(ms_tracker_);
    for (DSReqTrackerList::iterator it = ds_trackers_.begin();
         it != ds_trackers_.end();
         ++it)
      func(it->second);
    for (FCReqTrackerList::iterator it = fc_trackers_.begin();
         it != fc_trackers_.end();
         ++it)
      func(it->second);
  }

  void ForAllTrackers(
      boost::function<void(boost::shared_ptr<const IReqTracker>)> func) const  {
    func(ms_tracker_);
    for (DSReqTrackerList::const_iterator it = ds_trackers_.cbegin();
         it != ds_trackers_.cend();
         ++it)
      func(it->second);
    for (FCReqTrackerList::const_iterator it = fc_trackers_.cbegin();
         it != fc_trackers_.cend();
         ++it)
      func(it->second);
  }

  bool FindTracker_(
      boost::function<bool(boost::shared_ptr<IReqTracker>)> func) const {
    TrackerList trackers = GetTrackerList_();
    for (TrackerList::const_iterator it = trackers.begin();
         it != trackers.end();
         ++it)
      if (func(*it))
        return true;

    return false;
  }

  TrackerList GetTrackerList_() const {
    TrackerList trackers;
    trackers.push_back(ms_tracker_);
    for (DSReqTrackerList::const_iterator it = ds_trackers_.begin();
         it != ds_trackers_.end();
         ++it)
      trackers.push_back(it->second);
    for (FCReqTrackerList::const_iterator it = fc_trackers_.begin();
         it != fc_trackers_.end();
         ++it)
      trackers.push_back(it->second);
    return trackers;
  }
};

TrackerMapper::~TrackerMapper() {
  TrackerMapper::Reset();
}

void TrackerMapper::Reset() {
  std::list<boost::shared_ptr<IFimSocket> > fim_sockets;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    ForAllTrackers(boost::bind(DetachTracker_, _1, &fim_sockets));
  }
  BOOST_FOREACH(boost::shared_ptr<IFimSocket> socket, fim_sockets) {
    socket->Shutdown();
  }
}

void TrackerMapper::SetClientNum(ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  LOG(notice, Server, "Client number is ", PINT(client_num));
  req_id_gen_.SetClientNum(client_num);
  client_num_ = client_num;
}

void TrackerMapper::SetLimiterMaxSend(uint64_t limiter_max_send) {
  MUTEX_LOCK_GUARD(data_mutex_);
  limiter_max_send_ = limiter_max_send;
  ForAllTrackers(boost::bind(&IReqTracker::set_limiter_max_send, _1,
                             limiter_max_send));
}

boost::shared_ptr<IReqTracker> TrackerMapper::GetMSTracker() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetMSTracker_();
}

boost::shared_ptr<IFimSocket> TrackerMapper::GetMSFimSocket() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetFimSocket_(GetMSTracker_());
}

void TrackerMapper::SetMSFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                                   bool plug) {
  MUTEX_LOCK_GUARD(data_mutex_);
  boost::shared_ptr<IReqTracker> tracker = GetMSTracker_();
  tracker->SetFimSocket(fim_socket, plug);
  if (fim_socket)
    fim_socket->SetReqTracker(tracker);
}

boost::shared_ptr<IReqTracker> TrackerMapper::GetDSTracker(
    GroupId group_id, GroupRole role_id) {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetDSTracker_(group_id, role_id);
}

boost::shared_ptr<IFimSocket> TrackerMapper::GetDSFimSocket(GroupId group_id,
                                                            GroupRole role_id) {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetFimSocket_(GetDSTracker_(group_id, role_id));
}

void TrackerMapper::SetDSFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                                   GroupId group_id, GroupRole role_id,
                                   bool plug) {
  MUTEX_LOCK_GUARD(data_mutex_);
  boost::shared_ptr<IReqTracker> tracker = GetDSTracker_(group_id, role_id);
  boost::shared_ptr<IFimSocket> orig_fim_socket = GetFimSocket_(tracker);
  if (orig_fim_socket)
    ds_sockets_.erase(orig_fim_socket);
  tracker->SetFimSocket(fim_socket, plug);
  if (fim_socket) {
    fim_socket->SetReqTracker(tracker);
    ds_sockets_[fim_socket] = std::make_pair(group_id, role_id);
  }
}

bool TrackerMapper::FindDSRole(boost::shared_ptr<IFimSocket> fim_socket,
                               GroupId* group_id_ret, GroupRole* role_id_ret) {
  MUTEX_LOCK_GUARD(data_mutex_);
  DSFimSocketList::iterator it = ds_sockets_.find(fim_socket);
  if (it == ds_sockets_.end())
    return false;
  *group_id_ret = it->second.first;
  *role_id_ret = it->second.second;
  return true;
}

boost::shared_ptr<IReqTracker> TrackerMapper::GetFCTracker(
    ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetFCTracker_(client_num);
}

boost::shared_ptr<IFimSocket> TrackerMapper::GetFCFimSocket(
    ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetFimSocket_(GetFCTracker_(client_num));
}

void TrackerMapper::SetFCFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                                   ClientNum client_num, bool plug) {
  MUTEX_LOCK_GUARD(data_mutex_);
  FCReqTrackerList::iterator tracker_it = fc_trackers_.find(client_num);
  if (fim_socket) {
    if (tracker_it == fc_trackers_.end()) {
      tracker_it = fc_trackers_.insert(std::make_pair(
          client_num,
          MakeReqTracker((boost::format("FC %d") % client_num).str(),
                         asio_policy_, &req_id_gen_, client_num))).first;
    }
    fim_socket->SetReqTracker(tracker_it->second);
  } else if (tracker_it == fc_trackers_.end()) {
    return;
  }
  tracker_it->second->SetFimSocket(fim_socket, plug);
  if (!fim_socket)
    fc_trackers_.erase(client_num);
}

void TrackerMapper::DSGBroadcast(GroupId group, FIM_PTR<IFim> fim,
    GroupRole except = kNumDSPerGroup) {
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    if (r == except)
      continue;
    boost::shared_ptr<IFimSocket> fim_socket = GetDSFimSocket(group, r);
    if (fim_socket)
      fim_socket->WriteMsg(fim);
  }
}

void TrackerMapper::FCBroadcast(FIM_PTR<IFim> fim) {
  for (FCReqTrackerList::iterator it = fc_trackers_.begin();
       it != fc_trackers_.end();
       ++it) {
    boost::shared_ptr<IFimSocket> fim_socket = it->second->GetFimSocket();
    if (fim_socket)
      fim_socket->WriteMsg(fim);
  }
}

void TrackerMapper::Plug(bool plug) {
  ForAllTrackers(boost::bind(&TrackerMapper::PlugTracker_, _1, plug));
}

bool TrackerMapper::HasPendingWrite() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return FindTracker_(boost::bind(&TrackerMapper::CheckPendingWrite_, _1));
}

void TrackerMapper::DumpPendingRequests() const {
  ForAllTrackers(boost::bind(&IReqTracker::DumpPendingRequests, _1));
}

void TrackerMapper::Shutdown() {
  ForAllTrackers(boost::bind(&TrackerMapper::ShutdownTracker_, _1));
}

boost::shared_ptr<IReqTracker> TrackerMapper::GetAdminTracker(
    ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetAdminTracker_(client_num);
}

boost::shared_ptr<IFimSocket> TrackerMapper::GetAdminFimSocket(
    ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  return GetFimSocket_(GetAdminTracker_(client_num));
}

void TrackerMapper::SetAdminFimSocket(boost::shared_ptr<IFimSocket> fim_socket,
                                      ClientNum client_num, bool plug) {
  MUTEX_LOCK_GUARD(data_mutex_);
  AdminReqTrackerList::iterator tracker_it = admin_trackers_.find(client_num);
  if (fim_socket) {
    if (tracker_it == admin_trackers_.end()) {
      tracker_it = admin_trackers_.insert(std::make_pair(
          client_num,
          MakeReqTracker((boost::format("Admin %d") % client_num).str(),
                         asio_policy_, &req_id_gen_, client_num))).first;
    }
    fim_socket->SetReqTracker(tracker_it->second);
  } else if (tracker_it == admin_trackers_.end()) {
    return;
  }
  tracker_it->second->SetFimSocket(fim_socket, plug);
  if (!fim_socket)
    admin_trackers_.erase(client_num);
}

}  // namespace

ITrackerMapper* MakeTrackerMapper(IAsioPolicy* policy) {
  return new TrackerMapper(policy);
}

}  // namespace cpfs
