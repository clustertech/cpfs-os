/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IStatKeeper.
 */

#include "server/ms/stat_keeper_impl.hpp"

#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "tracker_mapper.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/stat_keeper.hpp"
#include "server/ms/topology.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Number of seconds to wait between queries.
 */
const double kRefreshPeriod = 10.0;

/**
 * A record of space stat query.
 */
struct SpaceStatQueryRec {
  SpaceStatQueryRec() : num_remain(0) {}
  int num_remain; /**< Number of responses to wait for */
  AllDSSpaceStat collected; /**< Statistics collected so far */
  std::vector<SpaceStatCallback> callbacks; /**< Callbacks registered */
};

/**
 * Implement the IStatKeeper interface.
 */
class StatKeeper : public IStatKeeper {
 public:
  /**
   * @param server The server object using the keeper
   */
  explicit StatKeeper(BaseMetaServer* server)
      : server_(server), data_mutex_(MUTEX_INIT) {}
  void Run();
  void OnAllStat(SpaceStatCallback callback);
  AllDSSpaceStat GetLastStat();
  void OnNewStat(SpaceStatCallback callback);

 private:
  BaseMetaServer* server_; /**< The server using the keeper */
  MUTEX_TYPE data_mutex_; /**< Protect fields below */
  std::vector<SpaceStatCallback> all_stat_callback_; /**< Global callback */
  AllDSSpaceStat last_stat_; /**< Latest stats collected */
  boost::shared_ptr<SpaceStatQueryRec> curr_query_; /**< Current query */
  /** Next query.  pending_query_ != null implies curr_query_ != null */
  boost::shared_ptr<SpaceStatQueryRec> pending_query_;
  boost::shared_ptr<DeadlineTimer> refresh_timer_; /**< Trigger next refresh */

  /**
   * Start a new query using curr_query_.
   */
  void StartQuery_();

  /**
   * Handle reply or lack of it from a DS.
   */
  void HandleDSStatReply(boost::shared_ptr<IReqEntry> entry,
                         GroupId group, GroupRole role);

  /**
   * Handle timeout of refresh timer.
   */
  void HandleRefreshTimeout(const boost::system::error_code&);
};

void StatKeeper::Run() {
  if (!refresh_timer_)
    refresh_timer_.reset(server_->asio_policy()->MakeDeadlineTimer());
  OnNewStat(SpaceStatCallback());
}

void StatKeeper::OnAllStat(SpaceStatCallback callback) {
  MUTEX_LOCK_GUARD(data_mutex_);
  all_stat_callback_.push_back(callback);
}

AllDSSpaceStat StatKeeper::GetLastStat() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return last_stat_;
}

void StatKeeper::OnNewStat(SpaceStatCallback callback) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (curr_query_ && !pending_query_)
    pending_query_.reset(new SpaceStatQueryRec);
  if (pending_query_) {
    if (callback)
      pending_query_->callbacks.push_back(callback);
    return;
  }
  // curr_query_ is empty
  server_->asio_policy()->SetDeadlineTimer(refresh_timer_.get(),
                                           0, IOHandler());
  curr_query_.reset(new SpaceStatQueryRec);
  if (callback)
    curr_query_->callbacks.push_back(callback);
  StartQuery_();
}

void StatKeeper::StartQuery_() {
  GroupId num_groups = server_->topology_mgr()->num_groups();
  curr_query_->collected.resize(num_groups);
  for (GroupId g = 0; g < num_groups; ++g) {
    if (!server_->topology_mgr()->DSGReady(g))
      continue;
    curr_query_->collected[g].resize(kNumDSPerGroup);
    for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
      boost::shared_ptr<IReqTracker> tracker =
          server_->tracker_mapper()->GetDSTracker(g, r);
      if (tracker) {
        boost::unique_lock<MUTEX_TYPE> tracker_lock;
        FIM_PTR<MSStatFSFim> fim = MSStatFSFim::MakePtr();
        boost::shared_ptr<IReqEntry> entry =
            MakeTransientReqEntry(tracker, fim);
        if (tracker->AddRequestEntry(entry, &tracker_lock)) {
          ++curr_query_->num_remain;
          entry->OnAck(boost::bind(&StatKeeper::HandleDSStatReply,
                                   this, _1, g, r));
        }
      }
    }
  }
}

void StatKeeper::HandleDSStatReply(boost::shared_ptr<IReqEntry> entry,
                                   GroupId group, GroupRole role) {
  AllDSSpaceStat all_space_stats;
  std::vector<SpaceStatCallback> callbacks;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    --curr_query_->num_remain;
    FIM_PTR<IFim> reply = entry->reply();
    if (reply && reply->type() == kMSStatFSReplyFim) {
      MSStatFSReplyFim& r_reply = static_cast<MSStatFSReplyFim&>(*reply);
      DSSpaceStat& space_stat = curr_query_->collected[group][role];
      space_stat.online = true;
      space_stat.total_space = r_reply->total_space;
      space_stat.free_space = r_reply->free_space;
    }
    if (curr_query_->num_remain <= 0) {
      all_space_stats = last_stat_ = curr_query_->collected;
      callbacks = all_stat_callback_;
      callbacks.insert(callbacks.end(), curr_query_->callbacks.begin(),
                       curr_query_->callbacks.end());
      curr_query_->callbacks.clear();
      if (pending_query_) {
        curr_query_ = pending_query_;
        pending_query_.reset();
        StartQuery_();
      } else {
        curr_query_.reset();
        server_->asio_policy()->SetDeadlineTimer(
            refresh_timer_.get(), kRefreshPeriod,
            boost::bind(&StatKeeper::HandleRefreshTimeout, this,
                        boost::asio::placeholders::error));
      }
    }
  }
  for (std::vector<SpaceStatCallback>::iterator it = callbacks.begin();
       it != callbacks.end();
       ++it)
    (*it)(all_space_stats);
}

void StatKeeper::HandleRefreshTimeout(const boost::system::error_code& error) {
  if (error)  // Cancelled
    return;
  MUTEX_LOCK_GUARD(data_mutex_);
  if (!curr_query_) {
    curr_query_.reset(new SpaceStatQueryRec);
    StartQuery_();
  }
}

}  // namespace

IStatKeeper* MakeStatKeeper(BaseMetaServer* server) {
  return new StatKeeper(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
