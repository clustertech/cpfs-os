/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of IDSQueryMgr.
 */
#include "ds_query_mgr.hpp"

#include <map>
#include <stdexcept>
#include <vector>

#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_maker.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"
#include "tracker_mapper.hpp"
#include "server/ms/base_ms.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/** Map of DS group to request entries */
typedef boost::unordered_map<
    GroupId,
    std::vector<boost::shared_ptr<IReqEntry> > > GroupEntries;

/**
 * Implement the IDSQueryMgr interface.
 */
class DSQueryMgr : public IDSQueryMgr {
 public:
  /**
   * Create the DSQueryMgr
   *
   * @param meta_server The meta server
   */
  explicit DSQueryMgr(BaseMetaServer* meta_server) : server_(meta_server) {}

  DSReplies Request(const FIM_PTR<IFim>& fim,
                    const std::vector<GroupId>& group_ids);

 private:
  BaseMetaServer* server_;

  /**
   * Check whether a reply is good.  Any type is considered good,
   * unless it is of type ResultCodeFim.  In that case, the err_no is
   * inspected, and the reply is considered good only if err_no is
   * zero.
   *
   * @param reply The reply to check
   *
   * @return Whether the reply is good
   */
  bool CheckReply(const FIM_PTR<IFim>& reply) {
    // N.B.: reply cannot be null, since a transient request entry is used
    if (reply->type() != kResultCodeReplyFim)
      return true;
    const ResultCodeReplyFim& rreply =
        static_cast<const ResultCodeReplyFim&>(*reply);
    return rreply->err_no == 0;
  }
};

DSReplies DSQueryMgr::Request(
    const FIM_PTR<IFim>& fim,
    const std::vector<GroupId>& group_ids) {
  DSReplies replies;
  GroupEntries entries;
  ITrackerMapper* tracker_mapper = server_->tracker_mapper();
  LOG(debug, Fim, "Sending request ", PVal(fim), " to DS groups");
  for (std::vector<GroupId>::const_iterator group_it = group_ids.begin();
       group_it != group_ids.end(); ++group_it) {
    for (GroupRole role = 0; role < kNumDSPerGroup; ++role) {
      boost::shared_ptr<IReqTracker> tracker =
          tracker_mapper->GetDSTracker(*group_it, role);
      boost::shared_ptr<IReqEntry> entry =
          MakeTransientReqEntry(tracker, GetFimsMaker().Clone(fim));
      if (!tracker->AddRequestEntry(entry))
        continue;
      entries[*group_it].push_back(entry);
    }
  }
  bool reply_error = false;
  for (std::vector<GroupId>::const_iterator group_it = group_ids.begin();
       group_it != group_ids.end(); ++group_it) {
    for (GroupRole role = 0; role < entries[*group_it].size(); ++role) {
      FIM_PTR<IFim> reply = entries[*group_it][role]->WaitReply();
      if (!CheckReply(reply)) {
        LOG(error, Fim, "DS group request failed. Invalid reply from: ",
            GroupRoleName(*group_it, role));
        reply_error = true;
        continue;
      }
      replies[*group_it][role] = reply;
    }
  }
  if (reply_error)
    throw std::runtime_error("Error in DS group request");
  return replies;
}

}  // namespace

IDSQueryMgr* MakeDSQueryMgr(BaseMetaServer* meta_server) {
  return new DSQueryMgr(meta_server);
}

}  // namespace ms

}  // namespace server

}  // namespace cpfs
