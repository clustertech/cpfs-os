/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the interface for the reply set.  The standby MS
 * and the DS uses it to keep recently replicated Fim replies, so that
 * if the Fim ID appear again, the reply is sent and the processing is
 * not repeated.
 */

#include "server/reply_set_impl.hpp"

#include <sys/time.h>

#include <ctime>
#include <utility>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "util.hpp"
#include "server/reply_set.hpp"

namespace cpfs {
namespace server {
namespace {

/**
 * An entry in the reply set.
 */
struct RSEntry {
  FIM_PTR<IFim> reply; /**< The reply */
  struct timespec time_added; /**< Time when the entries is added */

  /** Get the request ID of the entry */
  ReqId req_id() const {
    return reply->req_id();
  }

  /** Get the client number of the entry */
  ClientNum client_num() const {
    return reply->req_id() >> (64 - kClientBits);
  }
};

/** Tag for request ID index */
struct ByReqId {};

/** Request ID key type */
typedef boost::multi_index::const_mem_fun<RSEntry, ReqId,
                                          &RSEntry::req_id> ReqIdMember;

/** Tag for client number index */
struct ByClient {};

/** Request ID key type */
typedef boost::multi_index::const_mem_fun<RSEntry, ClientNum,
                                          &RSEntry::client_num> ClientNumMember;

/** Tag for insertion-order index */
struct ByInsertOrder {};

/**
 * The internal data structure holding entries in the reply set.
 */
typedef boost::multi_index::multi_index_container<
  RSEntry,
  boost::multi_index::indexed_by<
    boost::multi_index::hashed_unique<boost::multi_index::tag<ByReqId>,
                                      ReqIdMember>,
    boost::multi_index::ordered_non_unique<boost::multi_index::tag<ByClient>,
                                           ClientNumMember>,
    boost::multi_index::sequenced<boost::multi_index::tag<ByInsertOrder> >
  >
> RSEntries;

/**
 * Client number index of RSEntries.
 */
typedef RSEntries::index<ByClient>::type RSEntryByClient;

/**
 * Insertion order index of RSEntries.
 */
typedef RSEntries::index<ByInsertOrder>::type RSEntryByInsertOrder;

/**
 * Implement the IReplySet interface.
 */
class ReplySet : public IReplySet {
 public:
  ReplySet() : data_mutex_(MUTEX_INIT) {}
  FIM_PTR<IFim> FindReply(ReqId req_id) const;
  void AddReply(const FIM_PTR<IFim>& reply);
  void ExpireReplies(int min_age);
  void RemoveClient(ClientNum client_num);
  bool IsEmpty() const;

 private:
  mutable MUTEX_TYPE data_mutex_; /**< Protect the entries */
  RSEntries entries_; /**< The entries */
};

FIM_PTR<IFim> ReplySet::FindReply(ReqId req_id) const {
  MUTEX_LOCK_GUARD(data_mutex_);
  RSEntries::const_iterator it = entries_.find(req_id);
  if (it == entries_.end())
    return FIM_PTR<IFim>();
  return it->reply;
}

void ReplySet::AddReply(const FIM_PTR<IFim>& reply) {
  MUTEX_LOCK_GUARD(data_mutex_);
  RSEntry entry;
  ReqId req_id = reply->req_id();
  entries_.erase(req_id);
  entry.reply = reply;
  clock_gettime(CLOCK_MONOTONIC, &entry.time_added);
  entries_.insert(entry);
}

void ReplySet::ExpireReplies(int min_age) {
  LOG(debug, Server, "Expiring replies");
  MUTEX_LOCK_GUARD(data_mutex_);
  RSEntryByInsertOrder& ins_index = entries_.get<ByInsertOrder>();
  CompareTime ct;
  struct timespec max_time_added;
  clock_gettime(CLOCK_MONOTONIC, &max_time_added);
  max_time_added.tv_sec -= min_age;
  int cnt = 0;
  for (RSEntryByInsertOrder::iterator it = ins_index.begin();
       it != ins_index.end(); ++cnt) {
    if (!ct(it->time_added, max_time_added))
      break;
    ins_index.erase(it++);
  }
  LOG(debug, Server, "Expired ", PINT(cnt), " replies");
}

void ReplySet::RemoveClient(ClientNum client_num) {
  MUTEX_LOCK_GUARD(data_mutex_);
  RSEntryByClient& client_index = entries_.get<ByClient>();
  std::pair<RSEntryByClient::iterator, RSEntryByClient::iterator>
      pair = client_index.equal_range(client_num);
  RSEntryByClient::iterator start = pair.first, end = pair.second;
  while (start != end)
    client_index.erase(start++);
}

bool ReplySet::IsEmpty() const {
  MUTEX_LOCK_GUARD(data_mutex_);
  return entries_.empty();
}

}  // namespace

IReplySet* MakeReplySet() {
  return new ReplySet;
}

}  // namespace server
}  // namespace cpfs
