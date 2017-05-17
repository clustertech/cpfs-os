/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of request tracker related facilities.
 */

#include "req_tracker_impl.hpp"

#include <stdint.h>

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "asio_policy.hpp"
#include "common.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_limiter.hpp"
#include "req_limiter_impl.hpp"
#include "req_tracker.hpp"

namespace cpfs {

namespace {

/**
 * Request tracker implementation type.
 */
typedef boost::unordered_map<ReqId, boost::shared_ptr<IReqEntry> > ReqEntries;

/**
 * Sort entries for reconfirmation
 */
struct EntrySorter {
  /**
   * Comparator sorts inital replied or unanswered entry (but not both).
   *
   * Initial replied entries: sort by reply_no
   * Unanswered entries: sort by req_id
   *
   * The reply_no is a timestamp for initial_reply
   */
  bool operator() (const boost::shared_ptr<IReqEntry>& e1,
                   const boost::shared_ptr<IReqEntry>& e2) const {
    if (e1->reply() && e2->reply())
      return e1->reply_no() < e2->reply_no();
    else
      return e1->req_id() < e2->req_id();
  }
};

/**
 * Type for keeping entries poised for resend or redirection
 */
typedef std::vector<boost::shared_ptr<IReqEntry> > EntryVector;

/**
 * Implement IReqTracker.
 */
class ReqTracker : public IReqTracker {
 public:
  /**
   * @param name The name to be returned on name()
   *
   * @param policy The Asio policy to run callbacks
   *
   * @param req_id_gen The request ID generator used to generate
   * request IDs, which is used when AddRequest() is called with
   * alloc_req_id set
   *
   * @param peer_client_num The client number of peer.  If 0, the peer is
   * not a client.
   */
  explicit ReqTracker(const std::string& name, IAsioPolicy* policy,
                      ReqIdGenerator* req_id_gen, ClientNum peer_client_num)
      : name_(name), asio_policy_(policy), limiter_max_send_(256 * 1024 * 1024),
        data_mutex_(MUTEX_INIT),
        req_id_gen_(req_id_gen), peer_client_num_(peer_client_num),
        expecting_(false), plugged_(false), shutting_down_(false),
        last_reply_no_(0) {}
  std::string name() const { return name_; }
  void set_limiter_max_send(uint64_t limiter_max_send) {
    limiter_max_send_ = limiter_max_send;
  }
  ReqIdGenerator* req_id_gen() { return req_id_gen_; }
  ClientNum peer_client_num() { return peer_client_num_; }
  void SetFimSocket(const boost::shared_ptr<IFimSocket>& fim_socket, bool plug);
  boost::shared_ptr<IFimSocket> GetFimSocket();
  void SetExpectingFimSocket(bool expecting);
  boost::shared_ptr<IReqEntry> AddRequest(
      const FIM_PTR<IFim>& request,
      boost::unique_lock<MUTEX_TYPE>* lock);
  boost::shared_ptr<IReqEntry> AddTransientRequest(
      const FIM_PTR<IFim>& request,
      boost::unique_lock<MUTEX_TYPE>* lock);
  bool AddRequestEntry(const boost::shared_ptr<IReqEntry>& entry,
                       boost::unique_lock<MUTEX_TYPE>* lock);
  boost::shared_ptr<IReqEntry> GetRequestEntry(ReqId req_id) const;
  void AddReply(const FIM_PTR<IFim>& reply);
  bool ResendReplied();
  void RedirectRequest(ReqId req_id, boost::shared_ptr<IReqTracker> target);
  bool RedirectRequests(ReqRedirector get_tracker);
  void Plug(bool plug);
  IReqLimiter* GetReqLimiter();
  void DumpPendingRequests() const;
  void Shutdown();

 private:
  std::string name_; /**< The name of the tracker */
  IAsioPolicy* asio_policy_; /**< The Asio policy for running callbacks */
  /** max_send to use when creating ReqLimiter's */
  boost::atomic<uint64_t> limiter_max_send_;
  mutable MUTEX_TYPE data_mutex_; /**< Protect all fields in the tracker */

  ReqIdGenerator* req_id_gen_; /**< Request ID generator */
  ClientNum peer_client_num_; /**< Client number of peer */
  ReqEntries entries_; /**< The entries */
  bool expecting_; /**< Whether expecting a FimSocket is soon to be set */
  bool plugged_; /**< Whether to send data to fim_socket_ */
  bool shutting_down_; /**< Set tracker to shutdown. Rejecting new requests */
  /** The Fim socket for sending Fim's */
  boost::shared_ptr<IFimSocket> fim_socket_;
  boost::atomic<uint64_t> last_reply_no_; /**< Last reply order no */
  /** Request limiter for the tracker */
  boost::scoped_ptr<IReqLimiter> req_limiter_;

  void AddRequestEntry_(const boost::shared_ptr<IReqEntry>& entry,
                        boost::unique_lock<MUTEX_TYPE>* lock = 0) {
    entries_[entry->request()->req_id()] = entry;
    if (plugged_) {
      entry->set_sent(true);
      if (lock) {
        entry->FillLock(lock);
        MUTEX_LOCK_SCOPE_LOG();
        SendRequest_(entry);
      } else {
        SendRequest_(entry);
      }
    }
  }

  void ResendRequests_(EntryVector* resend_entries) {
    std::sort(resend_entries->begin(), resend_entries->end(), EntrySorter());
    for (EntryVector::iterator it = resend_entries->begin();
         it != resend_entries->end(); ++it) {
      (*it)->set_reply_error(false);
      SendRequest_(*it);
      (*it)->set_sent(true);
    }
  }

  void SendRequest_(const boost::shared_ptr<IReqEntry>& entry) {
    fim_socket_->WriteMsg(entry->request());
    if (!entry->IsPersistent())
      entry->UnsetRequest();
  }

  void UnsetSocket_() {
    EntryVector failed_entries;
    for (ReqEntries::iterator it = entries_.begin(); it != entries_.end();) {
      if (it->second->IsPersistent()) {
        it->second->set_sent(false);
        ++it;
      } else {
        failed_entries.push_back(it->second);
        it = entries_.erase(it);
      }
    }
    plugged_ = false;
    expecting_ = false;
    if (!failed_entries.empty())
      asio_policy_->Post(boost::bind(&ReqTracker::NotifyEntriesFailed,
                                     this, failed_entries));
  }

  void NotifyEntriesFailed(EntryVector failed_entries) {
    for (EntryVector::const_iterator it = failed_entries.begin();
         it != failed_entries.end();
         ++it)
      (*it)->SetReply(FIM_PTR<IFim>(), ++last_reply_no_);
  }

  void DoPlug_(bool plug) {
    plugged_ = plug;
    if (!plug)
      return;
    EntryVector resend_entries;
    for (ReqEntries::iterator it = entries_.begin();
         it != entries_.end(); ++it) {
      boost::shared_ptr<IReqEntry> entry = it->second;
      if (!entry->sent())
        resend_entries.push_back(entry);
    }
    if (!resend_entries.empty()) {
      ResendRequests_(&resend_entries);
      LOG(informational, Server,
          "Tracker is plugged. Unconfirmed requests are resent");
    }
  }
};

void ReqTracker::SetFimSocket(const boost::shared_ptr<IFimSocket>& fim_socket,
                              bool plug) {
  MUTEX_LOCK_GUARD(data_mutex_);
  fim_socket_ = fim_socket;
  if (fim_socket) {
    DoPlug_(plug);
  } else {
    UnsetSocket_();
  }
}

boost::shared_ptr<IFimSocket> ReqTracker::GetFimSocket() {
  MUTEX_LOCK_GUARD(data_mutex_);
  return fim_socket_;
}

void ReqTracker::SetExpectingFimSocket(bool expecting) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (expecting) {
    expecting_ = !fim_socket_;
    return;
  }
  expecting_ = false;
  if (!fim_socket_)
    UnsetSocket_();
}

boost::shared_ptr<IReqEntry> ReqTracker::AddRequest(
    const FIM_PTR<IFim>& request,
    boost::unique_lock<MUTEX_TYPE>* lock) {
  const boost::shared_ptr<IReqEntry>& entry =
      MakeDefaultReqEntry(shared_from_this(), request);
  AddRequestEntry(entry, lock);
  return entry;
}

boost::shared_ptr<IReqEntry> ReqTracker::AddTransientRequest(
    const FIM_PTR<IFim>& request,
    boost::unique_lock<MUTEX_TYPE>* lock) {
  const boost::shared_ptr<IReqEntry>& entry =
      MakeTransientReqEntry(shared_from_this(), request);
  AddRequestEntry(entry, lock);
  return entry;
}

bool ReqTracker::AddRequestEntry(const boost::shared_ptr<IReqEntry>& entry,
                                 boost::unique_lock<MUTEX_TYPE>* lock) {
  entry->PreQueueHook();
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    bool may_queue = plugged_ || expecting_ || entry->IsPersistent();
    if (may_queue && !shutting_down_) {
      AddRequestEntry_(entry, lock);
      return true;
    }
  }  // Unlock
  LOG(debug, Fim, "Dropping request for missing Fim socket: ",
      PVal(name_), " ", PVal(entry->request()));
  entry->SetReply(FIM_PTR<IFim>(), ++last_reply_no_);
  return false;
}

boost::shared_ptr<IReqEntry> ReqTracker::GetRequestEntry(ReqId req_id) const {
  MUTEX_LOCK_GUARD(data_mutex_);
  ReqEntries::const_iterator it = entries_.find(req_id);
  return it == entries_.cend() ? boost::shared_ptr<IReqEntry>() : it->second;
}

void ReqTracker::AddReply(const FIM_PTR<IFim>& reply) {
  ReqId req_id = reply->req_id();
  boost::shared_ptr<IReqEntry> entry;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    if (entries_.find(req_id) == entries_.end()) {
      LOG(warning, Fim, "No request matches Fim ", PVal(reply));
      return;
    }
    entry = entries_[req_id];
    if (!entry->IsPersistent() || reply->is_final())
      entries_.erase(req_id);
  }  // Unlock
  entry->SetReply(reply, ++last_reply_no_);
}

bool ReqTracker::ResendReplied() {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (!fim_socket_)
    return false;
  EntryVector resend_entries;
  for (ReqEntries::iterator it = entries_.begin(); it != entries_.end(); ++it) {
    const boost::shared_ptr<IReqEntry>& entry = it->second;
    if (!entry->sent() && bool(entry->reply()))
      resend_entries.push_back(entry);
  }
  ResendRequests_(&resend_entries);
  return resend_entries.size() > 0;
}

void ReqTracker::RedirectRequest(
    ReqId req_id, boost::shared_ptr<IReqTracker> target) {
  boost::shared_ptr<IReqEntry> entry;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    ReqEntries::iterator it = entries_.find(req_id);
    if (it == entries_.end())
      return;
    entry = it->second;
    entries_.erase(it);
  }  // Don't hold a mutex and acquire another: prevent deadlock
  entry->set_sent(false);
  {
    ReqTracker& rtarget = static_cast<ReqTracker&>(*target);
    MUTEX_LOCK_GUARD(rtarget.data_mutex_);
    rtarget.AddRequestEntry_(entry);
  }
}

bool ReqTracker::RedirectRequests(ReqRedirector redirector) {
  EntryVector entries;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    EntryVector fresh_entries;
    for (ReqEntries::iterator it = entries_.begin();
         it != entries_.end(); ++it) {
      const boost::shared_ptr<IReqEntry>& entry = it->second;
      (entry->reply() ? entries : fresh_entries).push_back(entry);
    }
    entries_.clear();
    std::sort(entries.begin(), entries.end(), EntrySorter());
    std::sort(fresh_entries.begin(), fresh_entries.end(), EntrySorter());
    std::copy(fresh_entries.begin(), fresh_entries.end(),
              std::inserter(entries, entries.end()));
  }
  for (unsigned i = 0; i < entries.size(); ++i) {
    const FIM_PTR<IFim>& fim = entries[i]->request();
    const boost::shared_ptr<IReqTracker>& target = redirector(fim);
    if (!target || target.get() == this) {
      LOG(warning, Fim, "Cannot redirect Fim: ", PVal(fim), ", target = ",
          target ? "this" : "null");
      entries[i]->SetReply(FIM_PTR<IFim>(), 0);
      continue;
    }
    entries[i]->set_sent(false);
    ReqTracker& rtarget = static_cast<ReqTracker&>(*target);
    MUTEX_LOCK_GUARD(rtarget.data_mutex_);
    rtarget.AddRequestEntry_(entries[i]);
  }
  return entries.size() > 0;
}

void ReqTracker::Plug(bool plug) {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (plug && !fim_socket_) {
    LOG(warning, Fim, "Plugging a ReqTracker without FimSocket, ignored");
    return;
  }
  DoPlug_(plug);
}

IReqLimiter* ReqTracker::GetReqLimiter() {
  MUTEX_LOCK_GUARD(data_mutex_);
  if (!req_limiter_)
    req_limiter_.reset(MakeReqLimiter(shared_from_this(), limiter_max_send_));
  return req_limiter_.get();
}

void ReqTracker::DumpPendingRequests() const {
  ReqEntries entries;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    entries = entries_;
  }
  LOG(notice, Fim, "Dump of ", PVal(name_), " requests begins");
  for (ReqEntries::iterator it = entries.begin(); it != entries.end(); ++it)
    it->second->Dump();
  LOG(notice, Fim, "Dump of ", PVal(name_), " requests ends");
}

void ReqTracker::Shutdown() {
  EntryVector failed_entries;
  {
    MUTEX_LOCK_GUARD(data_mutex_);
    // Notify failure to the pending entries
    for (ReqEntries::iterator it = entries_.begin(); it != entries_.end();) {
      failed_entries.push_back(it->second);
      it = entries_.erase(it);
    }
    // No new request entry will be accepted
    shutting_down_ = true;
  }
  asio_policy_->Post(boost::bind(&ReqTracker::NotifyEntriesFailed,
                                 this, failed_entries));
}

}  // namespace

boost::shared_ptr<IReqTracker> MakeReqTracker(
    const std::string& name, IAsioPolicy* policy,
    ReqIdGenerator* req_id_gen, ClientNum peer_client_num) {
  return boost::shared_ptr<IReqTracker>(
      new ReqTracker(name, policy, req_id_gen, peer_client_num));
}

}  // namespace cpfs
