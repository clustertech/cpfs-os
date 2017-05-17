/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of interfaces for handling user and group IDs in
 * meta-data server.
 */

#include "server/ms/ugid_handler_impl.hpp"

#include <grp.h>
#include <pwd.h>
#include <syscall.h>
#include <unistd.h>

#include <sys/fsuid.h>
#include <sys/syscall.h>  // IWYU pragma: keep
#include <sys/time.h>

#include <algorithm>
#include <cstring>
#include <ctime>
#include <memory>
#include <vector>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include "common.hpp"
#include "mutex_util.hpp"
#include "util.hpp"
#include "server/ms/ugid_handler.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Represent an entry that keep group list of a user.
 */
struct GLEntry {
  uid_t uid;                /**< user ID */
  struct timespec ctime;    /**< creation time */
  std::vector<gid_t> list;  /**< supplementary GIDs */
};

/** Uid member key type */
typedef boost::multi_index::member<GLEntry, uid_t, &GLEntry::uid> UidMember;

/** Tag for insertion-order index */
struct ByInsertOrder {};

/**
 * Type for the group list cache.
 */
typedef boost::multi_index::multi_index_container<
  GLEntry,
  boost::multi_index::indexed_by<
    boost::multi_index::hashed_unique<UidMember>,
    boost::multi_index::sequenced<boost::multi_index::tag<ByInsertOrder> >
  >
> GLEntries;

/**
 * Insertion order index of GLEntries.
 */
typedef GLEntries::index<ByInsertOrder>::type GLEntryByInsertOrder;

/**
 * Implement the IUgidHandler interface.
 */
class UgidHandler : public IUgidHandler {
 public:
  /**
   * @param cache_time Number of seconds for cached group list entries
   * to be used
   */
  explicit UgidHandler(int cache_time)
      : cache_time_(cache_time), check_(false) {
    init_groups_.resize(getgroups(0, 0));
    getgroups(init_groups_.size(), &init_groups_[0]);
  }

  void SetCheck(bool check) {
    check_ = check;
  }

  UNIQUE_PTR<IUgidSetterGuard> SetFsIds(uid_t uid, gid_t gid) {
    {
      std::vector<gid_t>  supp_gids = GetGroups(uid);
      Set(uid, gid, supp_gids);
    }
    return UNIQUE_PTR<IUgidSetterGuard>(new UgidSetterGuard(this));
  }

  bool HasSupplementaryGroup(uid_t uid, gid_t gid) {
    std::vector<gid_t>  supp_gids = GetGroups(uid);
    for (unsigned i = 0; i < supp_gids.size(); ++i)
      if (gid == supp_gids[i])
        return true;
    return false;
  }

  void Clean() {
    MUTEX_LOCK_GUARD(data_mutex_);
    struct timespec min_time;
    clock_gettime(CLOCK_MONOTONIC, &min_time);
    min_time.tv_sec -= cache_time_;
    while (true) {
      GLEntryByInsertOrder::iterator it
          = gl_cache_.get<ByInsertOrder>().begin();
      if (it == gl_cache_.get<ByInsertOrder>().end() ||
          CompareTime()(min_time, it->ctime))
        break;
      gl_cache_.erase(it->uid);
    }
  }

 private:
  const int cache_time_;
  std::vector<gid_t> init_groups_; /**< Initial group list */
  bool check_; /**< Whether to actually do the checking */
  mutable MUTEX_TYPE data_mutex_;  // Protect fields below
  GLEntries gl_cache_;  // Cached group list

  /**
   * Set the uid, gid and supplementary group list for checking file
   * access.
   *
   * @param uid The uid.
   *
   * @param gid The gid.
   *
   * @param sgids The supplementary group ids.
   */
  void Set(uid_t uid, gid_t gid, const std::vector<gid_t>& sgids) {
    if (!check_)
      return;
    setfsuid(uid);
    setfsgid(gid);
    // Must make syscall directly rather than call setgroups(), since
    // glibc intercept the latter to set groups for all threads which
    // is not what we want
    syscall(__NR_setgroups, sgids.size(), sgids.data());
  }

  std::vector<gid_t> GetGroups(uid_t uid) {
    std::vector<gid_t> ret;
    struct timespec min_time;
    clock_gettime(CLOCK_MONOTONIC, &min_time);
    min_time.tv_sec -= cache_time_;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      GLEntries::iterator it = gl_cache_.find(uid);
      if (it != gl_cache_.end()) {
        if (CompareTime()(min_time, it->ctime)) {
          ret.resize(it->list.size());
          std::memcpy(ret.data(), it->list.data(),
                      it->list.size() * sizeof(gid_t));
          return ret;
        }
      }
    }
    ret = DoGetGroups(uid);
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      GLEntry entry;
      entry.uid = uid;
      entry.ctime = min_time;
      entry.ctime.tv_sec += cache_time_;
      entry.list = ret;
      gl_cache_.erase(uid);
      gl_cache_.insert(entry);
    }
    return ret;
  }

  std::vector<gid_t> DoGetGroups(uid_t uid) {
    std::vector<gid_t> ret;
    struct passwd pwd_ent, *pwd_res = 0;
    char buf[16384];
    getpwuid_r(uid, &pwd_ent, buf, sizeof(buf), &pwd_res);
    if (pwd_res == 0)
      return ret;
    const int kMaxGroups = 1024;
    gid_t groups[kMaxGroups];
    int ngroups = kMaxGroups;
    getgrouplist(pwd_res->pw_name, pwd_res->pw_gid, groups, &ngroups);
    ngroups = std::max(std::min(ngroups, kMaxGroups), 0);
    ret.resize(ngroups);
    std::memcpy(ret.data(), groups, ngroups * sizeof(groups[0]));
    return ret;
  }

  /**
   * Reset the effect of SetFsIds().
   */
  void Reset() {
    Set(0, 0, init_groups_);
  }

  /**
   * Implement the IUgidSetterGuard interface.
   */
  class UgidSetterGuard : public IUgidSetterGuard {
   public:
    /**
     * @param handler The UgidHandler creating the guard.
     */
    explicit UgidSetterGuard(UgidHandler* handler) : handler_(handler) {}

    ~UgidSetterGuard() {
      handler_->Reset();
    };

   private:
    UgidHandler* handler_; /**< The handler creating the guard */
  };
};

}  // namespace

IUgidHandler* MakeUgidHandler(int cache_time) {
  return new UgidHandler(cache_time);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
