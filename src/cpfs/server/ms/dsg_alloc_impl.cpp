/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the implementation of ServerGroup for use in MS and
 * DS to keep slots of connection points.
 */

#include "server/ms/dsg_alloc_impl.hpp"

#include <stdint.h>

#include <algorithm>
#include <ctime>
#include <limits>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/random.hpp>

#include "common.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/dsg_alloc.hpp"
#include "server/ms/stat_keeper.hpp"
#include "server/ms/topology.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

/**
 * Implement the IDSGAllocator interface.  This implementation
 * generate random server group Ids.
 */
class DSGAllocator : public IDSGAllocator {
 public:
  /**
   * @param server The meta server
   */
  explicit DSGAllocator(BaseMetaServer* server)
      : server_(server), rng_(std::time(0)), uniform_prob_(rng_) {}

  std::vector<GroupId> Allocate(GroupId num_groups);

  void Advise(std::string item, GroupId group_id, int64_t value) {
    (void) item;
    (void) group_id;
    (void) value;
  }

 private:
  BaseMetaServer* server_; /**< The meta server */
  boost::random::mt19937 rng_; /**< Pseudo-random source */
  boost::uniform_01<boost::mt19937> uniform_prob_; /**< Probability generator */

  int random_group(int bound) {
    boost::random::uniform_int_distribution<> alloc(0, bound - 1);
    return alloc(rng_);
  }

  // The following avoid the need to specify the type of boost::bind
  template <typename TFunc>
  void shuffle_groups_func(std::vector<GroupId>* groups, TFunc func) {
    std::random_shuffle(groups->begin(), groups->end(), func);
  }

  void shuffle_groups(std::vector<GroupId>* groups) {
    shuffle_groups_func(groups,
                        boost::bind(&DSGAllocator::random_group, this, _1));
  }
};

std::vector<GroupId> DSGAllocator::Allocate(GroupId num_groups) {
  std::vector<GroupId> selected;
  {
    // List of probabilities of DSG to be used, mapped by free space
    std::list<std::pair<double, GroupId> > plist;
    AllDSSpaceStat stat = server_->stat_keeper()->GetLastStat();
    if (stat.empty()) {
      // Select num_groups of DSG from uniform distribution
      GroupId num_avail = server_->topology_mgr()->num_groups();
      for (GroupId group = 0; group < num_avail; ++group)
        if (server_->topology_mgr()->DSGReady(group))
          selected.push_back(group);
      shuffle_groups(&selected);
      selected.resize(std::min(num_groups, num_avail));
      return selected;
    }
    // Weight by free space if info are available
    for (GroupId group = 0; group < stat.size(); ++group) {
      if (stat[group].empty())  // The DS group is not yet ready to use
        continue;
      uint64_t min_free = std::numeric_limits<uint64_t>::max();
      for (GroupRole role = 0; role < stat[group].size(); ++role) {
        if (stat[group][role].online)
          min_free = std::min(min_free, stat[group][role].free_space);
      }
      if (min_free > 0)
        plist.push_back(
            std::make_pair(double(min_free) * double(min_free), group));
    }
    plist.sort();
    std::reverse(plist.begin(), plist.end());
    // Select num_groups # of groups
    GroupId num_remain = std::min(GroupId(plist.size()), num_groups);
    while (num_remain > 0 && !plist.empty()) {
      // Normalization
      double sum = 0;
      for (std::list<std::pair<double, GroupId> >::iterator itr = plist.begin();
           itr != plist.end(); ++itr)
        sum += itr->first;
      for (std::list<std::pair<double, GroupId> >::iterator itr = plist.begin();
           itr != plist.end(); ++itr)
        itr->first = itr->first / sum * num_remain;
      // Pick with calculated probability
      if (uniform_prob_() < plist.front().first) {
        selected.push_back(plist.front().second);
        --num_remain;
      }
      plist.pop_front();
    }
    shuffle_groups(&selected);
  }
  return selected;
}

}  // namespace

IDSGAllocator* MakeDSGAllocator(BaseMetaServer* server) {
  return new DSGAllocator(server);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
