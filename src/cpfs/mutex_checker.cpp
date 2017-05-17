/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define and implement a simple checker for mutex lock ordering.
 */

// This library does not use the mutex_util.hpp facility for locking,
// because we are defining how mutex_util.hpp should do locking.

#include "mutex_checker.hpp"

#include <string>
#include <utility>
#include <vector>

#include <boost/bimap/bimap.hpp>
#include <boost/bimap/unordered_set_of.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/breadth_first_search.hpp>
#include <boost/property_map/property_map.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/once.hpp>
#include <boost/thread/tss.hpp>
#include <boost/unordered_set.hpp>

#include "logger.hpp"

namespace cpfs {

namespace {

/**
 * A record of mutex locking kept per thread.
 */
struct LockRecord {
  unsigned id; /**< The id of the mutex as in checker vertex graph */
  std::string lock_info; /**< Info provided by lock operation */
  int count; /**< Number of recursive locks */
  /**
   * @param id The id of the mutex as in checker vertex graph
   *
   * @param lock_info Info provided by lock operation
   */
  LockRecord(unsigned id, const std::string& lock_info)
      : id(id), lock_info(lock_info), count(1) {}
};

/**
 * Per-thread information kept for mutex lock order checking.
 */
struct ThreadMutexInfo {
  /**
   * Lock records of the thread.  This allows each thread to quickly
   * tell what is the last lock which has been made, which should be
   * of the right ordering against any new lock operation.
   */
  std::vector<LockRecord> lock_recs;
  /**
   * Keep info for the next lock.  This is needed because we cannot
   * add the information to the lock operation: the Boost threading
   * library already defined how lock() and unlock() should be done
   * and we cannot add anything to it.  So we keep a per-thread record
   * instead.
   */
  std::string next_lock_info;
};

/**
 * Tag to keep dependency info for the edges.
 */
struct DepInfo {
  typedef boost::edge_property_tag kind; /**< Kind of graph property */
};

/**
 * Dependency info property type.  The pair is respectively the lock
 * info of the "from" vertex (earlier lock) and the "to" vertex (later
 * lock) at the time when the dependency is added.
 */
typedef boost::property<DepInfo, std::pair<std::string, std::string> >
  DepInfoProperty;

/**
 * Exception class to be used by the BFS when the target is found.
 */
class TargetFound {};

/** Type to use mapping between Ids and Tags */
typedef boost::bimaps::bimap<
  boost::bimaps::unordered_set_of<unsigned>,
  boost::bimaps::unordered_set_of<std::string> > IdTag;

/**
 * Define what to be done during the BFS of the dependency graph.  It
 * aborts the search if the target node is found (i.e., adding new
 * edge would create a cycle), and record predecessors along the way.
 */
template<typename Graph>
class BFSPathVisitor : public boost::bfs_visitor<> {
 public:
  /**
   * @param target What node to find during the BFS
   *
   * @param pred Where to store the predecessor information
   */
  BFSPathVisitor(unsigned target, std::vector<int>* pred)
      : inited_(false), target_(target), curr_(-1), pred_(pred) {}

  /**
   * Add a vertex.  We use it to initialize the predecessor map.
   *
   * @param vertex The new vertex
   *
   * @param g The dependency graph
   */
  void initialize_vertex(unsigned vertex, const Graph& g) {
    if (inited_)
      return;
    inited_ = true;
    unsigned num_vertices = boost::num_vertices(g);
    pred_->resize(num_vertices);
    for (unsigned i = 0; i < num_vertices; ++i)
      (*pred_)[i] = -1;
  }

  /**
   * A vertex is discovered.  We use it to record predecessor
   * information.
   *
   * @param vertex The vertex discovered
   *
   * @param g The dependency graph
   */
  void discover_vertex(unsigned vertex, const Graph& g) {
    (*pred_)[vertex] = curr_;
    if (vertex == target_)
      throw TargetFound();
  }

  /**
   * A vertex is popped from queue.  We use it to record parent vertex
   * when a new vertex is discovered.
   *
   * @param vertex The vertex popped
   *
   * @param g The dependency graph
   */
  void examine_vertex(unsigned vertex, const Graph& g) {
    curr_ = vertex;
  }

 private:
  bool inited_; /**< Whether pred_ is initialized */
  unsigned target_; /**< The target vertex to look for */
  int curr_; /**< The current vertex being examined */
  std::vector<int>* pred_; /**< The predecessor map */
};

/**
 * Mutex checker.
 */
class MutexChecker : public IMutexChecker {
  /**
   * Type to use for checking.
   */
  typedef boost::adjacency_list<boost::setS, boost::vecS, boost::directedS,
                                boost::no_property,
                                DepInfoProperty> DepGraph;

 public:
  unsigned NotifyMutex(std::string tag) {
    unsigned ret;
    { // Phase 1: No update
      boost::lock_guard<boost::mutex> guard(data_mutex_);
      if (Tag2Id_(tag, &ret))
        return ret;
    }
    { // Phase 2: May insert vertex.  Since multiple mutex may use the
      // same tag, this two phase scheme speeds up the important case
      // when mutex are created dynamically
      boost::lock_guard<boost::mutex> guard1(update_mutex_);
      boost::lock_guard<boost::mutex> guard2(data_mutex_);
      if (!Tag2Id_(tag, &ret)) {  // Try once more, perhaps created during lock
        ret = boost::add_vertex(approved_);
        id_tag_.insert(IdTag::value_type(ret, tag));
      }
    }
    return ret;
  }

  void SetNextLockInfo(const std::string& lock_info) {
    GetThreadInfo()->next_lock_info = lock_info;
  }

  void NotifyLock(unsigned tag_id, bool is_try) {
    (void) is_try;
    ThreadMutexInfo* thr_info = GetThreadInfo();
    std::string lock_info = thr_info->next_lock_info;
    thr_info->next_lock_info = "";
    for (unsigned i = 0; i < thr_info->lock_recs.size(); ++i) {
      if (tag_id != thr_info->lock_recs[i].id)
        continue;
      ++thr_info->lock_recs[i].count;
      // Warn against recursive locking of pattern A -> B -> ... -> A
      if (i != thr_info->lock_recs.size() - 1) {
        boost::lock_guard<boost::mutex> guard(data_mutex_);
        LockRecord& last = thr_info->lock_recs.back();
        LOG(warning, Lock, "Non-homogenious recursive lock: to lock ",
            id_tag_.left.at(tag_id), " (", lock_info, "), last locked ",
            id_tag_.left.at(last.id), " (", last.lock_info, ")");
      }
      return;
    }
    CheckLock(tag_id, thr_info, lock_info);
    thr_info->lock_recs.push_back(LockRecord(tag_id, lock_info));
  }

  void NotifyUnlock(unsigned tag_id) {
    ThreadMutexInfo* info = GetThreadInfo();
    for (unsigned i = 0; i < info->lock_recs.size(); ++i) {
      if (tag_id != info->lock_recs[i].id)
        continue;
      if (--info->lock_recs[i].count == 0)
        info->lock_recs.erase(info->lock_recs.begin() + i);
      return;
    }
    LOG(error, Lock, "Unmatched unlock");  // Defensive, can't cover
  }

 private:
  // Information related to each thread
  boost::thread_specific_ptr<ThreadMutexInfo> thread_info_;
  boost::mutex update_mutex_; /** Updater lock */
  boost::mutex data_mutex_; /** Protect information below */
  IdTag id_tag_; /** Convert between vertex IDs and tags */
  // The following (approved_ and failed_) may be read with either
  // data_mutex_ or update_mutex_, but to write, we need both.  This
  // means the data structure is concurrently read by up to two
  // threads.  This strategy is used because all our non-update reads
  // are very short and we don't mind them not concurrent.  This way
  // we can avoid an expensive shared mutex.
  DepGraph approved_; /**< Good edges added so far */
  /** Bad edges added */
  boost::unordered_set<std::pair<unsigned, unsigned> > failed_;

  ThreadMutexInfo* GetThreadInfo() {
    ThreadMutexInfo* ret = thread_info_.get();
    if (!ret)
      thread_info_.reset(ret = new ThreadMutexInfo());
    return ret;
  }

  void CheckLock(unsigned tag_id, ThreadMutexInfo* thr_info,
                 const std::string& lock_info) {
    if (thr_info->lock_recs.size() == 0)
      return;
    bool need_check = false;
    unsigned last_id = thr_info->lock_recs.back().id;
    { // First phase: No modification of lock graph.  Don't acquire
      // update_mutex_ for better performance.
      boost::lock_guard<boost::mutex> guard(data_mutex_);
      need_check = NeedDepCheck_(last_id, tag_id);
    }
    if (need_check) {  // Second phase: May insert edge.
      boost::lock_guard<boost::mutex> guard1(update_mutex_);
      need_check = NeedDepCheck_(last_id, tag_id);
      if (need_check) {
        const std::string& last_info = thr_info->lock_recs.back().lock_info;
        bool good = CheckDep_(last_id, tag_id, last_info, lock_info);
        boost::lock_guard<boost::mutex> guard2(data_mutex_);
        if (good)
          ApproveEdge_(last_id, tag_id, last_info, lock_info);
        else
          failed_.insert(std::make_pair(last_id, tag_id));
      }
    }
  }

  bool Tag2Id_(const std::string& tag, unsigned* ret) {
    IdTag::right_iterator it = id_tag_.right.find(tag);
    if (it == id_tag_.right.end())
      return false;
    *ret = it->second;
    return true;
  }

  bool NeedDepCheck_(unsigned last_id, unsigned tag_id) {
    return !boost::edge(last_id, tag_id, approved_).second &&
        failed_.find(std::make_pair(last_id, tag_id)) == failed_.end();
  }

  // This is rather expensive, of O(E+V).  But each (last_id, tag_id)
  // pair will only have this called at most once, because results
  // will be recorded either in approved_ or failed_ and skipped by
  // NeedDepCheck_().
  bool CheckDep_(unsigned last_id, unsigned tag_id,
                 const std::string& last_info,
                 const std::string& lock_info) {
    // Look for a path from tag_id to last_id.  If exist, this means
    // adding the intended edge from last_id to tag_id would form a
    // cycle, so the edge must be rejected.
    std::vector<int> preds;
    BFSPathVisitor<DepGraph> bfs_visitor(last_id, &preds);
    try {
      breadth_first_search(approved_, tag_id, boost::visitor(bfs_visitor));
      return true;
    } catch(const TargetFound& e) {}
    LOG(warning, Lock, "Lock problem found: locking ", id_tag_.left.at(tag_id),
        " (", lock_info, ") when holding ",
        id_tag_.left.at(last_id), " (", last_info, ")");
    unsigned curr = last_id;
    boost::property_map<DepGraph, DepInfo>::type dep_info_map =
        boost::get(DepInfo(), approved_);
    for (;;) {
      int pred = preds[curr];
      if (pred < 0)
        break;
      DepGraph::edge_descriptor e = boost::edge(pred, curr, approved_).first;
      std::pair<std::string, std::string> info = boost::get(dep_info_map, e);
      LOG(warning, Lock, "  ", id_tag_.left.at(pred), " (", info.first, ") -> ",
          id_tag_.left.at(curr), " (", info.second, ")");
      curr = pred;
    }
    LOG(warning, Lock, "  Adding new dependency leads to a cycle.");
    return false;
  }

  void ApproveEdge_(unsigned last_id, unsigned tag_id,
                    const std::string& last_info,
                    const std::string& lock_info) {
    std::pair<DepGraph::edge_descriptor, bool> res =
        boost::add_edge(last_id, tag_id, approved_);
    boost::property_map<DepGraph, DepInfo>::type dep_info_map =
        boost::get(DepInfo(), approved_);
    boost::put(dep_info_map, res.first, std::make_pair(last_info, lock_info));
  }
};

}  // namespace

// For default checker
boost::once_flag init_once; /**< Ensure single initization of def_checker */
boost::scoped_ptr<IMutexChecker> def_checker; /**< Global checker to use */

IMutexChecker* MakeMutexChecker() {
  return new MutexChecker;
}

static void InitDefaultMutexChecker() {
  LOG(notice, Lock, "Mutex lock ordering checker enabled");
  def_checker.reset(MakeMutexChecker());
}

IMutexChecker* GetDefaultMutexChecker() {
  boost::call_once(InitDefaultMutexChecker, init_once);
  return def_checker.get();
}

}  // namespace cpfs
