/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of tracer.
 */

#include "tracer_impl.hpp"

#include <stdint.h>

#include <algorithm>
#include <ctime>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include <boost/atomic.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/tss.hpp>

#include "common.hpp"
#include "mutex_util.hpp"
#include "tracer.hpp"

namespace cpfs {

namespace {

/** Number of recent operations to keep in memory */
const uint64_t kNumRecentOps = 64 * 1024;  // Better be a power of 2

/**
 * Trace information.  The structure is designed to be accessed
 * concurrently, but with minimal CPU overheads: all fields are
 * atomic, but are only accessed (by default) using relaxed memory
 * operations.  This prevents the compiler from re-ordering TraceInfo
 * operations with other atomic operations, and make it impossible for
 * each field read to get partially written contents.  But the info
 * itself can be partial: it is possible for a copy construction or
 * assignment to get a field updated by Set() and another not updated.
 * The design is such that updates can be done without a mutex lock.
 */
struct TraceInfo {
  // Really a char*, but make it void* to sidestep boost::atomic warning
  boost::atomic<const void*> method;  /**< The method name */
  boost::atomic<InodeNum> inode;  /**< The inode number */
  boost::atomic<ClientNum> client;  /**< The client number */
  boost::atomic<time_t> time_added;  /**< The time this info is created */
  boost::atomic<boost::thread::id> th_id;  /**< The thread ID */

  TraceInfo() : method(0), inode(0), client(0), time_added(0) {}

  /**
   * @param other The info to copy from
   */
  TraceInfo(const TraceInfo& other) {
    method.store(other.method.load(boost::memory_order_relaxed),
                 boost::memory_order_relaxed);
    inode.store(other.inode.load(boost::memory_order_relaxed),
                boost::memory_order_relaxed);
    client.store(other.client.load(boost::memory_order_relaxed),
                 boost::memory_order_relaxed);
    time_added.store(other.time_added.load(boost::memory_order_relaxed),
                     boost::memory_order_relaxed);
    th_id.store(other.th_id.load(boost::memory_order_relaxed),
                boost::memory_order_relaxed);
  }

  /**
   * @param other The info to copy from
   */
  TraceInfo& operator=(const TraceInfo& other) {
    method.store(other.method.load(boost::memory_order_relaxed),
                 boost::memory_order_relaxed);
    inode.store(other.inode.load(boost::memory_order_relaxed),
                boost::memory_order_relaxed);
    client.store(other.client.load(boost::memory_order_relaxed),
                 boost::memory_order_relaxed);
    time_added.store(other.time_added.load(boost::memory_order_relaxed),
                     boost::memory_order_relaxed);
    th_id.store(other.th_id.load(boost::memory_order_relaxed),
                boost::memory_order_relaxed);
    return *this;
  }

  /**
   * Set the trace info.
   *
   * @param method_ The method name
   *
   * @param inode_ The inode
   *
   * @param client_ The client number
   *
   * @param th_id_ The thread ID
   */
  void Set(const char* method_, InodeNum inode_,
           ClientNum client_, const boost::thread::id& th_id_) {
    method.store(method_, boost::memory_order_relaxed);
    inode.store(inode_, boost::memory_order_relaxed);
    client.store(client_, boost::memory_order_relaxed);
    th_id.store(th_id_, boost::memory_order_relaxed);
    time_added.store(time(0), boost::memory_order_relaxed);
  }
};

/**
 * Facility for logging recent operations
 */
template <uint64_t ring_buf_size = kNumRecentOps>
class Tracer : public ITracer {
  /* #Rationale#
   *
   * We would like this to be done lock-less, to avoid slowing down
   * the system by the logging code.  Intuitively, Set() in the logger
   * writes data to a ring buffer.  Each time a log is to be made, an
   * atomic variable is incremented, and the old value is treated as
   * an index into the ring buffer (with wrap around), so that each of
   * them writes to its own location.  When dumping, we copy the ring
   * buffer to a local buffer, and trim those values which may not be
   * completely written or may be overwritten by later Set(), possibly
   * due to compiler or CPU reordering.
   *
   * The logger needs to inform the dumper that it is in the process of
   * logging, so that partial values will not be dumped.  Such counts
   * are represented by a thread-local atomic long integer.  In
   * contrast, the global count (gcnt below) only requires a normal
   * atomic long integer.
   *
   * The detailed algorithm is as follows.
   *
   * gcnt: The global counter for calculating offset in circular buffer
   * cnt[]: Array of counter values currently used by threads
   *
   * <Logger thread t: Log()> <Dumper thread: DumpAll()>
   * (0) i = gcnt (CST)
   * (1) cnt[t] = i (REL)     (a) end_cnt = gcnt (CST)
   * (2) j = gcnt++ (CST)     (b) end_cnt = min(end_cnt, min among cnt[] (ACQ))
   * (3) cnt[t] = j (REL)     (c) copy
   * (4) Set(j, <data>)       (d) start_cnt = max(0, gcnt - size)
   * (5) cnt[t] = inf (REL)
   *
   * We argue that the range [start_cnt, end_cnt - 1] is always safe
   * to dump.  In other words, if during a run of the dumper thread,
   * start_cnt <= p <= end_cnt - 1, then Set(p) happens before the
   * copy, which in turns happens before Set(q) for all q >= start_cnt
   * + size (if ever called).
   *
   * Claim 1: For any p within [start_cnt, end_cnt - 1], one thread runs
   * Set(p), and it happens before the copy at (c).
   *
   * Proof: Because p is less than end_cnt, which in turn is at most
   * the gcnt value at some time, it is clear that Set(p) is to be
   * executed.  Because each Set(j, <data>) is associated with a
   * different j due to the atomic increment at (2), exactly one
   * thread runs Set(p), say t.  Consider the value obtained by the
   * dumper thread at (b) for thread t.  If it obtains a value other
   * than inf, it must be the value written by thread t at (1) or (3),
   * coming from (0) or (2) respectively.  Since the value is at least
   * end_cnt which is greater than p, Set(p) happens before this
   * write, which in turn happens before the copy at (c) due to the
   * RELEASE-ACQUIRE guarantee.  If it obtains inf, it must be set by
   * thread t at (5), and the associated Set(j, <data>) at (4) happens
   * before (c).  So the remaining case to consider is that Set(p)
   * happens at a run of the same logger thread in a future
   * invocation.  But even in this case, (2) must happen before (a)
   * due to the CST guarantee of gcnt (and the fact that (a) obtained
   * a count larger than p).  The chain (1) happens before (2) happens
   * before (a) happens before (b) implies that the count obtained for
   * thread t at (b) should not be inf.
   *
   * Claim 2: For any q >= start_cnt + size, the copy (c) happens
   * before Set(q), if the latter exists.
   *
   * Proof: If a thread runs Set(q), at (2) it sets gcnt to be q + 1.
   * Let g be the gcnt obtained by the dumper at (d).  Because q + 1
   * >= start_cnt + size + 1 > start_cnt + size >= g, (d) happens
   * before (2) according to the CST guarantee.  Our claim follows by
   * the chain (c) happens before (d) happens before (2) happens
   * before (4).
   */
 public:
  /**
   * Construct the tracer
   */
  explicit Tracer()
      : gcnt_(0), logs_(new TraceInfo[ring_buf_size]),
        data_mutex_(MUTEX_INIT) {}

  /**
   * Log the trace information
   *
   * @param method The method executing
   *
   * @param inode The inode number associated
   *
   * @param client The client number associated
   */
  void Log(const char* method, InodeNum inode, ClientNum client) {
    ThreadInfo* th_info_ptr = th_info_.get();
    if (!th_info_ptr) {
      th_info_.reset(new ThreadInfo(this));
      th_info_ptr = th_info_.get();
    }
    th_info_ptr->cnt.store(gcnt_, boost::memory_order_release);
    uint64_t j = gcnt_++;
    th_info_ptr->cnt.store(j, boost::memory_order_release);
    logs_[j % ring_buf_size].Set(method, inode, client, th_info_ptr->th_id);
    th_info_ptr->cnt.store(std::numeric_limits<uint64_t>::max(),
                           boost::memory_order_release);
  }

  /**
   * Dump operations tracked
   *
   * @return Vector of trace information in string
   */
  std::vector<std::string> DumpAll() const {
    std::vector<std::string> ret;
    {
      uint64_t end_cnt = gcnt_;
      {
        MUTEX_LOCK_GUARD(data_mutex_);
        BOOST_FOREACH(const ThreadInfo* info, th_info_set_) {
          end_cnt = std::min(end_cnt,
                             info->cnt.load(boost::memory_order_acquire));
        }
      }
      boost::scoped_array<TraceInfo> clone(new TraceInfo[ring_buf_size]);
      for (unsigned i = 0; i < ring_buf_size; ++i)
        clone[i] = logs_[i];
      uint64_t start_cnt = gcnt_;
      start_cnt = start_cnt > ring_buf_size ? start_cnt - ring_buf_size : 0;
      {
        for (uint64_t pos = start_cnt; pos < end_cnt; ++pos) {
          TraceInfo ti = clone[pos % ring_buf_size];
          struct tm time_ret;
          char time_str[64];
          time_t time_added = ti.time_added;
          localtime_r(&time_added, &time_ret);
          strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &time_ret);
          std::string msg;
          msg = (boost::format("%s %s %s() inode: %d, client: %d") %
                 boost::lexical_cast<std::string>(ti.th_id) % time_str %
                 reinterpret_cast<const char*>(ti.method.load()) %
                 ti.inode % ti.client).str();
          ret.push_back(msg);
        }
      }
    }
    return ret;
  }

 private:
  boost::atomic<uint64_t> gcnt_;  /**< The global count */
  boost::scoped_array<TraceInfo> logs_;  /**< Array of logs */

  /**
   * Thread information, track per-thread counts that is being
   * logged.
   */
  struct ThreadInfo {
    Tracer* tracer;  /**< The pointer to tracer info referring to */
    boost::atomic<uint64_t> cnt;  /**< The count */
    boost::thread::id th_id;  /**< The thread ID */

    explicit ThreadInfo(Tracer* tracer_)
        : tracer(tracer_), cnt(std::numeric_limits<uint64_t>::max()) {
      th_id = boost::this_thread::get_id();
      tracer->RegisterThread_(this);
    }
    ~ThreadInfo() {
      tracer->UnregisterThread_(this);
    }
  };

  mutable MUTEX_TYPE data_mutex_;  /**< Protect field below */
  typedef std::set<ThreadInfo*> ThreadInfoSet;  /**< Thread info type */
  ThreadInfoSet th_info_set_;  /**< All thread info */

  /**
   * Thread info of the current thread.
   */
  boost::thread_specific_ptr<ThreadInfo> th_info_;

  /**
   * Called during thread_specific_ptr construction.
   */
  void RegisterThread_(ThreadInfo* info) {
    MUTEX_LOCK_GUARD(data_mutex_);
    th_info_set_.insert(info);
  }

  /**
   * Called during thread_specific_ptr destruction.
   */
  void UnregisterThread_(ThreadInfo* info) {
    MUTEX_LOCK_GUARD(data_mutex_);
    th_info_set_.erase(info);
  }
};

}  // namespace

ITracer* MakeTracer() {
  return new Tracer<>();
}

ITracer* MakeSmallTracer() {
  return new Tracer<5>();
}

}  // namespace cpfs
