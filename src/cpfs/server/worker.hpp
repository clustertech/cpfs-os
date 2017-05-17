#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define common facilities for worker implementations.
 */

#include <stdint.h>

#include "fim_processor.hpp"
#include "server/ccache_tracker.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {
namespace server {

class BaseCpfsServer;

/**
 * Worker-memo kept in the worker, indexed by memo ID.  It is useful
 * for non-worker threads (e.g., communication thread running a
 * callback) to put to the map before sending the thread group an
 * internal Fim to trigger the worker thread, so that the memo is
 * available to the worker when the internal Fim is processed.  It is
 * expected to be subclassed to add data to it.
 */
class WorkerMemo {
 public:
  /**
   * @param type The memo type, for verification to ensure that the
   * program won't interpret the Fim wrongly.  Typically the Fim type
   * is used.
   */
  explicit WorkerMemo(int type) : type_(type) {}
  virtual ~WorkerMemo() {}

  /**
   * Get the memo type.
   *
   * @return The memo type
   */
  int type() { return type_; }

  /**
   * Static-cast this memo into another memo type.
   *
   * @param target_type The memo type of the target
   *
   * @return The memo, in another memo type
   */
  template <typename TMemo>
  TMemo* CheckCast(int target_type) {
    if (type() != target_type)
      return 0;
    return static_cast<TMemo*>(this);
  }

 private:
  int type_;
};

/**
 * Worker interface, providing common functionalities.
 */
class IWorker : public IFimProcessor {
 public:
  virtual ~IWorker() {}

  /**
   * @param server The server using the worker
   */
  virtual void set_server(BaseCpfsServer* server) = 0;

  /**
   * @return The server using the worker
   */
  virtual BaseCpfsServer* server() = 0;

  /**
   * @param queuer The Fim processor for enqueueing Fims
   */
  virtual void SetQueuer(IFimProcessor* queuer) = 0;

  /**
   * Queue a Fim
   *
   * @param fim The fim received
   *
   * @param socket The socket receiving the fim
   *
   * @return Whether the handling is completed
   */
  virtual bool Enqueue(const FIM_PTR<IFim>& fim,
                       const boost::shared_ptr<IFimSocket>& socket) = 0;

  /**
   * @param cache_inval_func The cache invalidation callback to
   * use, for unit tests only
   */
  virtual void SetCacheInvalFunc(CacheInvalFunc cache_inval_func) = 0;

  /**
   * Get an unused memo id.
   */
  virtual uint64_t GetMemoId() = 0;

  /**
   * Put worker memo.
   *
   * @param id The internal call info id, should be obtained by
   * GetMemoId
   *
   * @param info The info to put
   */
  virtual void PutMemo(uint64_t id, boost::shared_ptr<WorkerMemo> info) = 0;

  /**
   * Get worker memo.  Once obtained, the memo is removed from the
   * worker.
   */
  virtual boost::shared_ptr<WorkerMemo> GetMemo(uint64_t id) = 0;
};

}  // namespace server
}  // namespace cpfs
