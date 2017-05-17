#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define common facilities for worker implementations.
 */

#include <stdint.h>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_processor.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "tracker_mapper.hpp"
#include "server/base_server.hpp"
#include "server/ccache_tracker.hpp"
#include "server/worker.hpp"

namespace cpfs {
namespace server {

/**
 * Implementation base type of workers.  Use an internal Fim queue
 * thread to handle Fim's enqueued.  Provide services for interacting
 * with the client cache tracker and the sending of invalidations.
 */
class WorkerCacheInvalidator {
 public:
  WorkerCacheInvalidator() : tracker_mapper_(0), cache_tracker_(0) {}

  /**
   * @param tracker_mapper The tracker mapper to find where to send to
   * clients (for invalidations)
   */
  void SetTrackerMapper(ITrackerMapper* tracker_mapper) {
    tracker_mapper_ = tracker_mapper;
  }

  /**
   * @param cache_tracker The cache tracker to track client cache status
   */
  void SetCCacheTracker(ICCacheTracker* cache_tracker) {
    cache_tracker_ = cache_tracker;
  }

  /**
   * @param cache_inval_func The cache invalidation function to use
   */
  void SetCacheInvalFunc(CacheInvalFunc cache_inval_func) {
    cache_inval_func_ = cache_inval_func;
  }

  /**
   * Invalidate clients currently caching an inode.
   *
   * @param inode The inode.
   *
   * @param clear_page Whether to ask the clients to clear the
   * data associated to the inode as well.
   *
   * @param except A client number which is excluded from the
   * invalidation requests.  May use kNotClient if no client is to be
   * excempted.
   */
  void InvalidateClients(InodeNum inode, bool clear_page, ClientNum except) {
    if (cache_inval_func_)
      cache_inval_func_(inode, clear_page);
    cache_tracker_->InvGetClients(
        inode,
        except,
        boost::bind(&WorkerCacheInvalidator::SendInvalidate,
                    this, _1, _2, clear_page));
  }

 protected:
  ITrackerMapper* tracker_mapper_; /**< Map client numbers to senders */
  ICCacheTracker* cache_tracker_; /**< Track client cache */
  CacheInvalFunc cache_inval_func_; /**< Cache inval function */

 private:
  /**
   * Send invalidation Fim's to a client currently caching an inode.
   *
   * @param inode_num The inode to invalidate.
   *
   * @param client_num The client to receive invalidation.
   *
   * @param clear_page Whether cached pages associated with the inode
   * is to be cleared as well.
   */
  void SendInvalidate(InodeNum inode_num, ClientNum client_num,
                      bool clear_page) {
    FIM_PTR<InvInodeFim> inv_fim = InvInodeFim::MakePtr();
    (*inv_fim)->inode = inode_num;
    (*inv_fim)->clear_page = clear_page;
    boost::shared_ptr<IFimSocket> fim_socket =
        tracker_mapper_->GetFCFimSocket(client_num);
    LOG(debug, Cache, "SendInvalidate: ", PHex(inode_num),
        ":", PINT(clear_page));
    if (fim_socket)
      fim_socket->WriteMsg(inv_fim);
  }
};

/**
 * Base worker providing common functionalities.
 */
class BaseWorker : public IWorker {
 public:
  BaseWorker() : server_(0), data_mutex_(MUTEX_INIT), next_memo_id_(0) {}

  /**
   * @param server The server using the worker
   */
  void set_server(BaseCpfsServer* server) {
    server_ = server;
    cache_invalidator_.SetTrackerMapper(server_->tracker_mapper());
    cache_invalidator_.SetCCacheTracker(server_->cache_tracker());
  }

  /**
   * @return The server using the worker
   */
  BaseCpfsServer* server() {
    return server_;
  }

  void SetQueuer(IFimProcessor* queuer) {
    queuer_ = queuer;
  }

  /**
   * Enqueue a Fim
   *
   * @param fim The fim received
   *
   * @param socket The socket receiving the fim
   *
   * @return Whether the handling is enqueued
   */
  bool Enqueue(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket) {
    return queuer_->Process(fim, socket);
  }

  /**
   * @param cache_inval_func The cache invalidation function to
   * use, for unit tests only
   */
  void SetCacheInvalFunc(CacheInvalFunc cache_inval_func) {
    cache_invalidator_.SetCacheInvalFunc(cache_inval_func);
  }

  /**
   * Get an unused memo id.
   */
  uint64_t GetMemoId() {
    MUTEX_LOCK_GUARD(data_mutex_);
    return ++next_memo_id_;
  }

  /**
   * Put worker memo.
   *
   * @param id The internal call info id, should be obtained by
   * GetMemoId
   *
   * @param info The info to put
   */
  void PutMemo(uint64_t id, boost::shared_ptr<WorkerMemo> info) {
    MUTEX_LOCK_GUARD(data_mutex_);
    memo_map_[id] = info;
  }

  /**
   * Get worker memo.  Once obtained, the memo is removed from the
   * worker.
   */
  boost::shared_ptr<WorkerMemo> GetMemo(uint64_t id) {
    boost::shared_ptr<WorkerMemo> ret;
    {  // Avoid coverage false positive
      MUTEX_LOCK_GUARD(data_mutex_);
      ret = memo_map_[id];
      memo_map_.erase(id);
    }
    return ret;
  }

 protected:
  /**
   * The cache invalidation function to used, for unit tests only
   */
  WorkerCacheInvalidator cache_invalidator_;

 private:
  BaseCpfsServer* server_; /**< The server using the worker */
  IFimProcessor* queuer_; /**< Fim processor to enqueue Fims */
  MUTEX_TYPE data_mutex_;  /**< Protect the following */
  uint64_t next_memo_id_; /**< Next memo id */
  boost::unordered_map<uint64_t, boost::shared_ptr<WorkerMemo> > memo_map_;
};

}  // namespace server
}  // namespace cpfs
