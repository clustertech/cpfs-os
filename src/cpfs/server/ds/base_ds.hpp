#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define base class for data servers, as well as the actual data
 * server.  The base class here provide getters and setters for server
 * components, and data fields for storing them.  The actual class
 * adds the initialization and run code.
 */

#include <stdint.h>

#include <boost/scoped_ptr.hpp>

#include "common.hpp"
#include "dsg_state.hpp"
#include "server/base_server.hpp"

namespace boost {

class shared_mutex;
template <typename Mutex> class shared_lock;
template <typename Mutex> class unique_lock;

}  // namespace boost

namespace cpfs {

class ConfigMgr;
class IAsioPolicy;
class IPosixFS;
class IReqCompletionCheckerSet;
class IThreadFimProcessor;
class ITimeKeeper;

namespace server {
namespace ds {

class ICleaner;
class IConnMgr;
class IDataRecoveryMgr;
class IDegradedCache;
class IResyncMgr;
class IResyncFimProcessor;
class IStore;

/**
 * Interface for data servers.
 */
class BaseDataServer : public BaseCpfsServer {
 public:
  /**
   * @param configs The config items to use
   */
  explicit BaseDataServer(const ConfigMgr& configs);
  ~BaseDataServer();  // Don't generate destructor except at base_server.cpp

  /**
   * @param asio_policy Set the asio policy to perform async
   * operations for DSs
   */
  void set_ds_asio_policy(IAsioPolicy* asio_policy);
  /**
   * @return The asio policy to perform async operations for DS
   */
  IAsioPolicy* ds_asio_policy();

  /**
   * @param posix_fs The PosixFS to use
   */
  void set_posix_fs(IPosixFS* posix_fs);

  /**
   * @return The PosixFS used
   */
  IPosixFS* posix_fs();

  /**
   * @param store The store to use
   */
  void set_store(IStore* store);

  /**
   * @return The store used
   */
  IStore* store();

  /**
   * @param cleaner The cleaner to use
   */
  void set_cleaner(ICleaner* cleaner);

  /**
   * @return The cleaner used
   */
  ICleaner* cleaner();

  /**
   * @param conn_mgr The connection manager to use for DS
   * connection setup
   */
  void set_conn_mgr(IConnMgr* conn_mgr);

  /**
   * @return The connector used for DS connection setup
   */
  IConnMgr* conn_mgr();

  /**
   * @param dsg_ready_time_keeper Time keeper to use for keeping last
   * DSG ready time
   */
  void set_dsg_ready_time_keeper(ITimeKeeper* dsg_ready_time_keeper);

  /**
   * @return Time keeper keeping last DSG ready time
   */
  ITimeKeeper* dsg_ready_time_keeper();

  /**
   * @param degraded_cache The cache to hold recovered data when DS
   * group is degraded
   */
  void set_degraded_cache(IDegradedCache* degraded_cache);

  /**
   * @return The cache used to hold recovered data when DS
   * group is degraded
   */
  IDegradedCache* degraded_cache();

  /**
   * @param data_recovery_mgr The data recovery manager to use
   */
  void set_data_recovery_mgr(IDataRecoveryMgr* data_recovery_mgr);

  /**
   * @return The data recovery manager being used
   */
  IDataRecoveryMgr* data_recovery_mgr();

  /**
   * @param req_completion_checker_set Request completion checker set to use
   */
  void set_req_completion_checker_set(
      IReqCompletionCheckerSet* req_completion_checker_set);

  /**
   * @return The data recovery manager being used
   */
  IReqCompletionCheckerSet* req_completion_checker_set();

  /**
   * @param resync_mgr Manager to use for sending resync Fims
   */
  void set_resync_mgr(IResyncMgr* resync_mgr);

  /**
   * @return Manager used for sending resync Fims
   */
  IResyncMgr* resync_mgr();

  /**
   * @param resync_fim_processor Fim processor to use, set for the
   * duration as a resync receiver
   */
  void set_resync_fim_processor(IResyncFimProcessor* resync_fim_processor);

  /**
   * @return Fim processor used, set for the duration as a resync
   * receiver
   */
  IResyncFimProcessor* resync_fim_processor();

  /**
   * @param resync_thread_processor Thread Fim processor to use for resync
   */
  void set_resync_thread_processor(
      IThreadFimProcessor* resync_thread_processor);

  /**
   * @return Thread Fim processor to use for resync
   */
  IThreadFimProcessor* resync_thread_processor();

  /**
   * @param state_change_id The state change ID leading to the state
   *
   * @param dsg_state The current DSG state
   *
   * @param failed_role The role failed
   *
   * @param lock The lock to keep to prevent DSG state query to
   * complete.  If 0, use an internal lock and release it once
   * completed
   */
  void set_dsg_state(uint64_t state_change_id, DSGroupState dsg_state,
                     GroupRole failed_role,
                     boost::unique_lock<boost::shared_mutex>* lock = 0);

  /**
   * Prevent update of DSG state.
   *
   * @param lock The lock to keep to prevent DSG state update
   */
  void ReadLockDSGState(boost::shared_lock<boost::shared_mutex>* lock);

  /**
   * @param state_change_id_ret Where to return the state change ID
   *
   * @param failed_role_ret Where to return the failed role
   *
   * @return The current DSG state
   */
  DSGroupState dsg_state(uint64_t* state_change_id_ret,
                         GroupRole* failed_role_ret);

  /**
   * @return Whether optimized resync is to be used
   */
  bool opt_resync();

  /**
   * @param opt_resync Whether optimized resync is to be used
   */
  void set_opt_resync(bool opt_resync);

  /**
   * @return Whether server is in distressed mode
   */
  bool distressed();

  /**
   * @param distressed Whether distressed mode is set
   */
  void set_distressed(bool distressed);

 protected:
  /** Do Asio operations to DS */
  boost::scoped_ptr<IAsioPolicy> ds_asio_policy_;
  /** Perform some of the FS operations in mockable way */
  boost::scoped_ptr<IPosixFS> posix_fs_;
  /** Manipulate data directory of data server */
  boost::scoped_ptr<IStore> store_;
  /** Clean inode data */
  boost::scoped_ptr<ICleaner> cleaner_;
  /** Create connections to other data servers */
  boost::scoped_ptr<IConnMgr> conn_mgr_;
  /** Track when is the last time the DSG is fully ready */
  boost::scoped_ptr<ITimeKeeper> dsg_ready_time_keeper_;
  /** The cache used to hold recovered data when DS group is degraded */
  boost::scoped_ptr<IDegradedCache> degraded_cache_;
  /** The data recovery manager being used */
  boost::scoped_ptr<IDataRecoveryMgr> data_recovery_mgr_;
  /** Request completion checker set to use */
  boost::scoped_ptr<IReqCompletionCheckerSet> req_completion_checker_set_;
  /** Send resync Fims */
  boost::scoped_ptr<IResyncMgr> resync_mgr_;
  /** Process DS resync Fims received */
  IResyncFimProcessor* resync_fim_processor_;
  /** Thread for DS resync Fim processing */
  boost::scoped_ptr<IThreadFimProcessor> resync_thread_processor_;
  /** The current DSG state information */
  boost::scoped_ptr<DSGStateInfo> dsg_state_;
  /** Whether to use optimized resync */
  bool opt_resync_;
  /** Whether distressed mode is set */
  bool distressed_;
};

/**
 * Make an implementation of the BaseDataServer for production.
 */
BaseDataServer* MakeDataServer(const ConfigMgr& configs);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
