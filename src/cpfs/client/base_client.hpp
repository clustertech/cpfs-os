#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define base class for client.  It provides getters and setters for
 * client components, and data fields for storing them, but nothing
 * more.
 */

#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/unordered_map.hpp>

#include "asio_common.hpp"
#include "common.hpp"
#include "dsg_state.hpp"
#include "mutex_util.hpp"
#include "service.hpp"
#include "status_dumper.hpp"

#include "client/api_common.hpp"
#include "client/fs_common_lowlevel.hpp"

namespace cpfs {

class IAuthenticator;
class IConnector;
class IFimProcessor;
class IIOServiceRunner;
class IAsioPolicy;
class IOpCompletionCheckerSet;
class IShutdownMgr;
class IThreadFimProcessor;
class ITrackerMapper;

namespace client {

class ICacheMgr;
class ICleaner;
class IConnMgr;
class IInodeUsageSet;

/**
 * Base interface for CPFS client.
 */
class BaseClient : public IService {
 public:
  // Don't generate any code except at base_client.cpp
  BaseClient();
  virtual ~BaseClient();

  // IService API
  void Init();
  int Run();
  void Shutdown();

  /**
   * @return Whether the client is shutting down.
   */
  bool IsShuttingDown();

  /**
   * @param asio_policy Set the asio policy to perform async operations
   */
  void set_asio_policy(IAsioPolicy* asio_policy);
  /**
   * @return The asio policy to perform async operations
   */
  IAsioPolicy* asio_policy();

  /**
   * Set the runner to run io_service.
   *
   * @param service_runner The runner
   */
  void set_service_runner(IIOServiceRunner* service_runner);
  /**
   * @return The runner to run io_service
   */
  IIOServiceRunner* service_runner();

  /**
   * Set the mapper for finding connections.
   *
   * @param tracker_mapper The mapper
   */
  void set_tracker_mapper(ITrackerMapper* tracker_mapper);
  /**
   * @return The mapper for finding connection
   */
  ITrackerMapper* tracker_mapper();

  /**
   * Set the connector to make connections.
   *
   * @param connector The connector
   */
  void set_connector(IConnector* connector);
  /**
   * @return The connector to make connections
   */
  IConnector* connector();

  /**
   * Set the Fim processor handling MS Fims, if it is a non-thread fim processor.
   *
   * @param ms_fim_processor The Fim processor
   */
  void set_ms_fim_processor(IFimProcessor* ms_fim_processor);
  /**
   * @return The Fim processor handling MS Fims (whether threaded or not)
   */
  IFimProcessor* ms_fim_processor();

  /**
   * Set the Fim processor handling MS Fims, if it is a thread fim processor.
   *
   * @param ms_fim_processor The Fim processor
   */
  void set_ms_fim_processor_thread(IThreadFimProcessor* ms_fim_processor);
  /**
   * @return The Fim processor handling MS Fims, if it is a thread fim processor.
   */
  IThreadFimProcessor* ms_fim_processor_thread();

  /**
   * Set the Fim processor handling DS Fims.
   *
   * @param ds_fim_processor The Fim processor
   */
  void set_ds_fim_processor(IFimProcessor* ds_fim_processor);
  /**
   * @return The Fim processor handling DS Fims
   */
  IFimProcessor* ds_fim_processor();

  /**
   * Set the manager to connect servers.
   *
   * @param conn_mgr The manager
   */
  void set_conn_mgr(IConnMgr* conn_mgr);
  /**
   * @return The manager to connect servers
   */
  IConnMgr* conn_mgr();

  /**
   * Set the runner to serve FS or Admin requests.
   *
   * @param runner The runner
   */
  void set_runner(IService* runner);
  /**
   * @return The runner to serve FS or Admin requests
   */
  IService* runner();

  /**
   * @param authenticator The connection authenticator
   */
  void set_authenticator(IAuthenticator* authenticator);
  /**
   * @return The connection authenticator
   */
  IAuthenticator* authenticator();

  /**
   * Set the client number of the client.
   *
   * @param client_num The client number
   */
  void set_client_num(ClientNum client_num);

  /**
   * @return The client number of the client
   */
  ClientNum client_num();

  /**
   * Set request freeze state.
   *
   * If enabled, all new FUSE requests will block until the state is
   * disabled.
   *
   * @param freeze Whether to freeze requests
   */
  void SetReqFreeze(bool freeze);

  /**
   * Block until request freeze state is disabled.
   *
   * Return immediately if freeze is currently disabled.
   *
   * @param lock The lock to hold once the freeze is over.  While the
   * lock is not destroyed, request freeze setting will block.
   */
  void ReqFreezeWait(boost::shared_lock<boost::shared_mutex>* lock);

  /**
   * Record new state for a DS group.
   *
   * @param group The group to record new state
   *
   * @param state The new state
   *
   * @param failed The failed role in case state == kDSGDegraded
   *
   * @return The old state
   */
  DSGroupState set_dsg_state(GroupId group, DSGroupState state,
                             GroupRole failed);

  /**
   * Get current state of a DS group
   *
   * @param group The group to record new state
   *
   * @param failed_ret Where to return the failed role
   *
   * @return The state
   */
  DSGroupState dsg_state(GroupId group, GroupRole* failed_ret);

 protected:
  // I/O service
  /** The asio policy used */
  boost::scoped_ptr<IAsioPolicy> asio_policy_;
  boost::scoped_ptr<IOService> service_; /**< The main io_service */
  boost::scoped_ptr<IIOServiceRunner> service_runner_; /**< Run io_service */

  // Connection
  boost::scoped_ptr<ITrackerMapper> tracker_mapper_; /**< Find connection */
  boost::scoped_ptr<IConnector> connector_; /**< Make connections */
  /** MS handler if thread is not used */
  boost::scoped_ptr<IFimProcessor> ms_fim_processor_;
  /** MS handler if thread is used */
  boost::scoped_ptr<IThreadFimProcessor> ms_fim_processor_thread_;
  boost::scoped_ptr<IFimProcessor> ds_fim_processor_; /**< DS handler */
  boost::scoped_ptr<IConnMgr> conn_mgr_; /**< Connect servers */

  // Client processing
  boost::scoped_ptr<IService> runner_; /**< Serve requests */

  boost::scoped_ptr<IAuthenticator> authenticator_;  /**< Authenticator */

  // Internal states
  // TODO(Isaac): Protect with rw-lock for multi-thread access (race?)
  ClientNum client_num_; /**< Client number */
  boost::shared_mutex freeze_mutex_;  /**< Protect frozen_ */
  bool frozen_;  /**< Whether DSC request freeze is active */
  bool shutting_down_; /**< Whether Shutdown() is called */
  /** Type for the recorded states a DS group */
  struct DSGroupInfo {
    DSGroupState state; /**< The DS group state */
    GroupRole failed; /**< The failed DS role in the DS group */
  };
  /** The recorded states of DS groups */
  boost::unordered_map<GroupId,  DSGroupInfo> group_states_;
};

/**
 * Base interface for CPFS filesystem FUSE client.
 */
class BaseFSClient : public BaseClient, public IStatusDumpable {
 public:
  // Don't generate any code except at base_client.cpp
  BaseFSClient();
  ~BaseFSClient();

  // IStatusDumpable API
  void Dump();

  /**
   * Set the cache manager linking with kernel cache.
   *
   * @param cache_mgr The cache manager
   */
  void set_cache_mgr(ICacheMgr* cache_mgr);
  /**
   * @return The cache manager linking with kernel cache
   */
  ICacheMgr* cache_mgr();

  /**
   * Set the usage object to track opened files.
   *
   * @param inode_usage_set The usage object
   */
  void set_inode_usage_set(IInodeUsageSet* inode_usage_set);
  /**
   * @return The inode object to track opened files
   */
  IInodeUsageSet* inode_usage_set();

  /**
   * Set the completion checker set to track incomplete operations.
   *
   * @param checker_set The checker set
   */
  void set_op_completion_checker_set(IOpCompletionCheckerSet* checker_set);
  /**
   * @return The completion checker set to track incomplete operations
   */
  IOpCompletionCheckerSet* op_completion_checker_set();

  /**
   * Set the cleaner to clean cache mgr.
   *
   * @param cleaner The cleaner
   */
  void set_cleaner(ICleaner* cleaner);
  /**
   * @return The cleaner to clean cache mgr
   */
  ICleaner* cleaner();

  /**
   * Set manager to use for shutdown
   */
  void set_shutdown_mgr(IShutdownMgr* shutdown_mgr);
  /**
   * @return The shutdown manager to use
   */
  IShutdownMgr* shutdown_mgr();

  /**
   * @param status_dumper The status dumper to use
   */
  void set_status_dumper(IStatusDumper* status_dumper);
  /**
   * @return The status dumper used
   */
  IStatusDumper* status_dumper();

 private:
  // Client processing
  boost::scoped_ptr<ICacheMgr> cache_mgr_; /**< Link with kernel cache */
  boost::scoped_ptr<IInodeUsageSet> inode_usage_set_; /**< Track opened files */
  /** Track incomplete operations */
  boost::scoped_ptr<IOpCompletionCheckerSet> op_completion_checker_set_;
  boost::scoped_ptr<ICleaner> cleaner_; /**< Clean cache mgr */

  // Shutdown
  boost::scoped_ptr<IShutdownMgr> shutdown_mgr_;  /**< Handle shutdown */

  // Status dump
  boost::scoped_ptr<IStatusDumper> status_dumper_; /**< Dump current status */
};

/**
 * Base interface for CPFS API client.
 */
class BaseAPIClient : public BaseFSClient {
 public:
  /**
   * Set the low-level filesystem layer
   */
  void set_fs_ll(IFSCommonLL* fs_ll);
  /**
   * Get the low-level filesystem layer
   */
  IFSCommonLL* fs_ll();
  /**
   * Set the filesystem common
   */
  void set_api_common(IAPICommon* api_common);
  /**
   * Get the filesystem common
   */
  IAPICommon* api_common();

  /**
   * Set the FSAsyncRWExecutor
   */
  void set_fs_async_rw(IFSAsyncRWExecutor* fs_async_rw);
  /**
   * Get the FSAsyncRWExecutor
   */
  IFSAsyncRWExecutor* fs_async_rw();

 private:
  boost::scoped_ptr<IFSCommonLL> fs_ll_;  /*< The low level filesystem */
  boost::scoped_ptr<IAPICommon> api_common_;  /*< Filesystem common layer */
  boost::scoped_ptr<IFSAsyncRWExecutor> fs_async_rw_;  /*< FS Async executor */
};

/**
 * Base interface for CPFS Admin client.
 */
class BaseAdminClient : public BaseClient {
 public:
  // Don't generate any code except at base_client.cpp
  BaseAdminClient();
  ~BaseAdminClient();

  /**
   * Set whether the client is ready to process requests
   *
   * @param ready Ready or not
   */
  void SetReady(bool ready);

  /**
   * Blocking wait until client is ready to process requests or timeout occur
   *
   * @return Whether client is ready to process requests
   */
  bool WaitReady(double timeout);

 private:
  MUTEX_TYPE data_mutex_; /**< Protect data structure below */
  boost::condition_variable ready_cond_;  /**< Condition for ready */
  bool ready_;  /**< Whether Admin is ready */
};

}  // namespace client
}  // namespace cpfs
