#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define base class for meta servers, as well as the actual meta
 * server.  The base class provide getters and setters for server
 * components, and data fields for storing them.  The actual class
 * adds the initialization and run code.
 */

#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"
#include "mutex_util.hpp"
#include "server/base_server.hpp"

namespace cpfs {

class ConfigMgr;
class IFimProcessor;
class IOpCompletionCheckerSet;
class IThreadFimProcessor;
class ITimeKeeper;

namespace server {
namespace ms {

/**
 * Represent the operational state of a DS group.
 *
 * At present it only keeps which inodes are currently resyncing and
 * thus cannot be truncated yet.
 */
struct DSGOpsState {
  boost::shared_mutex data_mutex; /**< reader-writer lock for fields below */
  boost::unordered_set<InodeNum> resyncing; /**< inodes is being resynced */
};

class IAttrUpdater;
class ICleaner;
class IConnMgr;
class IDSGAllocator;
class IDSLocker;
class IDSQueryMgr;
class IDirtyInodeMgr;
class IFailoverMgr;
class IHACounter;
class IInodeSrc;
class IInodeUsage;
class IMetaDirReader;
class IReplier;
class IResyncMgr;
class IStartupMgr;
class IStatKeeper;
class IStateMgr;
class IStore;
class ITopologyMgr;
class IUgidHandler;

/**
 * Interface for meta data servers.
 */
class BaseMetaServer : public BaseCpfsServer {
 public:
  /**
   * @param configs The config items to use
   */
  explicit BaseMetaServer(const ConfigMgr& configs);
  ~BaseMetaServer();  // Don't generate destructor except at base_server.cpp

  /**
   * @return Whether MS is running in HA mode
   */
  bool IsHAMode() const;

  /**
   * Start the server in activated state.
   *
   * If the server is started in active state, it runs the first time
   * when the server starts allowing DS and FC connections.  Note that
   * this is used only if the server starts up and becomes active
   * without becoming a slave first.  The failover case uses
   * SwitchActive_ in failover_mgr_impl.cpp instead.
   */
  virtual void StartServerActivated();

  /**
   * Prepare to activate the server.
   *
   * At the moment, it cleans all inactive unclean entries in the
   * dirty inode manager, and starts MS permission checks if configured.
   */
  virtual void PrepareActivate();

  /**
   * @param store The store to use
   */
  void set_store(IStore* store);
  /**
   * @return The store used
   */
  IStore* store();

  /**
   * @param conn_mgr The connection manager to use for secondary MS
   * connection setup.
   */
  void set_conn_mgr(IConnMgr* conn_mgr);
  /**
   * @return The connector used for MS connection setup
   */
  IConnMgr* conn_mgr();

  /**
   * @param startup_mgr The startup manager to determine startup behavior
   */
  void set_startup_mgr(IStartupMgr* startup_mgr);
  /**
   * @return The startup manager used for determining startup behavior
   */
  IStartupMgr* startup_mgr();

  /**
   * @param ugid_handler The user and group id handler to use
   */
  void set_ugid_handler(IUgidHandler* ugid_handler);
  /**
   * @return The user and group id handler used
   */
  IUgidHandler* ugid_handler();

  /**
   * @param dsg_allocator The server group allocator to use
   */
  void set_dsg_allocator(IDSGAllocator* dsg_allocator);
  /**
   * @return The server group allocator used
   */
  IDSGAllocator* dsg_allocator();

  /**
   * @param inode_src The inode source to use
   */
  void set_inode_src(IInodeSrc* inode_src);
  /**
   * @return The inode source used
   */
  IInodeSrc* inode_src();

  /**
   * @param ds_locker The DS locker to use
   */
  void set_ds_locker(IDSLocker* ds_locker);
  /**
   * @return The DS locker used
   */
  IDSLocker* ds_locker();

  /**
   * @param inode_usage The inode usage to use
   */
  void set_inode_usage(IInodeUsage* inode_usage);
  /**
   * @return The inode usage
   */
  IInodeUsage* inode_usage();

  /**
   * @param replier The replier to use
   */
  void set_replier(IReplier* replier);
  /**
   * @return The replier used
   */
  IReplier* replier();

  /**
   * @param cleaner The meta server cleaner to use
   */
  void set_cleaner(ICleaner* cleaner);
  /**
   * @return The meta server cleaner
   */
  ICleaner* cleaner();

  /**
   * @param ha_counter The HA counter to use
   */
  void set_ha_counter(IHACounter* ha_counter);
  /**
   * @return The counter
   */
  IHACounter* ha_counter();

  /**
   * @param state_mgr The state manager to use
   */
  void set_state_mgr(IStateMgr* state_mgr);
  /**
   * @return The state manager
   */
  IStateMgr* state_mgr();

  /**
   * @param topology_mgr The topology manager to use
   */
  void set_topology_mgr(ITopologyMgr* topology_mgr);
  /**
   * @return The topology manager used
   */
  ITopologyMgr* topology_mgr();

  /**
   * @param proc The processor for use during failover
   */
  void set_failover_processor(IThreadFimProcessor* proc);
  /**
   * @return Failover processor
   */
  IThreadFimProcessor* failover_processor();

  /**
   * @param mgr The failover manager to use
   */
  void set_failover_mgr(IFailoverMgr* mgr);
  /**
   * @return Failover manager
   */
  IFailoverMgr* failover_mgr();

  /**
   * @param mgr The resync manager to use
   */
  void set_resync_mgr(IResyncMgr* mgr);
  /**
   * @return Resync manager
   */
  IResyncMgr* resync_mgr();

  /**
   * @param checker_set The completion checker set for DS operations
   */
  void set_ds_completion_checker_set(IOpCompletionCheckerSet* checker_set);
  /**
   * @return Completion checker set for DS operations
   */
  IOpCompletionCheckerSet* ds_completion_checker_set();

  /**
   * @param meta_dir_reader The meta directory reader to use
   */
  void set_meta_dir_reader(IMetaDirReader* meta_dir_reader);
  /**
   * @return Meta directory reader
   */
  IMetaDirReader* meta_dir_reader();

  /**
   * @param time_keeper Time keeper for peer MS
   */
  void set_peer_time_keeper(ITimeKeeper* time_keeper);
  /**
   * @return Time keeper for peer MS
   */
  ITimeKeeper* peer_time_keeper();

  /**
   * @param ds_query_mgr The DS query manager to use
   */
  void set_ds_query_mgr(IDSQueryMgr* ds_query_mgr);
  /**
   * @return The DS query manager
   */
  IDSQueryMgr* ds_query_mgr();

  /**
   * @param stat_keeper The stat keeper to use
   */
  void set_stat_keeper(IStatKeeper* stat_keeper);
  /**
   * @return The stat keeper used
   */
  IStatKeeper* stat_keeper();

  /**
   * @param proc The stat processor to use
   */
  void set_stat_processor(IThreadFimProcessor* proc);
  /**
   * @return Stat processor
   */
  IThreadFimProcessor* stat_processor();

  /**
   * @param dirty_inode_mgr The dirty inode manager to use
   */
  void set_dirty_inode_mgr(IDirtyInodeMgr* dirty_inode_mgr);
  /**
   * @return The dirty inode manager
   */
  IDirtyInodeMgr* dirty_inode_mgr();

  /**
   * @param attr_updater The dirty attribute updater to use
   */
  void set_attr_updater(IAttrUpdater* attr_updater);
  /**
   * @return The dirty attribute updater used
   */
  IAttrUpdater* attr_updater();

  /**
   * @param proc Set the processor for Admin FIMs
   */
  void set_admin_fim_processor(IFimProcessor* proc);
  /**
   * @return CLI fim processor
   */
  IFimProcessor* admin_fim_processor();

  /**
   * @return The counter variable storing number of clients connected so far
   */
  ClientNum& client_num_counter();

  /**
   * Prepare for reading of DSG operations state.
   *
   * @param group The group to read the operation state
   *
   * @param lock The lock to keep others from writing to the DSG ops state
   */
  void ReadLockDSGOpState(GroupId group,
                          boost::shared_lock<boost::shared_mutex>* lock);

  /**
   * Set the inodes to be resyncing.
   *
   * @param group The group to set the pending inodes
   *
   * @param inodes The inodes resyncing, cleared after the call
   */
  void set_dsg_inodes_resyncing(GroupId group,
                                const std::vector<InodeNum>& inodes);

  /**
   * Return whether an inode is to be resync'ed
   *
   * This function should be called with the LockDSOpState read lock
   * held (see ReadLockDSOpState()).
   *
   * @param group The group to check for inode resync
   *
   * @param inode The inode to check
   *
   * @return Whether the inode is to be resync'ed
   */
  bool is_dsg_inode_resyncing(GroupId group, InodeNum inode);

 protected:
  /** Manipulate data directory of meta server */
  boost::scoped_ptr<IStore> store_;
  /** The connection manager for secondary MS connection setup */
  boost::scoped_ptr<IConnMgr> conn_mgr_;
  /** Determine startup behaviors */
  boost::scoped_ptr<IStartupMgr> startup_mgr_;
  /** Handle permission switching */
  boost::scoped_ptr<IUgidHandler> ugid_handler_;
  /** Allocate server groups to new inodes */
  boost::scoped_ptr<IDSGAllocator> dsg_allocator_;
  /** Source of inode numbers for each client to allocate */
  boost::scoped_ptr<IInodeSrc> inode_src_;
  /** Helper to lock DS */
  boost::scoped_ptr<IDSLocker> ds_locker_;
  /** Track inode usage */
  boost::scoped_ptr<IInodeUsage> inode_usage_;
  /** Reply to requests handling replications */
  boost::scoped_ptr<IReplier> replier_;
  /** Clean up server */
  boost::scoped_ptr<ICleaner> cleaner_;
  /** THe HA counter */
  boost::scoped_ptr<IHACounter> ha_counter_;
  /** The HA state */
  boost::scoped_ptr<IStateMgr> state_mgr_;
  /** The topology manager */
  boost::scoped_ptr<ITopologyMgr> topology_mgr_;
  /** Failover processor */
  boost::scoped_ptr<IThreadFimProcessor> failover_processor_;
  /** Failover manager */
  boost::scoped_ptr<IFailoverMgr> failover_mgr_;
  /** Resync manager */
  boost::scoped_ptr<IResyncMgr> resync_mgr_;
  /** Completion checker for DS operations */
  boost::scoped_ptr<IOpCompletionCheckerSet> ds_completion_checker_set_;
  /** Meta directory reader */
  boost::scoped_ptr<IMetaDirReader> meta_dir_reader_;
  /** Time keeper for peer MS last seen */
  boost::scoped_ptr<ITimeKeeper> peer_time_keeper_;
  /** The DS query manager */
  boost::scoped_ptr<IDSQueryMgr> ds_query_mgr_;
  /** Fetch and keep stat periodically */
  boost::scoped_ptr<IStatKeeper> stat_keeper_;
  /** Failover processor */
  boost::scoped_ptr<IThreadFimProcessor> stat_processor_;
  /** The dirty inode manager */
  boost::scoped_ptr<IDirtyInodeMgr> dirty_inode_mgr_;
  /** The attribute updater */
  boost::scoped_ptr<IAttrUpdater> attr_updater_;
  /** The Admin FIM processor */
  boost::scoped_ptr<IFimProcessor> admin_fim_processor_;
  ClientNum client_num_counter_; /**< Number of clients connected so far */
  mutable MUTEX_TYPE data_mutex_; /**< Protect fields below */
  boost::unordered_map<GroupId, DSGOpsState>
  ops_states_; /**< DSG operation states */
};

/**
 * Make an implementation of the BaseMetaServer for production.
 */
BaseMetaServer* MakeMetaServer(const ConfigMgr& configs);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
