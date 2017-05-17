#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define base classes for servers.  Classes here provide getters and
 * setters for server components, and data fields for storing them,
 * but nothing more.
 */

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "config_mgr.hpp"
#include "fim_socket.hpp"
#include "service.hpp"
#include "status_dumper.hpp"

namespace cpfs {

class IAsioPolicy;
class IAuthenticator;
class IConnector;
class IFimProcessor;
class IIOServiceRunner;
class IShutdownMgr;
class ITracer;
class ITrackerMapper;

namespace server {

class ICCacheTracker;
class IDurableRange;
class IInodeRemovalTracker;
class IReplySet;
class IServerInfo;
class IThreadGroup;

/**
 * Base interface for CPFS servers.
 */
class BaseCpfsServer : public IService, public IStatusDumpable {
 public:
  /**
   * @param configs The config items to use.
   */
  explicit BaseCpfsServer(const ConfigMgr& configs);
  ~BaseCpfsServer();  // Don't generate destructor except at base_server.cpp
  void Init();
  int Run();
  void Shutdown();
  void Dump();

  /**
   * @return Whether the server is shutting down.
   */
  bool IsShuttingDown();

  /**
   * @return The config items used
   */
  ConfigMgr& configs();
  /**
   * @param tracker_mapper The mapper for ReqTracker to use
   */
  void set_tracker_mapper(ITrackerMapper* tracker_mapper);
  /**
   * @return The tracker mapper used
   */
  ITrackerMapper* tracker_mapper();
  /**
   * @param asio_policy Set the asio policy to perform async operations
   */
  void set_asio_policy(IAsioPolicy* asio_policy);
  /**
   * @return The asio policy to perform async operations
   */
  IAsioPolicy* asio_policy();
  /**
   * @param asio_policy Set the asio policy to perform async
   * operations for FCs
   */
  void set_fc_asio_policy(IAsioPolicy* asio_policy);
  /**
   * @return The asio policy to perform async operations for FCs
   */
  IAsioPolicy* fc_asio_policy();
  /**
   * @param asio_policy Set the asio policy to perform async
   * operations for signal etc
   */
  void set_aux_asio_policy(IAsioPolicy* asio_policy);
  /**
   * @return The asio policy to perform async operations for signal etc
   */
  IAsioPolicy* aux_asio_policy();
  /**
   * @param io_service_runner The IOServiceRunner to use
   */
  void set_io_service_runner(IIOServiceRunner* io_service_runner);
  /**
   * @return The IOServiceRunner used
   */
  IIOServiceRunner* io_service_runner();
  /**
   * @param fim_socket_maker The Fim socket maker to use
   */
  void set_fim_socket_maker(FimSocketMaker fim_socket_maker);
  /**
   * @return The Fim socket maker used
   */
  FimSocketMaker fim_socket_maker();
  /**
   * @param connector The connector to use
   */
  void set_connector(IConnector* connector);
  /**
   * @return The connector used
   */
  IConnector* connector();
  /**
   * @param proc The FimProcessor to use initially
   */
  void set_init_fim_processor(IFimProcessor* proc);
  /**
   * @return The FimProcessor used initially
   */
  IFimProcessor* init_fim_processor();
  /**
   * @param proc The FimProcessor to use when communicating with
   * another MS
   */
  void set_ms_fim_processor(IFimProcessor* proc);
  /**
   * @return The FimProcessor to use when communicating with a MS
   */
  IFimProcessor* ms_fim_processor();
  /**
   * @param proc The FimProcessor to use when communicating with a DS
   */
  void set_ds_fim_processor(IFimProcessor* proc);
  /**
   * @return The FimProcessor to use when communicating with a DS
   */
  IFimProcessor* ds_fim_processor();
  /**
   * @param proc The FimProcessor to use when communicating with a FC
   */
  void set_fc_fim_processor(IFimProcessor* proc);
  /**
   * @return The FimProcessor to use when communicating with a FC
   */
  IFimProcessor* fc_fim_processor();
  /**
   * @param thread_group The worker group to use
   */
  void set_thread_group(IThreadGroup* thread_group);
  /**
   * @return The IOServiceRunner used
   */
  IThreadGroup* thread_group();
  /**
   * @param cache_tracker The cache tracker to use
   */
  void set_cache_tracker(ICCacheTracker* cache_tracker);
  /**
   * @return The cache tracker used.
   */
  ICCacheTracker* cache_tracker();
  /**
   * @param recent_reply_set The recent reply set to use
   */
  void set_recent_reply_set(IReplySet* recent_reply_set);
  /**
   * @return The recent reply set used
   */
  IReplySet* recent_reply_set();
  /**
   * @param server_info The server info to use
   */
  void set_server_info(IServerInfo* server_info);
  /**
   * @return The server info
   */
  IServerInfo* server_info();
  /**
   * @param inode_removal_tracker The inode removal tracker to use
   */
  void set_inode_removal_tracker(IInodeRemovalTracker* inode_removal_tracker);
  /**
   * @return Inode removal tracker
   */
  IInodeRemovalTracker* inode_removal_tracker();
  /**
   * @param shutdown_mgr Shutdown manager
   */
  void set_shutdown_mgr(IShutdownMgr* shutdown_mgr);
  /**
   * @return The shutdown manager used
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

  /**
   * @param authenticator The connection authenticator
   */
  void set_authenticator(IAuthenticator* authenticator);
  /**
   * @return The connection authenticator
   */
  IAuthenticator* authenticator();

  /**
   * @param tracer The tracer
   */
  void set_tracer(ITracer* tracer);
  /**
   * @return The tracer
   */
  ITracer* tracer();

  /**
   * @param durable_range The durable range object to use
   */
  void set_durable_range(IDurableRange* durable_range);
  /**
   * @return The durable range object used
   */
  IDurableRange* durable_range();

 protected:
  ConfigMgr configs_; /**< Configuration specified */
  /** Find sockets and request trackers */
  boost::scoped_ptr<ITrackerMapper> tracker_mapper_;
  /** Do Asio operations */
  boost::scoped_ptr<IAsioPolicy> asio_policy_;
  /** Do Asio operations to FCs */
  boost::scoped_ptr<IAsioPolicy> fc_asio_policy_;
  /** Do Asio operations for internal */
  boost::scoped_ptr<IAsioPolicy> aux_asio_policy_;
  /** Make Fim sockets from raw sockets */
  FimSocketMaker fim_socket_maker_;
  /** Connect to remote targets */
  boost::shared_ptr<IConnector> connector_;
  /** Create thread to run io service */
  boost::scoped_ptr<IIOServiceRunner> io_service_runner_;
  /** FimProcessor to use initially */
  boost::scoped_ptr<IFimProcessor> init_fim_processor_;
  /** FimProcessor when communicating with a MS */
  boost::scoped_ptr<IFimProcessor> ms_fim_processor_;
  /** FimProcessor when communicating with a DS */
  boost::scoped_ptr<IFimProcessor> ds_fim_processor_;
  /** FimProcessor when communicating with a FC */
  boost::scoped_ptr<IFimProcessor> fc_fim_processor_;
  /** Worker group supporting FimProcessor */
  boost::scoped_ptr<IThreadGroup> thread_group_;
  /** Track client cache */
  boost::scoped_ptr<ICCacheTracker> cache_tracker_;
  /** Keep recent replies */
  boost::scoped_ptr<IReplySet> recent_reply_set_;
  /** Server info */
  boost::scoped_ptr<IServerInfo> server_info_;
  /** Inode removal tracker */
  boost::scoped_ptr<IInodeRemovalTracker> inode_removal_tracker_;
  /** Shutdown manager */
  boost::scoped_ptr<IShutdownMgr> shutdown_mgr_;
  /** Status dumper */
  boost::scoped_ptr<IStatusDumper> status_dumper_;
  /** Connection authenticator */
  boost::scoped_ptr<IAuthenticator> authenticator_;
  /** Tracer */
  boost::scoped_ptr<ITracer> tracer_;
  /** Which inodes is recently used */
  boost::scoped_ptr<IDurableRange> durable_range_;
  /** Whether Shutdown() is called */
  bool shutting_down_;
};

}  // namespace server
}  // namespace cpfs
