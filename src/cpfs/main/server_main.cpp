/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS.
 */

#include "main/server_main.hpp"

#include <cstdio>
#include <exception>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include <boost/program_options.hpp>
#include <boost/scoped_ptr.hpp>

#include "asio_policy.hpp"
#include "asio_policy_impl.hpp"
#include "authenticator.hpp"
#include "authenticator_impl.hpp"
#include "common.hpp"
#include "composite_fim_processor.hpp"
#include "config_mgr.hpp"
#include "connecting_socket_impl.hpp"
#include "connector.hpp"
#include "connector_impl.hpp"
#include "crypto_impl.hpp"
#include "daemonizer.hpp"
#include "fim_processor_base.hpp"
#include "fim_socket_impl.hpp"
#include "io_service_runner.hpp"
#include "io_service_runner_impl.hpp"
#include "listening_socket_impl.hpp"
#include "logger.hpp"
#include "op_completion_impl.hpp"
#include "periodic_timer_impl.hpp"
#include "posix_fs_impl.hpp"
#include "shaped_sender_impl.hpp"
#include "shutdown_mgr.hpp"
#include "status_dumper.hpp"
#include "status_dumper_impl.hpp"
#include "thread_fim_processor.hpp"
#include "thread_fim_processor_impl.hpp"
#include "time_keeper_impl.hpp"
#include "tracer_impl.hpp"
#include "tracker_mapper.hpp"
#include "tracker_mapper_impl.hpp"
#include "util.hpp"
#include "server/ccache_tracker_impl.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/cleaner.hpp"
#include "server/ds/cleaner_impl.hpp"
#include "server/ds/conn_mgr_impl.hpp"
#include "server/ds/degrade_impl.hpp"
#include "server/ds/fim_processors.hpp"
#include "server/ds/resync.hpp"
#include "server/ds/resync_impl.hpp"
#include "server/ds/shutdown_mgr_impl.hpp"
#include "server/ds/store.hpp"
#include "server/ds/store_impl.hpp"
#include "server/ds/worker_impl.hpp"
#include "server/durable_range_impl.hpp"
#include "server/inode_removal_tracker_impl.hpp"
#include "server/ms/admin_fim_processor.hpp"
#include "server/ms/base_ms.hpp"
#include "server/ms/cleaner.hpp"
#include "server/ms/cleaner_impl.hpp"
#include "server/ms/conn_mgr_impl.hpp"
#include "server/ms/dirty_inode_impl.hpp"
#include "server/ms/ds_locker_impl.hpp"
#include "server/ms/ds_query_mgr_impl.hpp"
#include "server/ms/dsg_alloc_impl.hpp"
#include "server/ms/dsg_op_state_impl.hpp"
#include "server/ms/failover_mgr.hpp"
#include "server/ms/failover_mgr_impl.hpp"
#include "server/ms/fim_processors.hpp"
#include "server/ms/inode_src_impl.hpp"
#include "server/ms/inode_usage_impl.hpp"
#include "server/ms/meta_dir_reader_impl.hpp"
#include "server/ms/replier_impl.hpp"
#include "server/ms/resync_mgr_impl.hpp"
#include "server/ms/shutdown_mgr_impl.hpp"
#include "server/ms/startup_mgr.hpp"
#include "server/ms/startup_mgr_impl.hpp"
#include "server/ms/stat_keeper_impl.hpp"
#include "server/ms/state_mgr_impl.hpp"
#include "server/ms/store.hpp"
#include "server/ms/store_impl.hpp"
#include "server/ms/topology_impl.hpp"
#include "server/ms/ugid_handler_impl.hpp"
#include "server/ms/worker_impl.hpp"
#include "server/reply_set_impl.hpp"
#include "server/server_info_impl.hpp"
#include "server/thread_group.hpp"
#include "server/thread_group_impl.hpp"
#include "server/worker.hpp"

namespace cpfs {

class IService;
class ITimeKeeper;

namespace server {

class ICCacheTracker;
class IDurableRange;
class IInodeRemovalTracker;
class IServerInfo;

namespace ds {

class IConnMgr;

}  // namespace ds

namespace ms {

class IDSGAllocator;
class IDSLocker;
class IDSQueryMgr;
class IInodeSrc;
class IInodeUsage;
class IStatKeeper;
class IUgidHandler;

}  // namespace ms

}  // namespace server

namespace main {

namespace ds = cpfs::server::ds;
namespace ms = cpfs::server::ms;

// Configuration Keys
//
// role: The role of server, MSx (x=1,2,3..) or DS
// ds_host: The host ip for DS
// ds_port: The port number for DS
// num_ds_groups: The number of DS groups
// data_dir: The data directory
// ms1_host: MS1 ip
// ms1_port: MS1 port
// ms2_host: MS2 ip
// ms2_port: MS2 port
// log_severity: The log severity used by pantheios
// log_path: The log path used by pantheios
// ms_perms: If MS should do access permission checks
// heartbeat_interval: The heartbeat interval in sec
// socket_read_timeout: The socket read timeout in sec

bool ParseOpts(int argc, char* argv[], ConfigMgr* configs) {
  namespace po = boost::program_options;
  po::variables_map vm;
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "Produce help message")
      ("meta-server", po::value<std::string>()->required(),
       "host:port, The host:port of the meta server(s), separated by comma")
      ("role", po::value<std::string>()->required(),
       "MSx or DS, where x is the server no")
      ("ds-host", po::value<std::string>(),
       "The DS listening ip")
      ("ds-port", po::value<int>(),
       "The listening port")
      ("data", po::value<std::string>()->required(),
       "The data directory")
      ("log-level", po::value<std::string>()->default_value("5"),
       "The logging level (0 - 7).  May also be specified for particular "
       "category, E.g., 5:Server=7 use 7 for Server, 5 for others\n")
      ("log-path", po::value<std::string>()->default_value("/dev/stderr"),
       "Redirect logging information to a file instead of standard error")
      ("ms-perms", po::value<bool>()->default_value(true),
       "Whether MS should check permissions")
      ("daemonize", po::value<bool>(),
       "Whether the server should be daemonized")
      ("pidfile", po::value<std::string>(), "The file path to write the PID")
      ("heartbeat-interval",
       po::value<double>()->default_value(kDefaultHeartbeatInterval),
       (boost::format("The heartbeat interval. Default: %.2f seconds")
            % kDefaultHeartbeatInterval).str().c_str())
      ("socket-read-timeout",
       po::value<double>()->default_value(kDefaultSocketReadTimeout),
       (boost::format("The socket read timeout. Default: %.2f seconds")
            % kDefaultSocketReadTimeout).str().c_str())
      ("data-sync-num-inodes",
       po::value<unsigned>()->default_value(kDefaultDataSyncNumInodes),
       (boost::format("The number of inodes to sync in each phase. Default: %u")
            % kDefaultDataSyncNumInodes).str().c_str());

  po::store(po::parse_command_line(argc, argv, desc), vm);
  if (vm.count("help")) {
    std::stringstream desc_ss;
    desc.print(desc_ss);
    std::fprintf(stderr, "%s", desc_ss.str().c_str());
    return false;
  }
  po::notify(vm);
  configs->set_log_path(vm["log-path"].as<std::string>());
  configs->set_log_severity(vm["log-level"].as<std::string>());
  // Parse options
  std::vector<std::string> meta_servers;
  boost::split(meta_servers, vm["meta-server"].as<std::string>(),
               boost::is_any_of(","));

  boost::scoped_ptr<IAsioPolicy> asio_policy(MakeAsioPolicy());
  std::string host;
  int port;
  asio_policy->ParseHostPort(meta_servers[0], &host, &port);
  configs->set_ms1_host(host);
  configs->set_ms1_port(port);

  if (meta_servers.size() == 2) {
    asio_policy->ParseHostPort(meta_servers[1], &host, &port);
    configs->set_ms2_host(host);
    configs->set_ms2_port(port);
  }

  std::string role = vm["role"].as<std::string>();
  if (role != "MS1" && role != "MS2" && role != "DS")
    throw std::runtime_error("Unrecognized role");
  configs->set_role(role);

  if (vm.count("ds-port")) {
    int ds_port = vm["ds-port"].as<int>();
    if (ds_port <= 0 || ds_port >= 65535)
      throw std::runtime_error("Invalid port number");
    configs->set_ds_port(ds_port);
  } else {
    if (role == "DS")
      throw std::runtime_error("Missing ds-port");
  }

  if (vm.count("ds-host")) {
    configs->set_ds_host(vm["ds-host"].as<std::string>());
  } else {
    if (role == "DS")
      throw std::runtime_error("Missing ds-host");
  }

  std::string data_dir = vm["data"].as<std::string>();
  if (!boost::filesystem::is_directory(data_dir)) {
    throw std::runtime_error("Data directory does not exist");
  }
  configs->set_data_dir(data_dir);
  configs->set_ms_perms(vm["ms-perms"].as<bool>());
  if (vm.count("daemonize"))
    configs->set_daemonize(vm["daemonize"].as<bool>());
  if (vm.count("pidfile"))
    configs->set_pidfile(vm["pidfile"].as<std::string>());
  configs->set_heartbeat_interval(vm["heartbeat-interval"].as<double>());
  configs->set_socket_read_timeout(vm["socket-read-timeout"].as<double>());
  configs->set_data_sync_num_inodes(vm["data-sync-num-inodes"].as<unsigned>());
  return true;
}

namespace {

/** Number of meta / data worker threads */
const unsigned kNumWorkers = 8;

/**
 * Create a meta worker for use as FimProcessor
 */
server::IWorker* BuildMSWorker(ms::BaseMetaServer* server) {
  server::IWorker* ret = ms::MakeWorker();
  ret->set_server(server);
  return ret;
}

/**
 * Create a fully integrated meta data server.
 */
IService* BuildMetaServer(const ConfigMgr& configs) {
  IAsioPolicy* asio_policy = MakeAsioPolicy();
  UNIQUE_PTR<ms::BaseMetaServer> ret(ms::MakeMetaServer(configs));
  ret->set_asio_policy(asio_policy);
  ret->set_fc_asio_policy(MakeAsioPolicy());
  IAsioPolicy* aux_asio_policy = MakeAsioPolicy();
  ret->set_aux_asio_policy(aux_asio_policy);
  ret->set_fim_socket_maker(kFimSocketMaker);
  IIOServiceRunner* io_service_runner = MakeIOServiceRunner();
  ret->set_io_service_runner(io_service_runner);
  io_service_runner->AddService(asio_policy->io_service());
  io_service_runner->AddService(ret->fc_asio_policy()->io_service());
  io_service_runner->AddService(aux_asio_policy->io_service());
  ITrackerMapper* tracker_mapper = MakeTrackerMapper(asio_policy);
  ret->set_tracker_mapper(tracker_mapper);
  tracker_mapper->SetClientNum(kNotClient);
  IAuthenticator* authenticator =
      MakeAuthenticator(MakeCrypto(), asio_policy);
  authenticator->SetPeriodicTimerMaker(kPeriodicTimerMaker,
                                       kUnauthenticatedSocketAge);
  ret->set_authenticator(authenticator);
  ret->set_tracer(MakeTracer());
  IConnector* connector = MakeConnector(asio_policy, authenticator);
  ret->set_connector(connector);
  connector->SetFimSocketMaker(kFimSocketMaker);
  connector->SetConnectingSocketMaker(kConnectingSocketMaker);
  connector->SetListeningSocketMaker(kListeningSocketMaker);
  connector->set_heartbeat_interval(
      configs.heartbeat_interval());
  connector->set_socket_read_timeout(
      configs.socket_read_timeout());
  ms::IUgidHandler* ugid_handler = ms::MakeUgidHandler();
  ret->set_ugid_handler(ugid_handler);
  std::string data_dir = configs.data_dir();
  ms::IInodeSrc* inode_src = ms::MakeInodeSrc(data_dir);
  ret->set_inode_src(inode_src);
  ms::IDSLocker* ds_locker = ms::MakeDSLocker(ret.get());
  ret->set_ds_locker(ds_locker);
  ms::IStatKeeper* stat_keeper = ms::MakeStatKeeper(ret.get());
  ret->set_stat_keeper(stat_keeper);
  ms::IDSGAllocator* dsg_alloc = ms::MakeDSGAllocator(ret.get());
  ret->set_dsg_allocator(dsg_alloc);
  ms::IStore* store = ms::MakeStore(data_dir);
  ret->set_store(store);
  server::IServerInfo* server_info = server::MakeServerInfo(data_dir);
  ret->set_server_info(server_info);
  ms::IStartupMgr* startup_mgr = ms::MakeStartupMgr(ret.get());
  ret->set_startup_mgr(startup_mgr);
  startup_mgr->SetPeriodicTimerMaker(kPeriodicTimerMaker);
  ms::IInodeUsage* inode_usage = ms::MakeInodeUsage();
  ret->set_inode_usage(inode_usage);
  store->SetUgidHandler(ugid_handler);
  store->SetInodeSrc(inode_src);
  store->SetDSGAllocator(dsg_alloc);
  store->SetInodeUsage(inode_usage);
  ret->set_dirty_inode_mgr(ms::MakeDirtyInodeMgr(data_dir));
  ret->set_attr_updater(ms::MakeAttrUpdater(ret.get()));
  ret->set_cache_tracker(server::MakeICacheTracker());
  ret->set_recent_reply_set(server::MakeReplySet());
  ret->set_replier(ms::MakeReplier(ret.get()));
  ms::ICleaner* cleaner;
  ret->set_cleaner(cleaner = ms::MakeCleaner(ret.get()));
  cleaner->SetPeriodicTimerMaker(kPeriodicTimerMaker);
  cleaner->Init();
  server::IWorker* failover_worker = BuildMSWorker(ret.get());
  IThreadFimProcessor* failover_thread =
      kBasicThreadFimProcessorMaker(failover_worker);
  failover_worker->SetQueuer(failover_thread);
  ret->set_failover_processor(failover_thread);
  server::IThreadGroup* thread_group = server::MakeThreadGroup();
  thread_group->SetThreadFimProcessorMaker(kThreadFimProcessorMaker);
  ret->set_thread_group(thread_group);
  for (unsigned i = 0; i < kNumWorkers; ++i)
    thread_group->AddWorker(BuildMSWorker(ret.get()));
  ret->set_ha_counter(ms::MakeHACounter(data_dir, configs.role()));
  ret->set_state_mgr(ms::MakeStateMgr());
  ret->set_meta_dir_reader(ms::MakeMetaDirReader(data_dir));
  server::IInodeRemovalTracker* removal_tracker =
      server::MakeInodeRemovalTracker(data_dir);
  ret->set_inode_removal_tracker(removal_tracker);
  store->SetInodeRemovalTracker(removal_tracker);
  ret->set_resync_mgr(ms::MakeResyncMgr(ret.get()));
  ret->set_dsg_op_state_mgr(
      ms::MakeDSGOpStateMgr(MakeOpCompletionCheckerSet()));
  if (!configs.ms2_host().empty()) {
    ITimeKeeper* time_keeper =
        MakeTimeKeeper(data_dir,
                       "user.peer.seen", kLastSeenPersistInterval);
    ret->set_peer_time_keeper(time_keeper);
    server::IDurableRange* durable_range =
        server::MakeDurableRange((data_dir + "/m").c_str(), kDurableRangeOrder);
    ret->set_durable_range(durable_range);
    store->SetDurableRange(durable_range);
    CompositeFimProcessor* ms_proc = new CompositeFimProcessor;
    ret->set_ms_fim_processor(ms_proc);
    BaseFimProcessor* base_proc = new BaseFimProcessor;
    base_proc->SetTimeKeeper(time_keeper);
    ms_proc->AddFimProcessor(base_proc);
    // The inter-MS worker has no queuer, so use carefully!
    ms_proc->AddFimProcessor(BuildMSWorker(ret.get()));
    ms_proc->AddFimProcessor(ms::MakeMSCtrlFimProcessor(ret.get()));
    ms::IFailoverMgr* failover_mgr;
    ret->set_failover_mgr(failover_mgr = ms::MakeFailoverMgr(ret.get()));
    failover_mgr->SetPeriodicTimerMaker(kPeriodicTimerMaker);
  }
  CompositeFimProcessor* ds_proc = new CompositeFimProcessor;
  ret->set_ds_fim_processor(ds_proc);
  ds_proc->AddFimProcessor(new BaseFimProcessor);
  ds_proc->AddFimProcessor(ms::MakeDSCtrlFimProcessor(ret.get()));
  CompositeFimProcessor* fc_proc = new CompositeFimProcessor;
  ret->set_fc_fim_processor(fc_proc);
  ms::IDSQueryMgr* ds_query_mgr = MakeDSQueryMgr(ret.get());
  ret->set_ds_query_mgr(ds_query_mgr);;
  IThreadFimProcessor* stat_proc =
      kThreadFimProcessorMaker(server::ms::MakeAdminFimProcessor(ret.get()));
  ret->set_stat_processor(stat_proc);
  fc_proc->AddFimProcessor(new BaseFimProcessor);
  fc_proc->AddFimProcessor(stat_proc, true);
  fc_proc->AddFimProcessor(thread_group, true);
  CompositeFimProcessor* admin_proc = new CompositeFimProcessor;
  admin_proc->AddFimProcessor(new BaseFimProcessor);
  admin_proc->AddFimProcessor(stat_proc, true);
  ret->set_admin_fim_processor(admin_proc);
  ret->set_topology_mgr(MakeTopologyMgr(ret.get()));
  ret->set_conn_mgr(MakeConnMgr(ret.get()));
  CompositeFimProcessor* init_fim_processor = new CompositeFimProcessor;
  ret->set_init_fim_processor(init_fim_processor);
  init_fim_processor->AddFimProcessor(new BaseFimProcessor);
  init_fim_processor->AddFimProcessor(ms::MakeInitFimProcessor(ret.get()));
  IShutdownMgr* shutdown_mgr = ms::MakeShutdownMgr(
      ret.get(), kPeriodicTimerMaker);
  ret->set_shutdown_mgr(shutdown_mgr);
  shutdown_mgr->SetAsioPolicy(asio_policy);
  ret->set_status_dumper(MakeStatusDumper(ret.get()));
  ret->status_dumper()->SetAsioPolicy(aux_asio_policy);
  return ret.release();
}

/**
 * Create a fully integrated data server.
 */
IService* BuildDataServer(const ConfigMgr& configs) {
  IAsioPolicy* asio_policy = MakeAsioPolicy();
  UNIQUE_PTR<ds::BaseDataServer> ret(ds::MakeDataServer(configs));
  ret->set_asio_policy(asio_policy);
  ret->set_fc_asio_policy(MakeAsioPolicy());
  ret->set_ds_asio_policy(MakeAsioPolicy());
  IAsioPolicy* aux_asio_policy = MakeAsioPolicy();
  ret->set_aux_asio_policy(aux_asio_policy);
  ret->set_fim_socket_maker(kFimSocketMaker);
  IIOServiceRunner* io_service_runner = MakeIOServiceRunner();
  ret->set_io_service_runner(io_service_runner);
  io_service_runner->AddService(asio_policy->io_service());
  io_service_runner->AddService(ret->fc_asio_policy()->io_service());
  io_service_runner->AddService(ret->ds_asio_policy()->io_service());
  io_service_runner->AddService(aux_asio_policy->io_service());
  ITrackerMapper* tracker_mapper = MakeTrackerMapper(asio_policy);
  ret->set_tracker_mapper(tracker_mapper);
  tracker_mapper->SetClientNum(kNotClient);
  ds::ICleaner* cleaner;
  ret->set_cleaner(cleaner = MakeCleaner(ret.get()));
  cleaner->SetPeriodicTimerMaker(kPeriodicTimerMaker);
  cleaner->SetMinInodeRemoveTime(kDSMinInodeRemoveRetryTime);
  cleaner->Init();
  IAuthenticator* authenticator =
      MakeAuthenticator(MakeCrypto(), asio_policy);
  authenticator->SetPeriodicTimerMaker(kPeriodicTimerMaker,
                                       kUnauthenticatedSocketAge);
  ret->set_authenticator(authenticator);
  ret->set_tracer(MakeTracer());
  IConnector* connector = MakeConnector(asio_policy, authenticator, false);
  ret->set_connector(connector);
  connector->SetFimSocketMaker(kFimSocketMaker);
  connector->SetConnectingSocketMaker(kConnectingSocketMaker);
  connector->SetListeningSocketMaker(kListeningSocketMaker);
  connector->set_heartbeat_interval(
      configs.heartbeat_interval());
  connector->set_socket_read_timeout(
      configs.socket_read_timeout());
  ds::IConnMgr* conn_mgr = ds::MakeConnMgr(ret.get());
  ret->set_conn_mgr(conn_mgr);
  ret->set_posix_fs(MakePosixFS());
  std::string data_dir = configs.data_dir();
  ds::IStore* store = ds::MakeStore(data_dir);
  store->SetPosixFS(ret->posix_fs());
  ret->set_store(store);
  ret->set_server_info(server::MakeServerInfo(data_dir));
  server::IInodeRemovalTracker* removal_tracker =
      server::MakeInodeRemovalTracker(data_dir);
  ret->set_inode_removal_tracker(removal_tracker);
  store->SetInodeRemovalTracker(removal_tracker);
  ITimeKeeper* time_keeper =
      MakeTimeKeeper(data_dir, "user.ready.time",
                     kLastSeenPersistInterval);
  server::IDurableRange* durable_range =
      server::MakeDurableRange((data_dir + "/m").c_str(), kDurableRangeOrder);
  ret->set_durable_range(durable_range);
  store->SetDurableRange(durable_range);
  ret->set_dsg_ready_time_keeper(time_keeper);
  server::ICCacheTracker* cache_tracker = server::MakeICacheTracker();
  ret->set_cache_tracker(cache_tracker);
  server::IThreadGroup* thread_group = server::MakeThreadGroup();
  thread_group->SetThreadFimProcessorMaker(kThreadFimProcessorMaker);
  ret->set_thread_group(thread_group);
  ds::IResyncFimProcessor* resync_fim_proc =
      ds::MakeResyncFimProcessor(ret.get());
  ret->set_resync_thread_processor(kThreadFimProcessorMaker(resync_fim_proc));
  ret->set_resync_fim_processor(resync_fim_proc);
  CompositeFimProcessor* ms_fim_proc = new CompositeFimProcessor;
  ret->set_ms_fim_processor(ms_fim_proc);
  BaseFimProcessor* base_ms_proc = new BaseFimProcessor;
  ms_fim_proc->AddFimProcessor(base_ms_proc);
  base_ms_proc->SetTimeKeeper(time_keeper);
  ms_fim_proc->AddFimProcessor(ds::MakeMSCtrlFimProcessor(ret.get()));
  ms_fim_proc->AddFimProcessor(thread_group, true);
  ms_fim_proc->AddFimProcessor(resync_fim_proc, true);
  CompositeFimProcessor* ds_proc = new CompositeFimProcessor;
  ret->set_ds_fim_processor(ds_proc);
  ds_proc->AddFimProcessor(new BaseFimProcessor);
  ds_proc->AddFimProcessor(thread_group, true);
  ds_proc->AddFimProcessor(resync_fim_proc, true);
  CompositeFimProcessor* fc_proc = new CompositeFimProcessor;
  ret->set_fc_fim_processor(fc_proc);
  fc_proc->AddFimProcessor(new BaseFimProcessor);
  fc_proc->AddFimProcessor(thread_group, true);
  ret->set_degraded_cache(ds::MakeDegradedCache(
      kDegradedCacheSegments));
  ret->set_data_recovery_mgr(ds::MakeDataRecoveryMgr());
  ret->set_op_completion_checker_set(MakeOpCompletionCheckerSet());
  ds::IResyncMgr* resync_mgr = ds::MakeResyncMgr(ret.get());
  resync_mgr->SetShapedSenderMaker(kShapedSenderMaker);
  ret->set_resync_mgr(resync_mgr);
  for (unsigned i = 0; i < kNumWorkers; ++i) {
    server::IWorker* worker;
    thread_group->AddWorker(worker = ds::MakeWorker());
    worker->set_server(ret.get());
  }
  CompositeFimProcessor* init_fim_processor = new CompositeFimProcessor;
  ret->set_init_fim_processor(init_fim_processor);
  init_fim_processor->AddFimProcessor(new BaseFimProcessor);
  init_fim_processor->AddFimProcessor(ds::MakeInitFimProcessor(ret.get()));
  IShutdownMgr* shutdown_mgr = ds::MakeShutdownMgr(ret.get());
  ret->set_shutdown_mgr(shutdown_mgr);
  shutdown_mgr->SetAsioPolicy(asio_policy);
  ret->set_status_dumper(MakeStatusDumper(ret.get()));
  ret->status_dumper()->SetAsioPolicy(aux_asio_policy);
  return ret.release();
}

}  // namespace

IService* MakeServer(int argc, char* argv[], IDaemonizer* daemonizer) {
  try {
    ConfigMgr configs;
    if (!ParseOpts(argc, argv, &configs))
      return 0;
    if (configs.daemonize())
      daemonizer->Daemonize();
    if (!configs.pidfile().empty())
      WritePID(configs.pidfile());
    std::string role = configs.role();
    if (role.find("MS") != std::string::npos)
      return BuildMetaServer(configs);
    if (role == "DS")
      return BuildDataServer(configs);
  } catch (const std::exception& ex) {
    SetLogPath("/");
    LOG(critical, Server, "CPFS Error: ", ex.what());
  }
  return 0;
}

}  // namespace main
}  // namespace cpfs
