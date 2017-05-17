/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement base classes for meta server.
 */

#include "server/ms/base_ms.hpp"

#include <csignal>
#include <string>

#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "authenticator.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "connector.hpp"
#include "fim.hpp"
#include "fim_processor.hpp"
#include "fims.hpp"
#include "io_service_runner.hpp"
#include "logger.hpp"
#include "shutdown_mgr.hpp"
#include "thread_fim_processor.hpp"
#include "time_keeper.hpp"
#include "server/durable_range.hpp"
#include "server/ms/cleaner.hpp"
#include "server/ms/conn_mgr.hpp"
#include "server/ms/dirty_inode.hpp"
#include "server/ms/ds_locker.hpp"
#include "server/ms/ds_query_mgr.hpp"
#include "server/ms/dsg_alloc.hpp"
#include "server/ms/failover_mgr.hpp"
#include "server/ms/inode_src.hpp"
#include "server/ms/inode_usage.hpp"
#include "server/ms/meta_dir_reader.hpp"
#include "server/ms/replier.hpp"
#include "server/ms/resync_mgr.hpp"
#include "server/ms/startup_mgr.hpp"
#include "server/ms/stat_keeper.hpp"
#include "server/ms/state_mgr.hpp"
#include "server/ms/store.hpp"
#include "server/ms/topology.hpp"
#include "server/ms/ugid_handler.hpp"
#include "server/thread_group.hpp"

namespace boost {
namespace system {
class error_code;
}
}

namespace cpfs {

class IFimSocket;

namespace server {
namespace ms {
namespace {

/**
 * Actual MetaServer for production.
 */
class MetaServer : public BaseMetaServer {
 public:
  /**
   * Construct the MetaServer
   *
   * @param configs The config items parsed
   */
  explicit MetaServer(const ConfigMgr& configs) : BaseMetaServer(configs) {}

  void Init() {
    LOG(notice, Server, "CPFS Meta Server is running");
    int term_signal = SIGUSR1;
    shutdown_mgr()->SetupSignals(&term_signal, 1);
    // Meta directory
    if (!store_->IsInitialized())
      store_->Initialize();
    store_->PrepareUUID();
    inode_src_->Init();
    authenticator_->Init();
    stat_processor_->Start();
    // HA-mode
    if (!configs_.ms2_host().empty()) {
      durable_range_->Load();
      failover_processor_->Start();
    } else {
      state_mgr_->SwitchState(kStateStandalone);
      StartServerActivated();
    }
  }

  int Run() {
    topology_mgr_->Init();
    startup_mgr_->Init();
    conn_mgr_->Init();
    std::string host;
    int port;
    if (configs_.role() == "MS1") {
      host = configs_.ms1_host();
      port = configs_.ms1_port();
    } else {
      host = configs_.ms2_host();
      port = configs_.ms2_port();
    }
    connector_->Listen(host, port, init_fim_processor_.get());
    io_service_runner_->Run();
    io_service_runner_->Join();
    return 0;
  }

  void StartServerActivated() {
    dirty_inode_mgr()->Reset(true);
    inode_src_->SetupAllocation();
    PrepareActivate();
    topology_mgr_->StartStopWorker();
  }

  void PrepareActivate() {
    BOOST_FOREACH(
        DirtyInodeMap::value_type item, dirty_inode_mgr()->GetList()) {
      if (item.second.active)
        continue;
      FIM_PTR<InodeCleanAttrFim> fim = InodeCleanAttrFim::MakePtr();
      (*fim)->inode = item.first;
      (*fim)->gen = item.second.gen;
      thread_group()->Process(fim, boost::shared_ptr<IFimSocket>());
    }
    ugid_handler_->SetCheck(configs_.ms_perms());
  }

 private:
  /**
   * Handle termination signals
   */
  void HandleTermSignal(const boost::system::error_code& error, int sig_num) {
    (void) error;
    if (sig_num == SIGUSR1)
      shutdown_mgr()->Init();
  }
};

}  // namespace

BaseMetaServer::~BaseMetaServer() {}

BaseMetaServer::BaseMetaServer(const ConfigMgr& configs)
    : BaseCpfsServer(configs),
      client_num_counter_(0) {}

bool BaseMetaServer::IsHAMode() const {
  return !configs_.ms2_host().empty();
}

void BaseMetaServer::StartServerActivated() {}

void BaseMetaServer::PrepareActivate() {}

void BaseMetaServer::set_store(IStore* store) {
  store_.reset(store);
}

IStore* BaseMetaServer::store() {
  return store_.get();
}

void BaseMetaServer::set_conn_mgr(IConnMgr* conn_mgr) {
  conn_mgr_.reset(conn_mgr);
}

IConnMgr* BaseMetaServer::conn_mgr() {
  return conn_mgr_.get();
}

void BaseMetaServer::set_startup_mgr(IStartupMgr* startup_mgr) {
  startup_mgr_.reset(startup_mgr);
}

IStartupMgr* BaseMetaServer::startup_mgr() {
  return startup_mgr_.get();
}

void BaseMetaServer::set_ugid_handler(IUgidHandler* ugid_handler) {
  ugid_handler_.reset(ugid_handler);
}

IUgidHandler* BaseMetaServer::ugid_handler() {
  return ugid_handler_.get();
}

void BaseMetaServer::set_dsg_allocator(IDSGAllocator* dsg_allocator) {
  dsg_allocator_.reset(dsg_allocator);
}

IDSGAllocator* BaseMetaServer::dsg_allocator() {
  return dsg_allocator_.get();
}

void BaseMetaServer::set_inode_src(IInodeSrc* inode_src) {
  inode_src_.reset(inode_src);
}

IInodeSrc* BaseMetaServer::inode_src() {
  return inode_src_.get();
}

void BaseMetaServer::set_ds_locker(IDSLocker* ds_locker) {
  ds_locker_.reset(ds_locker);
}

IDSLocker* BaseMetaServer::ds_locker() {
  return ds_locker_.get();
}

void BaseMetaServer::set_inode_usage(IInodeUsage* inode_usage) {
  inode_usage_.reset(inode_usage);
}

IInodeUsage* BaseMetaServer::inode_usage() {
  return inode_usage_.get();
}

void BaseMetaServer::set_replier(IReplier* replier) {
  replier_.reset(replier);
}

IReplier* BaseMetaServer::replier() {
  return replier_.get();
}

void BaseMetaServer::set_cleaner(ICleaner* cleaner) {
  cleaner_.reset(cleaner);
}

ICleaner* BaseMetaServer::cleaner() {
  return cleaner_.get();
}

void BaseMetaServer::set_state_mgr(IStateMgr* state_mgr) {
  state_mgr_.reset(state_mgr);
}

IStateMgr* BaseMetaServer::state_mgr() {
  return state_mgr_.get();
}

void BaseMetaServer::set_ha_counter(IHACounter* ha_counter) {
  ha_counter_.reset(ha_counter);
}

IHACounter* BaseMetaServer::ha_counter() {
  return ha_counter_.get();
}

void BaseMetaServer::set_topology_mgr(ITopologyMgr* topology_mgr) {
  topology_mgr_.reset(topology_mgr);
}

ITopologyMgr* BaseMetaServer::topology_mgr() {
  return topology_mgr_.get();
}

void BaseMetaServer::set_failover_processor(IThreadFimProcessor* proc) {
  failover_processor_.reset(proc);
}

IThreadFimProcessor* BaseMetaServer::failover_processor() {
  return failover_processor_.get();
}

void BaseMetaServer::set_failover_mgr(IFailoverMgr* mgr) {
  failover_mgr_.reset(mgr);
}

IFailoverMgr* BaseMetaServer::failover_mgr() {
  return failover_mgr_.get();
}

void BaseMetaServer::set_resync_mgr(IResyncMgr* mgr) {
  resync_mgr_.reset(mgr);
}

IResyncMgr* BaseMetaServer::resync_mgr() {
  return resync_mgr_.get();
}

void BaseMetaServer::set_meta_dir_reader(IMetaDirReader* meta_dir_reader) {
  meta_dir_reader_.reset(meta_dir_reader);
}

IMetaDirReader* BaseMetaServer::meta_dir_reader() {
  return meta_dir_reader_.get();
}

void BaseMetaServer::set_peer_time_keeper(ITimeKeeper* time_keeper) {
  peer_time_keeper_.reset(time_keeper);
}

ITimeKeeper* BaseMetaServer::peer_time_keeper() {
  return peer_time_keeper_.get();
}

void BaseMetaServer::set_ds_query_mgr(IDSQueryMgr* ds_query_mgr) {
  ds_query_mgr_.reset(ds_query_mgr);
}

IDSQueryMgr* BaseMetaServer::ds_query_mgr() {
  return ds_query_mgr_.get();
}

void BaseMetaServer::set_stat_keeper(IStatKeeper* stat_keeper) {
  stat_keeper_.reset(stat_keeper);
}

IStatKeeper* BaseMetaServer::stat_keeper() {
  return stat_keeper_.get();
}

void BaseMetaServer::set_stat_processor(IThreadFimProcessor* proc) {
  stat_processor_.reset(proc);
}

IThreadFimProcessor* BaseMetaServer::stat_processor() {
  return stat_processor_.get();
}

void BaseMetaServer::set_dirty_inode_mgr(IDirtyInodeMgr* dirty_inode_mgr) {
  dirty_inode_mgr_.reset(dirty_inode_mgr);
}

IDirtyInodeMgr* BaseMetaServer::dirty_inode_mgr() {
  return dirty_inode_mgr_.get();
}

void BaseMetaServer::set_attr_updater(IAttrUpdater* attr_updater) {
  attr_updater_.reset(attr_updater);
}

IAttrUpdater* BaseMetaServer::attr_updater() {
  return attr_updater_.get();
}

void BaseMetaServer::set_admin_fim_processor(IFimProcessor* proc) {
  admin_fim_processor_.reset(proc);
}

IFimProcessor* BaseMetaServer::admin_fim_processor() {
  return admin_fim_processor_.get();
}

ClientNum& BaseMetaServer::client_num_counter() {
  return client_num_counter_;
}

BaseMetaServer* MakeMetaServer(const ConfigMgr& configs) {
  return new MetaServer(configs);
}

}  // namespace ms
}  // namespace server
}  // namespace cpfs
