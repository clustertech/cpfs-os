/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement base classes for data server.
 */

#include "server/ds/base_ds.hpp"

#include <stdint.h>

#include <vector>

#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "asio_policy.hpp"
#include "authenticator.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "connector.hpp"
#include "dsg_state.hpp"
#include "io_service_runner.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "op_completion.hpp"
#include "posix_fs.hpp"
#include "thread_fim_processor.hpp"
#include "time_keeper.hpp"
#include "version.hpp"
#include "server/base_server.hpp"
#include "server/ds/cleaner.hpp"
#include "server/ds/conn_mgr.hpp"
#include "server/ds/degrade.hpp"
#include "server/ds/resync.hpp"
#include "server/ds/store.hpp"
#include "server/durable_range.hpp"
#include "server/thread_group.hpp"

namespace cpfs {
namespace server {
namespace ds {
namespace {

/**
 * Actual DataServer for production.
 */
class DataServer : public BaseDataServer {
 public:
  /**
   * @param configs The config items parsed
   */
  explicit DataServer(const ConfigMgr& configs) : BaseDataServer(configs) {
    LOG(notice, Server, "CPFS Data Server ", CPFS_VERSION, " is running");
  }

  void Init() {
    durable_range_->Load();
    thread_group_->Start();
    authenticator_->Init();
    resync_thread_processor_->Start();
  }

  int Run() {
    conn_mgr_->Init();
    connector_->Listen(configs_.ds_host(), configs_.ds_port(),
                       init_fim_processor_.get());
    io_service_runner_->Run();
    io_service_runner_->Join();
    return 0;
  }
};

}  // namespace

BaseDataServer::~BaseDataServer() {}

BaseDataServer::BaseDataServer(const ConfigMgr& configs)
    : BaseCpfsServer(configs),
      resync_fim_processor_(0),
      dsg_state_(new DSGStateInfo(kDSGPending)),
      opt_resync_(false),
      distressed_(false) {}

void BaseDataServer::set_ds_asio_policy(IAsioPolicy* asio_policy) {
  ds_asio_policy_.reset(asio_policy);
}

IAsioPolicy* BaseDataServer::ds_asio_policy() {
  return ds_asio_policy_.get();
}

void BaseDataServer::set_posix_fs(IPosixFS* posix_fs) {
  posix_fs_.reset(posix_fs);
}

IPosixFS* BaseDataServer::posix_fs() {
  return posix_fs_.get();
}

void BaseDataServer::set_store(IStore* store) {
  store_.reset(store);
}

IStore* BaseDataServer::store() {
  return store_.get();
}

void BaseDataServer::set_cleaner(ICleaner* cleaner) {
  cleaner_.reset(cleaner);
}

ICleaner* BaseDataServer::cleaner() {
  return cleaner_.get();
}

void BaseDataServer::set_conn_mgr(IConnMgr* conn_mgr) {
  conn_mgr_.reset(conn_mgr);
}

IConnMgr* BaseDataServer::conn_mgr() {
  return conn_mgr_.get();
}

void BaseDataServer::set_dsg_ready_time_keeper(
    ITimeKeeper* dsg_ready_time_keeper) {
  dsg_ready_time_keeper_.reset(dsg_ready_time_keeper);
}

ITimeKeeper* BaseDataServer::dsg_ready_time_keeper() {
  return dsg_ready_time_keeper_.get();
}

void BaseDataServer::set_degraded_cache(IDegradedCache* degraded_cache) {
  degraded_cache_.reset(degraded_cache);
}

IDegradedCache* BaseDataServer::degraded_cache() {
  return degraded_cache_.get();
}

void BaseDataServer::set_data_recovery_mgr(
    IDataRecoveryMgr* data_recovery_mgr) {
  data_recovery_mgr_.reset(data_recovery_mgr);
}

IDataRecoveryMgr* BaseDataServer::data_recovery_mgr() {
  return data_recovery_mgr_.get();
}

void BaseDataServer::set_op_completion_checker_set(
    IOpCompletionCheckerSet* op_completion_checker_set) {
  op_completion_checker_set_.reset(op_completion_checker_set);
}

IOpCompletionCheckerSet* BaseDataServer::op_completion_checker_set() {
  return op_completion_checker_set_.get();
}

void BaseDataServer::set_resync_mgr(IResyncMgr* resync_mgr) {
  resync_mgr_.reset(resync_mgr);
}

IResyncMgr* BaseDataServer::resync_mgr() {
  return resync_mgr_.get();
}

void BaseDataServer::set_resync_fim_processor(
    IResyncFimProcessor* resync_fim_processor) {
  resync_fim_processor_ = resync_fim_processor;
}

IResyncFimProcessor* BaseDataServer::resync_fim_processor() {
  return resync_fim_processor_;
}

void BaseDataServer::set_resync_thread_processor(
    IThreadFimProcessor* resync_thread_processor) {
  resync_thread_processor_.reset(resync_thread_processor);
}

IThreadFimProcessor* BaseDataServer::resync_thread_processor() {
  return resync_thread_processor_.get();
}

void BaseDataServer::set_dsg_state(
    uint64_t state_change_id, DSGroupState dsg_state, GroupRole failed_role,
    boost::unique_lock<boost::shared_mutex>* lock) {
  MUTEX_LOCK(boost::unique_lock, dsg_state_->data_mutex, my_lock);
  if (lock)
    lock->swap(my_lock);
  dsg_state_->state_change_id = state_change_id;
  dsg_state_->dsg_state = dsg_state;
  dsg_state_->failed_role = failed_role;
}

void BaseDataServer::set_dsg_inodes_to_resync(
    boost::unordered_set<InodeNum>* inodes) {
  dsg_state_->resyncing.clear();
  dsg_state_->to_resync.clear();
  dsg_state_->to_resync.swap(*inodes);
}

bool BaseDataServer::is_inode_to_resync(InodeNum inode) {
  return dsg_state_->to_resync.find(inode) != dsg_state_->to_resync.end();
}

void BaseDataServer::set_dsg_inodes_resyncing(
    const std::vector<InodeNum>& inodes) {
  BOOST_FOREACH(const InodeNum& elt, dsg_state_->resyncing) {
    dsg_state_->to_resync.erase(elt);
  }
  dsg_state_->resyncing.clear();
  BOOST_FOREACH(const InodeNum& elt, inodes) {
    dsg_state_->resyncing.insert(elt);
  }
}

bool BaseDataServer::is_inode_resyncing(InodeNum inode) {
  return dsg_state_->resyncing.find(inode) != dsg_state_->resyncing.end();
}

void BaseDataServer::ReadLockDSGState(
    boost::shared_lock<boost::shared_mutex>* lock) {
  MUTEX_LOCK(boost::shared_lock, dsg_state_->data_mutex, my_lock);
  lock->swap(my_lock);
}

void BaseDataServer::WriteLockDSGState(
    boost::unique_lock<boost::shared_mutex>* lock) {
  MUTEX_LOCK(boost::unique_lock, dsg_state_->data_mutex, my_lock);
  lock->swap(my_lock);
}

DSGroupState BaseDataServer::dsg_state(uint64_t* state_change_id_ret,
                                       GroupRole* failed_role_ret) {
  *state_change_id_ret = dsg_state_->state_change_id;
  *failed_role_ret = dsg_state_->failed_role;
  return dsg_state_->dsg_state;
}

bool BaseDataServer::opt_resync() {
  return opt_resync_;
}

void BaseDataServer::set_opt_resync(bool opt_resync) {
  opt_resync_ = opt_resync;
}

bool BaseDataServer::distressed() {
  return distressed_;
}

void BaseDataServer::set_distressed(bool distressed) {
  distressed_ = distressed;
}

BaseDataServer* MakeDataServer(const ConfigMgr& configs) {
  return new DataServer(configs);
}

}  // namespace ds
}  // namespace server
}  // namespace cpfs
