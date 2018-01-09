/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement base class for client.
 */

#include "client/base_client.hpp"

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "authenticator.hpp"
#include "common.hpp"
#include "connector.hpp"
#include "dsg_state.hpp"
#include "fim_processor.hpp"
#include "io_service_runner.hpp"
#include "mutex_util.hpp"
#include "op_completion.hpp"
#include "service.hpp"
#include "shutdown_mgr.hpp"
#include "status_dumper.hpp"
#include "thread_fim_processor.hpp"
#include "tracker_mapper.hpp"
#include "util.hpp"
#include "client/api_common.hpp"
#include "client/cache.hpp"
#include "client/cleaner.hpp"
#include "client/conn_mgr.hpp"
#include "client/fs_common_lowlevel.hpp"
#include "client/inode_usage.hpp"

namespace cpfs {
namespace client {

BaseClient::BaseClient()
    : client_num_(0), frozen_(false), shutting_down_(false) {}

BaseClient::~BaseClient() {}

void BaseClient::Init() {}

int BaseClient::Run() {
  int ret = runner()->Run();
  service_runner()->Stop();
  service_runner()->Join();
  return ret;
}

void BaseClient::Shutdown() { shutting_down_ = true; }

bool BaseClient::IsShuttingDown() { return shutting_down_; }

void BaseClient::set_asio_policy(IAsioPolicy* asio_policy) {
  asio_policy_.reset(asio_policy);
}

IAsioPolicy* BaseClient::asio_policy() {
  return asio_policy_.get();
}

void BaseClient::set_service_runner(IIOServiceRunner* service_runner) {
  service_runner_.reset(service_runner);
}

IIOServiceRunner* BaseClient::service_runner() {
  return service_runner_.get();
}

void BaseClient::set_tracker_mapper(ITrackerMapper* tracker_mapper) {
  tracker_mapper_.reset(tracker_mapper);
}

ITrackerMapper* BaseClient::tracker_mapper() {
  return tracker_mapper_.get();
}

void BaseClient::set_connector(IConnector* connector) {
  connector_.reset(connector);
}

IConnector* BaseClient::connector() {
  return connector_.get();
}

void BaseClient::set_ms_fim_processor(IFimProcessor* ms_fim_processor) {
  ms_fim_processor_thread_.reset();
  ms_fim_processor_.reset(ms_fim_processor);
}

IFimProcessor* BaseClient::ms_fim_processor() {
  if (ms_fim_processor_)
    return ms_fim_processor_.get();
  return ms_fim_processor_thread_.get();
}

void BaseClient::set_ms_fim_processor_thread(
    IThreadFimProcessor* ms_fim_processor) {
  ms_fim_processor_.reset();
  ms_fim_processor_thread_.reset(ms_fim_processor);
}

IThreadFimProcessor* BaseClient::ms_fim_processor_thread() {
  return ms_fim_processor_thread_.get();
}

void BaseClient::set_ds_fim_processor(IFimProcessor* ds_fim_processor) {
  ds_fim_processor_.reset(ds_fim_processor);
}

IFimProcessor* BaseClient::ds_fim_processor() {
  return ds_fim_processor_.get();
}

void BaseClient::set_conn_mgr(IConnMgr* conn_mgr) {
  conn_mgr_.reset(conn_mgr);
}

IConnMgr* BaseClient::conn_mgr() {
  return conn_mgr_.get();
}

void BaseClient::set_runner(IService* runner) {
  runner_.reset(runner);
}

IService* BaseClient::runner() {
  return runner_.get();
}

void BaseClient::set_authenticator(IAuthenticator* authenticator) {
  authenticator_.reset(authenticator);
}

IAuthenticator* BaseClient::authenticator() {
  return authenticator_.get();
}

void BaseClient::set_client_num(ClientNum client_num) {
  client_num_ = client_num;
}

/**
 * @return The client number of the client
 */
ClientNum BaseClient::client_num() {
  return client_num_;
}

void BaseClient::SetReqFreeze(bool freeze) {
  if (freeze && !frozen_) {
    freeze_mutex_.lock();
    frozen_ = true;
  } else if (!freeze && frozen_) {
    frozen_ = false;
    freeze_mutex_.unlock();
  }
}

void BaseClient::ReqFreezeWait(boost::shared_lock<boost::shared_mutex>* lock) {
  MUTEX_LOCK(boost::shared_lock, freeze_mutex_, my_lock);
  lock->swap(my_lock);
}

DSGroupState BaseClient::set_dsg_state(GroupId group, DSGroupState state,
                                       GroupRole failed) {
  DSGroupInfo& group_info = group_states_[group];
  DSGroupState old_state = group_info.state;
  group_info.state = state;
  group_info.failed = failed;
  return old_state;
}

DSGroupState BaseClient::dsg_state(GroupId group, GroupRole* failed_ret) {
  boost::unordered_map<GroupId,  DSGroupInfo>::iterator it =
      group_states_.find(group);
  if (it == group_states_.end())
    return kDSGPending;
  *failed_ret = it->second.failed;
  return it->second.state;
}

BaseFSClient::BaseFSClient() {}

BaseFSClient::~BaseFSClient() {}

void BaseFSClient::Dump() {
  tracker_mapper_->DumpPendingRequests();
}

void BaseFSClient::set_cache_mgr(ICacheMgr* cache_mgr) {
  cache_mgr_.reset(cache_mgr);
}

ICacheMgr* BaseFSClient::cache_mgr() {
  return cache_mgr_.get();
}

void BaseFSClient::set_inode_usage_set(IInodeUsageSet* inode_usage_set) {
  inode_usage_set_.reset(inode_usage_set);
}

IInodeUsageSet* BaseFSClient::inode_usage_set() {
  return inode_usage_set_.get();
}

void BaseFSClient::set_op_completion_checker_set(
    IOpCompletionCheckerSet* checker_set) {
  op_completion_checker_set_.reset(checker_set);
}
/**
 * @return The completion checker set to track incomplete operations
 */
IOpCompletionCheckerSet* BaseFSClient::op_completion_checker_set() {
  return op_completion_checker_set_.get();
}

void BaseFSClient::set_cleaner(ICleaner* cleaner) {
  cleaner_.reset(cleaner);
}

ICleaner* BaseFSClient::cleaner() {
  return cleaner_.get();
}

void BaseFSClient::set_shutdown_mgr(IShutdownMgr* shutdown_mgr) {
  shutdown_mgr_.reset(shutdown_mgr);
}

IShutdownMgr* BaseFSClient::shutdown_mgr() {
  return shutdown_mgr_.get();
}

void BaseFSClient::set_status_dumper(IStatusDumper* status_dumper) {
  status_dumper_.reset(status_dumper);
}

IStatusDumper* BaseFSClient::status_dumper() {
  return status_dumper_.get();
}

void BaseAPIClient::set_fs_ll(IFSCommonLL* fs_ll) {
  fs_ll_.reset(fs_ll);
}

IFSCommonLL* BaseAPIClient::fs_ll() {
  return fs_ll_.get();
}

void BaseAPIClient::set_api_common(IAPICommon* api_common) {
  api_common_.reset(api_common);
}

IAPICommon* BaseAPIClient::api_common() {
  return api_common_.get();
}

void BaseAPIClient::set_fs_async_rw(IFSAsyncRWExecutor* fs_async_rw) {
  fs_async_rw_.reset(fs_async_rw);
}

IFSAsyncRWExecutor* BaseAPIClient::fs_async_rw() {
  return fs_async_rw_.get();
}

BaseAdminClient::BaseAdminClient() : data_mutex_(MUTEX_INIT), ready_(false) {}

BaseAdminClient::~BaseAdminClient() {}

void BaseAdminClient::SetReady(bool ready) {
  MUTEX_LOCK_GUARD(data_mutex_);
  ready_ = ready;
  ready_cond_.notify_all();
}

bool BaseAdminClient::WaitReady(double timeout) {
  boost::unique_lock<MUTEX_TYPE> lock(data_mutex_);
  bool to_wait = true;
  while (!ready_ && to_wait)
    to_wait = MUTEX_TIMED_WAIT(lock, ready_cond_, ToTimeDuration(timeout));
  return ready_;
}

}  // namespace client
}  // namespace cpfs
