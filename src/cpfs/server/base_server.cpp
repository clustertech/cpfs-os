/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement base classes for servers.
 */

#include "server/base_server.hpp"

#include <string>
#include <vector>

#include "asio_policy.hpp"
#include "authenticator.hpp"
#include "connector.hpp"
#include "fim_processor.hpp"
#include "fim_socket.hpp"
#include "io_service_runner.hpp"
#include "logger.hpp"
#include "shutdown_mgr.hpp"
#include "status_dumper.hpp"
#include "tracer.hpp"
#include "tracker_mapper.hpp"
#include "server/ccache_tracker.hpp"
#include "server/durable_range.hpp"
#include "server/inode_removal_tracker.hpp"
#include "server/reply_set.hpp"
#include "server/server_info.hpp"
#include "server/thread_group.hpp"

namespace cpfs {
namespace server {

BaseCpfsServer::BaseCpfsServer(const ConfigMgr& configs)
    : configs_(configs), shutting_down_(false) {}

BaseCpfsServer::~BaseCpfsServer() {}

ConfigMgr& BaseCpfsServer::configs() {
  return configs_;
}

void BaseCpfsServer::set_tracker_mapper(ITrackerMapper* tracker_mapper) {
  tracker_mapper_.reset(tracker_mapper);
}

ITrackerMapper* BaseCpfsServer::tracker_mapper() {
  return tracker_mapper_.get();
}

void BaseCpfsServer::set_asio_policy(IAsioPolicy* asio_policy) {
  asio_policy_.reset(asio_policy);
}

IAsioPolicy* BaseCpfsServer::asio_policy() {
  return asio_policy_.get();
}

void BaseCpfsServer::set_fc_asio_policy(IAsioPolicy* asio_policy) {
  fc_asio_policy_.reset(asio_policy);
}

IAsioPolicy* BaseCpfsServer::fc_asio_policy() {
  return fc_asio_policy_.get();
}

void BaseCpfsServer::set_aux_asio_policy(IAsioPolicy* asio_policy) {
  aux_asio_policy_.reset(asio_policy);
}

IAsioPolicy* BaseCpfsServer::aux_asio_policy() {
  return aux_asio_policy_.get();
}

void BaseCpfsServer::set_io_service_runner(
    IIOServiceRunner* io_service_runner) {
  io_service_runner_.reset(io_service_runner);
}

IIOServiceRunner* BaseCpfsServer::io_service_runner() {
  return io_service_runner_.get();
}

void BaseCpfsServer::set_fim_socket_maker(FimSocketMaker fim_socket_maker) {
  fim_socket_maker_ = fim_socket_maker;
}

FimSocketMaker BaseCpfsServer::fim_socket_maker() {
  return fim_socket_maker_;
}

void BaseCpfsServer::set_connector(IConnector* connector) {
  connector_.reset(connector);
}

IConnector* BaseCpfsServer::connector() {
  return connector_.get();
}

void BaseCpfsServer::set_init_fim_processor(IFimProcessor* proc) {
  init_fim_processor_.reset(proc);
}

IFimProcessor* BaseCpfsServer::init_fim_processor() {
  return init_fim_processor_.get();
}

void BaseCpfsServer::set_ms_fim_processor(IFimProcessor* proc) {
  ms_fim_processor_.reset(proc);
}

IFimProcessor* BaseCpfsServer::ms_fim_processor() {
  return ms_fim_processor_.get();
}

void BaseCpfsServer::set_ds_fim_processor(IFimProcessor* proc) {
  ds_fim_processor_.reset(proc);
}

IFimProcessor* BaseCpfsServer::ds_fim_processor() {
  return ds_fim_processor_.get();
}

void BaseCpfsServer::set_fc_fim_processor(IFimProcessor* proc) {
  fc_fim_processor_.reset(proc);
}

IFimProcessor* BaseCpfsServer::fc_fim_processor() {
  return fc_fim_processor_.get();
}

void BaseCpfsServer::set_thread_group(IThreadGroup* thread_group) {
  thread_group_.reset(thread_group);
}

IThreadGroup* BaseCpfsServer::thread_group() {
  return thread_group_.get();
}

void BaseCpfsServer::set_cache_tracker(ICCacheTracker* cache_tracker) {
  cache_tracker_.reset(cache_tracker);
}

ICCacheTracker* BaseCpfsServer::cache_tracker() {
  return cache_tracker_.get();
}

void BaseCpfsServer::set_recent_reply_set(IReplySet* recent_reply_set) {
  recent_reply_set_.reset(recent_reply_set);
}

IReplySet* BaseCpfsServer::recent_reply_set() {
  return recent_reply_set_.get();
}

void BaseCpfsServer::set_server_info(IServerInfo* server_info) {
  server_info_.reset(server_info);
}

IServerInfo* BaseCpfsServer::server_info() {
  return server_info_.get();
}

void BaseCpfsServer::set_inode_removal_tracker(IInodeRemovalTracker* tracker) {
  inode_removal_tracker_.reset(tracker);
}

IInodeRemovalTracker* BaseCpfsServer::inode_removal_tracker() {
  return inode_removal_tracker_.get();
}

void BaseCpfsServer::set_shutdown_mgr(IShutdownMgr* shutdown_mgr) {
  shutdown_mgr_.reset(shutdown_mgr);
}

IShutdownMgr* BaseCpfsServer::shutdown_mgr() {
  return shutdown_mgr_.get();
}

void BaseCpfsServer::set_status_dumper(IStatusDumper* status_dumper) {
  status_dumper_.reset(status_dumper);
}

IStatusDumper* BaseCpfsServer::status_dumper() {
  return status_dumper_.get();
}

void BaseCpfsServer::set_authenticator(IAuthenticator* authenticator) {
  authenticator_.reset(authenticator);
}

IAuthenticator* BaseCpfsServer::authenticator() {
  return authenticator_.get();
}


void BaseCpfsServer::set_tracer(ITracer* tracer) {
  tracer_.reset(tracer);
}

ITracer* BaseCpfsServer::tracer() {
  return tracer_.get();
}

void BaseCpfsServer::set_durable_range(IDurableRange* durable_range) {
  durable_range_.reset(durable_range);
}

IDurableRange* BaseCpfsServer::durable_range() {
  return durable_range_.get();
}

void BaseCpfsServer::Init() {}

int BaseCpfsServer::Run() {
  return 0;
}

void BaseCpfsServer::Shutdown() {
  shutting_down_ = true;
  tracker_mapper()->Reset();
}

bool BaseCpfsServer::IsShuttingDown() {
  return shutting_down_;
}

void BaseCpfsServer::Dump() {
  tracker_mapper_->DumpPendingRequests();
  std::vector<std::string> ops = tracer()->DumpAll();
  for (unsigned i = 0; i < ops.size(); ++i)
    LOG(notice, Server, "[Operation Log] ", ops[i]);
}

}  // namespace server
}  // namespace cpfs
