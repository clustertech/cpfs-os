/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS API client.
 */
#include "client/access_api_main.hpp"

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "asio_policy_impl.hpp"
#include "authenticator.hpp"
#include "authenticator_impl.hpp"
#include "common.hpp"
#include "composite_fim_processor.hpp"
#include "connecting_socket_impl.hpp"
#include "connector.hpp"
#include "connector_impl.hpp"
#include "crypto_impl.hpp"
#include "fim_processor_base.hpp"
#include "fim_socket_impl.hpp"
#include "io_service_runner.hpp"
#include "io_service_runner_impl.hpp"
#include "op_completion_impl.hpp"
#include "periodic_timer_impl.hpp"
#include "shutdown_mgr.hpp"
#include "status_dumper.hpp"
#include "status_dumper_impl.hpp"
#include "tracker_mapper.hpp"
#include "tracker_mapper_impl.hpp"
#include "client/api_common.hpp"
#include "client/api_common_impl.hpp"
#include "client/base_client.hpp"
#include "client/cache.hpp"
#include "client/cache_impl.hpp"
#include "client/cleaner.hpp"
#include "client/cleaner_impl.hpp"
#include "client/conn_mgr_impl.hpp"
#include "client/fim_processors.hpp"
#include "client/fs_common_lowlevel.hpp"
#include "client/fs_common_lowlevel_impl.hpp"
#include "client/inode_usage_impl.hpp"
#include "client/shutdown_mgr_impl.hpp"

namespace cpfs {
namespace client {
namespace {

/**
 * The API client class.  This is meant for integration with other
 * components in the system.
 */
class APIClient : public BaseAPIClient {
 public:
  /**
   * @param num_threads Number of worker threads to use
   */
  explicit APIClient(unsigned num_threads) {
    set_asio_policy(MakeAsioPolicy());
    set_service_runner(MakeIOServiceRunner());

    set_tracker_mapper(MakeTrackerMapper(asio_policy()));
    IAuthenticator* authenticator;
    set_authenticator(authenticator = MakeAuthenticator(
        MakeCrypto(), asio_policy()));
    authenticator->SetPeriodicTimerMaker(kPeriodicTimerMaker,
                                         kUnauthenticatedSocketAge);
    authenticator->Init();
    IConnector* connector = MakeConnector(asio_policy(), authenticator);
    set_connector(connector);
    connector->SetFimSocketMaker(kFimSocketMaker);
    connector->SetConnectingSocketMaker(kConnectingSocketMaker);

    set_cache_mgr(MakeCacheMgr(0, new NullCachePolicy));
    CompositeFimProcessor* ms_proc = new CompositeFimProcessor;
    ms_proc->AddFimProcessor(new BaseFimProcessor);
    ms_proc->AddFimProcessor(MakeGenCtrlFimProcessor(this));
    ms_proc->AddFimProcessor(MakeMSCtrlFimProcessor(this));
    set_ms_fim_processor(ms_proc);
    CompositeFimProcessor* ds_proc = new CompositeFimProcessor;
    ds_proc->AddFimProcessor(new BaseFimProcessor);
    ds_proc->AddFimProcessor(MakeGenCtrlFimProcessor(this));
    ds_proc->AddFimProcessor(MakeDSCtrlFimProcessor(this));
    set_ds_fim_processor(ds_proc);
    set_conn_mgr(MakeConnMgr(this, 'F'));
    set_inode_usage_set(MakeInodeUsageSet());
    set_op_completion_checker_set(MakeOpCompletionCheckerSet());
    ICleaner* cleaner;
    set_cleaner(cleaner = MakeCleaner(this));
    cleaner->SetPeriodicTimerMaker(kPeriodicTimerMaker);
    cleaner->Init();
    IShutdownMgr* shutdown_mgr = MakeShutdownMgr(this);
    set_shutdown_mgr(shutdown_mgr);
    shutdown_mgr->SetAsioPolicy(asio_policy());
    set_status_dumper(MakeStatusDumper(this));
    status_dumper()->SetAsioPolicy(asio_policy());
    set_fs_ll(MakeFSCommonLL());
    fs_ll()->SetClient(this);
    set_api_common(MakeAPICommon(fs_ll()));
    set_fs_async_rw(MakeAsyncRWExecutor(api_common()));
    fs_async_rw()->Start(num_threads);
  }

  void Init() {
    service_runner()->AddService(asio_policy()->io_service());
  }

  void Shutdown() {
    this->BaseClient::Shutdown();
    cache_mgr()->Shutdown();
    asio_policy()->io_service()->stop();
    tracker_mapper()->Reset();
    service_runner()->Stop();
    service_runner()->Join();
  }
};

}  // namespace

BaseAPIClient* MakeAPIClient(unsigned num_threads) {
  return new APIClient(num_threads);
}

}  // namespace client
}  // namespace cpfs
