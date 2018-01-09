/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS.
 */

#include "client/client_main.hpp"

#include <boost/function.hpp>

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
#include "fuseobj.hpp"
#include "io_service_runner.hpp"
#include "io_service_runner_impl.hpp"
#include "op_completion_impl.hpp"
#include "periodic_timer_impl.hpp"
#include "shutdown_mgr.hpp"
#include "status_dumper.hpp"
#include "status_dumper_impl.hpp"
#include "thread_fim_processor.hpp"
#include "thread_fim_processor_impl.hpp"
#include "tracker_mapper.hpp"
#include "tracker_mapper_impl.hpp"
#include "client/base_client.hpp"
#include "client/cache.hpp"
#include "client/cache_impl.hpp"
#include "client/cleaner.hpp"
#include "client/cleaner_impl.hpp"
#include "client/conn_mgr_impl.hpp"
#include "client/cpfs_fso.hpp"
#include "client/fim_processors.hpp"
#include "client/inode_usage_impl.hpp"
#include "client/shutdown_mgr_impl.hpp"

namespace cpfs {

class IService;

namespace client {
namespace {

/**
 * The filesystem client class.  This is meant for integration of the fuse
 * runner with other components in the system.
 */
class FSClient : public BaseFSClient {
 public:
  /**
   * @param argc The number of command line arguments.
   *
   * @param argv The command line argument.
   */
  FSClient(int argc, char** argv) {
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

    set_cache_mgr(MakeCacheMgr(
        0,  // fuse_chan is set later
        new CacheInvPolicy<FuseMethodPolicy>(fso_.fuse_method_policy())));
    CompositeFimProcessor* ms_proc = new CompositeFimProcessor;
    ms_proc->AddFimProcessor(MakeGenCtrlFimProcessor(this));
    ms_proc->AddFimProcessor(MakeMSCtrlFimProcessor(this));
    IThreadFimProcessor* ms_proc_thread =
        kBasicThreadFimProcessorMaker(ms_proc);
    set_ms_fim_processor_thread(ms_proc_thread);
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
    BaseFuseRunner<CpfsFuseObj<> >* runner =
        new BaseFuseRunner<CpfsFuseObj<> >(&fso_);
    set_runner(runner);
    IShutdownMgr* shutdown_mgr = MakeShutdownMgr(this);
    set_shutdown_mgr(shutdown_mgr);
    shutdown_mgr->SetAsioPolicy(asio_policy());
    set_status_dumper(MakeStatusDumper(this));
    status_dumper()->SetAsioPolicy(asio_policy());
    runner->SetArgs(argc, argv);
  }

  void Init() {
    service_runner()->AddService(asio_policy()->io_service());
    fso_.SetClient(this);
  }

  void Shutdown() {
    ms_fim_processor_thread()->Stop();
    ms_fim_processor_thread()->Join();
    this->BaseClient::Shutdown();
    tracker_mapper()->Reset();
    cache_mgr()->Shutdown();
    asio_policy()->io_service()->stop();
    service_runner()->Join();
  }

 private:
  CpfsFuseObj<> fso_;
};

}  // namespace

IService* MakeFSClient(int argc, char** argv) {
  return new FSClient(argc, argv);
}

}  // namespace client
}  // namespace cpfs
