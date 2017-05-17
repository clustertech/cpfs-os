/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Integration point for CPFS Admin Client.
 */

#include "client/admin_client_impl.hpp"

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
#include "periodic_timer_impl.hpp"
#include "tracker_mapper.hpp"
#include "tracker_mapper_impl.hpp"
#include "client/admin_fim_processor.hpp"
#include "client/base_client.hpp"
#include "client/conn_mgr.hpp"
#include "client/conn_mgr_impl.hpp"

namespace cpfs {

namespace client {

namespace {

/**
 * The Admin client class. This is meant for component integration
 */
class AdminClient : public BaseAdminClient {
 public:
  /**
   * The Admin client
   */
  explicit AdminClient(const AdminConfigItems& configs) {
    IAsioPolicy* asio_policy = MakeAsioPolicy();
    set_asio_policy(asio_policy);
    set_service_runner(MakeIOServiceRunner());
    service_runner()->AddService(asio_policy->io_service());
    set_tracker_mapper(MakeTrackerMapper(asio_policy));

    IAuthenticator* authenticator;
    set_authenticator(authenticator = MakeAuthenticator(
        MakeCrypto(), asio_policy));
    authenticator->SetPeriodicTimerMaker(kPeriodicTimerMaker,
                                         kUnauthenticatedSocketAge);
    authenticator->Init();

    IConnector* connector = MakeConnector(asio_policy, authenticator);
    set_connector(connector);
    connector->SetFimSocketMaker(kFimSocketMaker);
    connector->SetConnectingSocketMaker(kConnectingSocketMaker);
    connector->set_heartbeat_interval(configs.heartbeat_interval);
    connector->set_socket_read_timeout(configs.socket_read_timeout);

    CompositeFimProcessor* ms_proc = new CompositeFimProcessor;
    ms_proc->AddFimProcessor(new BaseFimProcessor);
    ms_proc->AddFimProcessor(MakeAdminFimProcessor(this));
    set_ms_fim_processor(ms_proc);
    CompositeFimProcessor* ds_proc = new CompositeFimProcessor;
    ds_proc->AddFimProcessor(new BaseFimProcessor);
    set_ds_fim_processor(ds_proc);

    set_conn_mgr(MakeConnMgr(this, 'A'));
    conn_mgr()->Init(configs.meta_servers);
  }

  ~AdminClient() {
    BaseAdminClient::Shutdown();
    asio_policy()->io_service()->stop();
    tracker_mapper()->Reset();
    service_runner()->Join();
  }
};

}  // namespace

BaseAdminClient* MakeAdminClient(const AdminConfigItems& configs) {
  return new AdminClient(configs);
}

}  // namespace client
}  // namespace cpfs
