/* Copyright 2013 ClusterTech Ltd */
#include "server/ds/base_ds.hpp"

#include <stdint.h>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "authenticator_mock.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "connector_mock.hpp"
#include "dsg_state.hpp"
#include "fim_processor_mock.hpp"
#include "finfo.hpp"  // IWYU pragma: keep (need by gtest over store.hpp)
#include "io_service_runner_mock.hpp"
#include "mock_actions.hpp"
#include "thread_fim_processor_mock.hpp"
#include "server/ds/conn_mgr_mock.hpp"
#include "server/ds/store_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;

namespace cpfs {
namespace server {
namespace ds {
namespace {

ConfigMgr InitConfig() {
  ConfigMgr ret;
  ret.set_ds_port(7777);
  ret.set_ms1_port(0);
  ret.set_ms2_port(0);
  return ret;
}

void SetDSGState(BaseDataServer* ds, DSGroupState state, GroupRole role) {
  Sleep(0.1)();
  boost::unique_lock<boost::shared_mutex> lock;
  ds->set_dsg_state(42, state, role, &lock);
}

TEST(BaseDSTest, GetSet) {
  BaseDataServer ds(InitConfig());
  EXPECT_EQ(0, ds.ds_asio_policy());
  ds.set_ds_asio_policy(0);
  EXPECT_EQ(0, ds.posix_fs());
  ds.set_posix_fs(0);
  EXPECT_EQ(0, ds.store());
  ds.set_store(0);
  EXPECT_EQ(0, ds.cleaner());
  ds.set_cleaner(0);
  EXPECT_EQ(0, ds.conn_mgr());
  ds.set_conn_mgr(0);
  EXPECT_EQ(0, ds.dsg_ready_time_keeper());
  ds.set_dsg_ready_time_keeper(0);
  EXPECT_EQ(0, ds.degraded_cache());
  ds.set_degraded_cache(0);
  EXPECT_EQ(0, ds.data_recovery_mgr());
  ds.set_data_recovery_mgr(0);
  EXPECT_EQ(0, ds.req_completion_checker_set());
  ds.set_req_completion_checker_set(0);
  EXPECT_EQ(0, ds.resync_mgr());
  ds.set_resync_mgr(0);
  EXPECT_EQ(0, ds.resync_fim_processor());
  ds.set_resync_fim_processor(0);
  EXPECT_EQ(0, ds.resync_thread_processor());
  ds.set_resync_thread_processor(0);
  uint64_t state_change_id;
  GroupRole failed;
  EXPECT_EQ(kDSGPending, ds.dsg_state(&state_change_id, &failed));
  ds.set_dsg_state(20, kDSGReady, 3);
  ds.set_dsg_state(20, kDSGReady, 3);
  boost::thread thread(boost::bind(SetDSGState, &ds, kDSGDegraded, 0));
  {
    boost::shared_lock<boost::shared_mutex> lock1, lock2;
    ds.ReadLockDSGState(&lock1);
    ds.ReadLockDSGState(&lock2);
    Sleep(0.2)();
    EXPECT_EQ(kDSGReady, ds.dsg_state(&state_change_id, &failed));
    EXPECT_EQ(20U, state_change_id);
    EXPECT_EQ(3U, failed);
  }
  Sleep(0.1)();
  EXPECT_EQ(kDSGDegraded, ds.dsg_state(&state_change_id, &failed));
  EXPECT_EQ(42U, state_change_id);
  EXPECT_EQ(0U, failed);
  EXPECT_FALSE(ds.opt_resync());
  ds.set_opt_resync(true);
  EXPECT_TRUE(ds.opt_resync());
  ds.set_distressed(true);
  EXPECT_TRUE(ds.distressed());
}

TEST(DSTest, Init) {
  ConfigMgr ds_cfg;
  ds_cfg.set_role("DS");
  ds_cfg.set_ms1_host("0.0.0.0");
  ds_cfg.set_ms1_port(5000);
  ds_cfg.set_ds_host("0.0.0.0");
  ds_cfg.set_ds_port(4000);
  ds_cfg.set_heartbeat_interval(15.0);
  ds_cfg.set_socket_read_timeout(30.0);
  boost::scoped_ptr<BaseDataServer> server(MakeDataServer(ds_cfg));
  MockIAsioPolicy* policy = new MockIAsioPolicy;
  server->set_asio_policy(policy);
  IOService service;
  EXPECT_CALL(*policy, io_service())
      .WillRepeatedly(Return(&service));
  MockIOServiceRunner* service_runner = new MockIOServiceRunner;
  server->set_io_service_runner(service_runner);
  MockIConnMgr* conn_mgr = new MockIConnMgr;
  server->set_conn_mgr(conn_mgr);
  MockIDurableRange* durable_range = new MockIDurableRange;
  server->set_durable_range(durable_range);
  MockIStore* store = new MockIStore;
  server->set_store(store);
  MockIFimProcessor* init_processor = new MockIFimProcessor;
  server->set_init_fim_processor(init_processor);
  MockIThreadGroup* thread_group = new MockIThreadGroup;
  server->set_thread_group(thread_group);
  MockIConnector* connector = new MockIConnector;
  server->set_connector(connector);
  MockIAuthenticator* authenticator = new MockIAuthenticator;
  server->set_authenticator(authenticator);
  MockIThreadFimProcessor* resync_fim_processor = new MockIThreadFimProcessor;
  server->set_resync_thread_processor(resync_fim_processor);
  EXPECT_CALL(*durable_range, Load());
  EXPECT_CALL(*thread_group, Start());
  EXPECT_CALL(*authenticator, Init());
  EXPECT_CALL(*resync_fim_processor, Start());
  server->Init();
  EXPECT_CALL(*conn_mgr, Init());
  EXPECT_CALL(*service_runner, Run());
  EXPECT_CALL(*service_runner, Join());
  EXPECT_CALL(*connector, Listen(_, _, _));

  server->Run();
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
