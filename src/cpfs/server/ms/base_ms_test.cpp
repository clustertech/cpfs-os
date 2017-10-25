/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/base_ms.hpp"

#include <csignal>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/unordered_map.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy_mock.hpp"
#include "authenticator_mock.hpp"
#include "common.hpp"
#include "config_mgr.hpp"
#include "connector_mock.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "io_service_runner_mock.hpp"
#include "shutdown_mgr_mock.hpp"
#include "thread_fim_processor_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/ms/conn_mgr_mock.hpp"
#include "server/ms/dirty_inode_mock.hpp"
#include "server/ms/inode_src_mock.hpp"
#include "server/ms/startup_mgr_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/store_mock.hpp"
#include "server/ms/topology_mock.hpp"
#include "server/ms/ugid_handler_mock.hpp"
#include "server/thread_group_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Pointee;
using ::testing::Return;
using ::testing::Mock;
using ::testing::SaveArg;

namespace cpfs {
namespace server {
namespace ms {
namespace {

ConfigMgr InitConfig() {
  ConfigMgr ret;
  ret.set_ds_port(7777);
  ret.set_ms1_port(0);
  ret.set_ms2_port(0);
  return ret;
}

TEST(BaseMSTest, HAMode) {
  ConfigMgr cfgs[2];
  cfgs[0].set_ms1_host("0.0.0.0");
  cfgs[0].set_ms2_host("0.0.0.0");
  cfgs[1].set_ms1_host("0.0.0.0");
  BaseMetaServer ms1(cfgs[0]);
  EXPECT_TRUE(ms1.IsHAMode());
  BaseMetaServer ms2(cfgs[1]);
  EXPECT_FALSE(ms2.IsHAMode());
}

TEST(BaseMSTest, GetSet) {
  BaseMetaServer ms(InitConfig());
  EXPECT_EQ(0, ms.store());
  ms.set_store(0);
  EXPECT_EQ(0, ms.conn_mgr());
  ms.set_conn_mgr(0);
  EXPECT_EQ(0, ms.startup_mgr());
  ms.set_startup_mgr(0);
  EXPECT_EQ(0, ms.ha_counter());
  ms.set_ha_counter(0);
  EXPECT_EQ(0, ms.state_mgr());
  ms.set_state_mgr(0);
  EXPECT_EQ(0, ms.ugid_handler());
  ms.set_ugid_handler(0);
  EXPECT_EQ(0, ms.dsg_allocator());
  ms.set_dsg_allocator(0);
  EXPECT_EQ(0, ms.inode_src());
  ms.set_inode_src(0);
  EXPECT_EQ(0U, ms.client_num_counter());
  EXPECT_EQ(0U, ms.ds_locker());
  ms.set_ds_locker(0);
  EXPECT_EQ(0, ms.inode_usage());
  ms.set_inode_usage(0);
  EXPECT_EQ(0, ms.replier());
  ms.set_replier(0);
  EXPECT_EQ(0, ms.topology_mgr());
  ms.set_topology_mgr(0);
  EXPECT_EQ(0, ms.cleaner());
  ms.set_cleaner(0);
  ms.set_failover_processor(0);
  EXPECT_EQ(0, ms.failover_processor());
  ms.set_failover_mgr(0);
  EXPECT_EQ(0, ms.failover_mgr());
  ms.set_resync_mgr(0);
  EXPECT_EQ(0, ms.resync_mgr());
  ms.set_meta_dir_reader(0);
  EXPECT_EQ(0, ms.meta_dir_reader());
  ms.set_peer_time_keeper(0);
  EXPECT_EQ(0, ms.peer_time_keeper());
  ms.set_ds_query_mgr(0);
  EXPECT_EQ(0, ms.ds_query_mgr());
  ms.set_stat_keeper(0);
  EXPECT_EQ(0, ms.stat_keeper());
  ms.set_stat_processor(0);
  EXPECT_EQ(0, ms.stat_processor());
  ms.set_dirty_inode_mgr(0);
  EXPECT_EQ(0, ms.dirty_inode_mgr());
  ms.set_attr_updater(0);
  EXPECT_EQ(0, ms.attr_updater());
  ms.set_admin_fim_processor(0);
  EXPECT_EQ(0, ms.admin_fim_processor());
  ms.StartServerActivated();
  ms.PrepareActivate();
  std::vector<InodeNum> resyncing;
  resyncing.push_back(2);
  resyncing.push_back(3);
  ms.set_dsg_inodes_resyncing(1, resyncing);
  {
    boost::shared_lock<boost::shared_mutex> lock;
    ms.ReadLockDSGOpState(1, &lock);
    EXPECT_TRUE(ms.is_dsg_inode_resyncing(1, 2));
    EXPECT_FALSE(ms.is_dsg_inode_resyncing(1, 4));
  }
}

class MSTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<BaseMetaServer> server_;
  MockIAsioPolicy* policy_;
  MockIAuthenticator* authenticator_;
  MockIOServiceRunner* service_runner_;
  MockFimSocketMaker fsm_;
  MockIConnector* connector_;
  MockIFimProcessor* init_processor_;
  MockIStartupMgr* startup_mgr_;
  MockIStore* store_;
  MockIConnMgr* conn_mgr_;
  MockIStateMgr* state_mgr_;
  MockITopologyMgr* topology_mgr_;
  MockIThreadFimProcessor* failover_proc_;
  MockIInodeSrc* inode_src_;
  MockIShutdownMgr* shutdown_mgr_;
  MockIThreadFimProcessor* stat_proc_;
  MockIDirtyInodeMgr* dirty_inode_mgr_;
  MockIThreadGroup* thread_group_;
  MockIUgidHandler* ugid_handler_;
  MockIDurableRange* durable_range_;
  IOService service_;

  ConfigMgr MakeConfig(const std::string& role, bool ha_mode) {
    ConfigMgr cfg;
    cfg.set_role(role);
    cfg.set_ms1_host("0.0.0.0");
    cfg.set_ms1_port(4000);
    if (ha_mode) {
      cfg.set_ms2_host("1.2.3.4");
      cfg.set_ms2_port(4001);
    }
    cfg.set_heartbeat_interval(15.0);
    cfg.set_socket_read_timeout(30.0);
    return cfg;
  }

  void Init(ConfigMgr cfg) {
    server_.reset(MakeMetaServer(cfg));
    server_->set_asio_policy(policy_ = new MockIAsioPolicy);
    server_->set_authenticator(authenticator_ = new MockIAuthenticator);
    server_->set_io_service_runner(service_runner_ = new MockIOServiceRunner);
    server_->set_fim_socket_maker(fsm_.GetMaker());
    server_->set_connector(connector_ = new MockIConnector);
    server_->set_init_fim_processor(init_processor_ = new MockIFimProcessor);
    server_->set_startup_mgr(startup_mgr_ = new MockIStartupMgr);
    server_->set_store(store_ = new MockIStore);
    server_->set_conn_mgr(conn_mgr_ = new MockIConnMgr);
    server_->set_state_mgr(state_mgr_ = new MockIStateMgr);
    server_->set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    server_->set_failover_processor(
        failover_proc_ = new MockIThreadFimProcessor);
    server_->set_inode_src(inode_src_ = new MockIInodeSrc);
    server_->set_shutdown_mgr(shutdown_mgr_ = new MockIShutdownMgr);
    server_->set_stat_processor(stat_proc_ = new MockIThreadFimProcessor);
    server_->set_dirty_inode_mgr(dirty_inode_mgr_ = new MockIDirtyInodeMgr);
    server_->set_thread_group(thread_group_ = new MockIThreadGroup);
    server_->set_ugid_handler(ugid_handler_ = new MockIUgidHandler);
    server_->set_durable_range(durable_range_ = new MockIDurableRange);
    EXPECT_CALL(*policy_, io_service())
        .WillRepeatedly(Return(&service_));
  }
};

TEST_F(MSTest, Standalone) {
  Init(MakeConfig("MS1", false));

  // Initialize
  EXPECT_CALL(*topology_mgr_, Init());
  EXPECT_CALL(*startup_mgr_, Init());
  EXPECT_CALL(*shutdown_mgr_, SetupSignals(Pointee(SIGUSR1), 1));
  EXPECT_CALL(*store_, IsInitialized())
      .WillOnce(Return(false));
  EXPECT_CALL(*store_, Initialize());
  EXPECT_CALL(*store_, PrepareUUID());
  EXPECT_CALL(*inode_src_, Init());
  EXPECT_CALL(*authenticator_, Init());
  EXPECT_CALL(*stat_proc_, Start());
  EXPECT_CALL(*state_mgr_, SwitchState(kStateStandalone));
  EXPECT_CALL(*dirty_inode_mgr_, Reset(true));
  DirtyInodeMap dim;
  dim[41].active = true;
  dim[41].gen = 62;
  dim[42].clean = false;
  dim[42].gen = 63;
  EXPECT_CALL(*dirty_inode_mgr_, GetList())
      .WillOnce(Return(dim));
  FIM_PTR<IFim> processed;
  EXPECT_CALL(*thread_group_, Process(_, _))
      .WillOnce(DoAll(SaveArg<0>(&processed),
                      Return(true)));
  EXPECT_CALL(*ugid_handler_, SetCheck(false));
  EXPECT_CALL(*topology_mgr_, StartStopWorker());
  EXPECT_CALL(*inode_src_, SetupAllocation());

  server_->Init();
  InodeCleanAttrFim& rprocessed = dynamic_cast<InodeCleanAttrFim&>(*processed);
  EXPECT_EQ(42U, rprocessed->inode);
  EXPECT_EQ(63U, rprocessed->gen);

  // Run the server
  EXPECT_CALL(*conn_mgr_, Init());
  EXPECT_CALL(*connector_, Listen(_, _, _));
  EXPECT_CALL(*service_runner_, Run());
  EXPECT_CALL(*service_runner_, Join());

  server_->Run();

  Mock::VerifyAndClear(&fsm_);
}

TEST_F(MSTest, MS2) {
  Init(MakeConfig("MS2", true));

  // Initialize
  EXPECT_CALL(*topology_mgr_, Init());
  EXPECT_CALL(*startup_mgr_, Init());
  EXPECT_CALL(*shutdown_mgr_, SetupSignals(Pointee(SIGUSR1), 1));
  EXPECT_CALL(*store_, IsInitialized())
      .WillOnce(Return(false));
  EXPECT_CALL(*store_, Initialize());
  EXPECT_CALL(*store_, PrepareUUID());
  EXPECT_CALL(*inode_src_, Init());
  EXPECT_CALL(*authenticator_, Init());
  EXPECT_CALL(*stat_proc_, Start());
  EXPECT_CALL(*failover_proc_, Start());
  EXPECT_CALL(*durable_range_, Load())
      .WillOnce(Return(true));

  server_->Init();

  // Run the server
  EXPECT_CALL(*conn_mgr_, Init());
  EXPECT_CALL(*connector_, Listen(_, _, _));
  EXPECT_CALL(*service_runner_, Run());
  EXPECT_CALL(*service_runner_, Join());

  server_->Run();
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
