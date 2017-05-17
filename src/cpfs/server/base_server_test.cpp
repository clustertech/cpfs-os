/* Copyright 2013 ClusterTech Ltd */
#include "server/base_server.hpp"

#include <string>
#include <vector>

#include <boost/function.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "tracer_mock.hpp"
#include "tracker_mapper_mock.hpp"

using ::testing::Return;

namespace cpfs {
namespace server {
namespace {

ConfigMgr InitConfig() {
  ConfigMgr ret;
  ret.set_ds_port(7777);
  ret.set_ms1_port(0);
  ret.set_ms2_port(0);
  return ret;
}

TEST(ServerTest, BaseCpfsServer) {
  ConfigMgr config = InitConfig();
  BaseCpfsServer cs(config);
  MockITrackerMapper* tracker_mapper = new MockITrackerMapper;
  EXPECT_EQ(0, cs.tracker_mapper());
  cs.set_tracker_mapper(tracker_mapper);
  EXPECT_EQ(0, cs.asio_policy());
  cs.set_asio_policy(0);
  EXPECT_EQ(0, cs.fc_asio_policy());
  cs.set_fc_asio_policy(0);
  EXPECT_EQ(0, cs.aux_asio_policy());
  cs.set_aux_asio_policy(0);
  EXPECT_EQ(0, cs.io_service_runner());
  cs.set_io_service_runner(0);
  FimSocketMaker fsm = cs.fim_socket_maker();
  EXPECT_FALSE(fsm);
  cs.set_fim_socket_maker(fsm);
  EXPECT_EQ(0, cs.connector());
  cs.set_connector(0);
  EXPECT_EQ(0, cs.init_fim_processor());
  cs.set_init_fim_processor(0);
  EXPECT_EQ(0, cs.ms_fim_processor());
  cs.set_ms_fim_processor(0);
  EXPECT_EQ(0, cs.ds_fim_processor());
  cs.set_ds_fim_processor(0);
  EXPECT_EQ(0, cs.fc_fim_processor());
  cs.set_fc_fim_processor(0);
  EXPECT_EQ(0, cs.thread_group());
  cs.set_thread_group(0);
  EXPECT_EQ(0, cs.cache_tracker());
  cs.set_cache_tracker(0);
  EXPECT_EQ(0, cs.recent_reply_set());
  cs.set_recent_reply_set(0);
  EXPECT_EQ(0, cs.server_info());
  cs.set_server_info(0);
  cs.set_inode_removal_tracker(0);
  EXPECT_EQ(0, cs.inode_removal_tracker());
  cs.set_shutdown_mgr(0);
  EXPECT_EQ(0, cs.shutdown_mgr());
  cs.set_status_dumper(0);
  EXPECT_EQ(0, cs.status_dumper());
  cs.set_authenticator(0);
  EXPECT_EQ(0, cs.authenticator());
  MockITracer* tracer = new MockITracer;
  cs.set_tracer(tracer);
  EXPECT_EQ(0, cs.durable_range());
  cs.set_durable_range(0);
  cs.Init();
  cs.Run();
  EXPECT_FALSE(cs.IsShuttingDown());

  EXPECT_CALL(*tracker_mapper, DumpPendingRequests());
  std::vector<std::string> ops;
  ops.push_back("Dump Trace 1");
  EXPECT_CALL(*tracer, DumpAll())
      .WillOnce(Return(ops));

  cs.Dump();

  EXPECT_CALL(*tracker_mapper, Reset());

  cs.Shutdown();
  EXPECT_TRUE(cs.IsShuttingDown());
}

}  // namespace
}  // namespace server
}  // namespace cpfs
