/* Copyright 2013 ClusterTech Ltd */
#include "client/base_client.hpp"

#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "io_service_runner_mock.hpp"
#include "mock_actions.hpp"
#include "service_mock.hpp"
#include "tracker_mapper_mock.hpp"

namespace cpfs {
namespace client {
namespace {

TEST(BaseClientTest, GetSet) {
  BaseFSClient client;
  MockIService* service = new MockIService;
  MockIOServiceRunner* service_runner;
  MockITrackerMapper* tracker_mapper;

  EXPECT_EQ(0, client.asio_policy());
  client.set_asio_policy(0);
  EXPECT_EQ(0, client.service_runner());
  client.set_service_runner(service_runner = new MockIOServiceRunner);
  EXPECT_EQ(0, client.tracker_mapper());
  client.set_tracker_mapper(tracker_mapper = new MockITrackerMapper);
  EXPECT_EQ(0, client.connector());
  client.set_connector(0);
  EXPECT_EQ(0, client.ms_fim_processor());
  client.set_ms_fim_processor(0);
  EXPECT_EQ(0, client.ds_fim_processor());
  client.set_ds_fim_processor(0);
  EXPECT_EQ(0, client.conn_mgr());
  client.set_conn_mgr(0);
  EXPECT_EQ(0, client.cache_mgr());
  client.set_cache_mgr(0);
  EXPECT_EQ(0, client.inode_usage_set());
  client.set_inode_usage_set(0);
  EXPECT_EQ(0, client.req_completion_checker_set());
  client.set_req_completion_checker_set(0);
  EXPECT_EQ(0, client.cleaner());
  client.set_cleaner(0);
  EXPECT_EQ(0, client.runner());
  client.set_runner(service);
  EXPECT_EQ(0, client.shutdown_mgr());
  client.set_shutdown_mgr(0);
  EXPECT_EQ(0, client.status_dumper());
  client.set_status_dumper(0);
  EXPECT_EQ(0, client.authenticator());
  client.set_authenticator(0);
  EXPECT_EQ(0U, client.client_num());
  client.set_client_num(1U);

  client.Init();
  EXPECT_CALL(*service, Run());
  EXPECT_CALL(*service_runner, Stop());
  EXPECT_CALL(*service_runner, Join());
  client.Run();

  EXPECT_CALL(*tracker_mapper, DumpPendingRequests());

  client.Dump();

  EXPECT_FALSE(client.IsShuttingDown());
  client.Shutdown();
  EXPECT_TRUE(client.IsShuttingDown());
  GroupId failed;
  EXPECT_EQ(kDSGPending, client.dsg_state(2, &failed));
  EXPECT_EQ(kDSGPending, client.set_dsg_state(2, kDSGDegraded, 3));
  EXPECT_EQ(kDSGDegraded, client.dsg_state(2, &failed));
  EXPECT_EQ(3U, failed);
}

TEST(BaseClientTest, APIGetSet) {
  BaseAPIClient client;
  client.set_fs_ll(0);
  EXPECT_EQ(0, client.fs_ll());
  client.set_api_common(0);
  EXPECT_EQ(0, client.api_common());
  client.set_fs_async_rw(0);
  EXPECT_EQ(0, client.fs_async_rw());
}

void WaitAdminClientReady(BaseAdminClient* client) {
  EXPECT_TRUE(client->WaitReady(1));
}

TEST(BaseClientTest, AdminGetSet) {
  BaseAdminClient client;
  boost::thread th(boost::bind(&WaitAdminClientReady, &client));
  Sleep(0.1)();
  client.SetReady(true);
  th.join();
}

}  // namespace
}  // namespace client
}  // namespace cpfs
