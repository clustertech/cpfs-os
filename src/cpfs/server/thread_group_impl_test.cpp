/* Copyright 2013 ClusterTech Ltd */
#include "server/thread_group_impl.hpp"

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "mock_actions.hpp"
#include "req_tracker_mock.hpp"
#include "thread_fim_processor_impl.hpp"
#include "thread_fim_processor_mock.hpp"
#include "server/thread_group.hpp"
#include "server/worker_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::InvokeWithoutArgs;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::StartsWith;
using ::testing::Throw;

namespace cpfs {
namespace server {
namespace {

class ThreadGroupTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<IThreadGroup> thread_group_;
  boost::scoped_ptr<MockIReqTracker> tracker_;

  ThreadGroupTest()
      : thread_group_(MakeThreadGroup()),
        tracker_(new MockIReqTracker) {
    thread_group_->SetThreadFimProcessorMaker(kThreadFimProcessorMaker);
    EXPECT_CALL(*tracker_, peer_client_num())
        .WillRepeatedly(Return(kNotClient));
  }

  ~ThreadGroupTest() {
    thread_group_->Stop();
  }

  FIM_PTR<IFim> MakeFim(InodeNum inode) {
    ReqContext context = {100, 100, {0, 0}};
    FIM_PTR<GetattrFim> fim = GetattrFim::MakePtr();
    fim->set_req_id(100);
    (*fim)->inode = inode;
    (*fim)->context = context;
    return fim;
  }

  void TestProcessFim(MockIWorker* worker, FIM_PTR<IFim> fim,
                      boost::shared_ptr<IFimSocket> sender) {
    EXPECT_CALL(*worker, Accept(fim))
        .WillOnce(Return(true));
    EXPECT_CALL(*worker, Process(fim, sender))
        .WillOnce(Return(true));

    thread_group_->Process(fim, sender);
    Sleep(0.1)();
    Mock::VerifyAndClear(worker);
  }
};

TEST_F(ThreadGroupTest, Basic) {
  EXPECT_EQ(0U, thread_group_->num_workers());

  MockIWorker* worker = new MockIWorker;
  EXPECT_CALL(*worker, SetQueuer(_));

  thread_group_->AddWorker(worker);

  EXPECT_EQ(1U, thread_group_->num_workers());
  thread_group_->Start();

  boost::shared_ptr<MockIFimSocket> sender =
      boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*sender, GetReqTracker())
      .WillRepeatedly(Return(tracker_.get()));
  EXPECT_CALL(*sender, ShutdownCalled())
      .WillRepeatedly(Return(false));

  TestProcessFim(worker, MakeFim(1), sender);

  // Accept
  EXPECT_CALL(*worker, Accept(_))
      .WillOnce(Return(true));
  EXPECT_TRUE(thread_group_->Accept(MakeFim(2)));
}

TEST_F(ThreadGroupTest, Async) {
  EXPECT_EQ(0U, thread_group_->num_workers());

  MockIWorker* worker = new MockIWorker;
  EXPECT_CALL(*worker, SetQueuer(_));

  thread_group_->AddWorker(worker);

  EXPECT_EQ(1U, thread_group_->num_workers());
  thread_group_->Start();
  thread_group_->Stop(0);
  Sleep(0.01)();
  // Can start again
  thread_group_->Start();
  thread_group_->Stop();
}

TEST_F(ThreadGroupTest, AsyncSlow) {
  EXPECT_EQ(0U, thread_group_->num_workers());

  MockIWorker* worker = new MockIWorker;
  EXPECT_CALL(*worker, SetQueuer(_));

  thread_group_->AddWorker(worker);

  EXPECT_EQ(1U, thread_group_->num_workers());
  thread_group_->Start();

  // Send some work to the worker
  boost::shared_ptr<MockIFimSocket> m_sender
      = boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<IFimSocket> sender = m_sender;
  EXPECT_CALL(*m_sender, GetReqTracker())
      .WillRepeatedly(Return(tracker_.get()));
  EXPECT_CALL(*m_sender, ShutdownCalled())
      .WillRepeatedly(Return(false));
  FIM_PTR<IFim> fim = MakeFim(1);
  EXPECT_CALL(*worker, Accept(fim))
      .WillOnce(Return(true));
  EXPECT_CALL(*worker, Process(fim, sender))
      .WillOnce(DoAll(InvokeWithoutArgs(Sleep(0.05)),
                      Return(true)));

  thread_group_->Process(fim, sender);
  Sleep(0.01)();

  thread_group_->Stop(0.01);
  Sleep(0.1)();
}

TEST_F(ThreadGroupTest, MultiWorker) {
  MockIWorker* worker1 = new MockIWorker;
  EXPECT_CALL(*worker1, SetQueuer(_));

  thread_group_->AddWorker(worker1);

  MockIWorker* worker2 = new MockIWorker;
  EXPECT_CALL(*worker2, SetQueuer(_));

  thread_group_->AddWorker(worker2);

  thread_group_->Start();

  boost::shared_ptr<MockIFimSocket> sender1 =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<MockIFimSocket> sender2 =
      boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*sender1, GetReqTracker())
      .WillRepeatedly(Return(tracker_.get()));
  EXPECT_CALL(*sender2, GetReqTracker())
      .WillRepeatedly(Return(tracker_.get()));
  EXPECT_CALL(*sender1, ShutdownCalled())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*sender2, ShutdownCalled())
      .WillRepeatedly(Return(false));

  // Test FIMs
  FIM_PTR<IFim> fim1 = MakeFim(1);
  FIM_PTR<IFim> fim2 = MakeFim(2);

  // Test FIMs are queued to the corresponding worker according
  // to the inode number
  TestProcessFim(worker2, fim1, sender1);
  TestProcessFim(worker1, fim2, sender2);

  // Same inode should enqueue to the same worker again
  TestProcessFim(worker1, fim2, sender2);

  // Test EnqueueAll
  FIM_PTR<IFim> fim3 = MakeFim(3);
  EXPECT_CALL(*worker1, Accept(fim3))
      .WillOnce(Return(true));
  EXPECT_CALL(*worker1, Process(fim3, boost::shared_ptr<IFimSocket>()))
      .WillOnce(Return(true));
  EXPECT_CALL(*worker2, Accept(fim3))
      .WillOnce(Return(false));

  thread_group_->EnqueueAll(fim3);
  Sleep(0.1)();
}

TEST_F(ThreadGroupTest, SocketPending) {
  // Alter thread fim processor maker and add workers
  MockIWorker worker;
  MockThreadFimProcessorMaker thread_fim_processor_maker;
  thread_group_->SetThreadFimProcessorMaker(
      thread_fim_processor_maker.GetMaker());

  MockIThreadFimProcessor* wthread1(new MockIThreadFimProcessor);
  EXPECT_CALL(thread_fim_processor_maker, Make(&worker))
      .WillOnce(Return(wthread1));
  EXPECT_CALL(worker, SetQueuer(wthread1));

  thread_group_->AddWorker(&worker);

  MockIThreadFimProcessor* wthread2(new MockIThreadFimProcessor);
  EXPECT_CALL(thread_fim_processor_maker, Make(&worker))
      .WillOnce(Return(wthread2));
  EXPECT_CALL(worker, SetQueuer(wthread2));

  thread_group_->AddWorker(&worker);

  // Query SocketPending: true case
  boost::shared_ptr<IFimSocket> sender1 =
      boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*wthread1, SocketPending(sender1))
      .WillOnce(Return(true));

  EXPECT_TRUE(thread_group_->SocketPending(sender1));

  // Query SocketPending: false case
  EXPECT_CALL(*wthread1, SocketPending(sender1))
      .WillOnce(Return(false));
  EXPECT_CALL(*wthread2, SocketPending(sender1))
      .WillOnce(Return(false));

  EXPECT_FALSE(thread_group_->SocketPending(sender1));

  // Stopping
  EXPECT_CALL(*wthread1, Stop());
  EXPECT_CALL(*wthread1, Join());
  EXPECT_CALL(*wthread2, Stop());
  EXPECT_CALL(*wthread2, Join());
}

}  // namespace
}  // namespace server
}  // namespace cpfs
