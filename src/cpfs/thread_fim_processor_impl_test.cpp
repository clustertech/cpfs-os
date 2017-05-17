/* Copyright 2013 ClusterTech Ltd */
#include "thread_fim_processor_impl.hpp"

#include <stdexcept>

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "req_tracker_mock.hpp"
#include "thread_fim_processor.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::InvokeWithoutArgs;
using ::testing::Mock;
using ::testing::Return;
using ::testing::StartsWith;
using ::testing::Throw;

namespace cpfs {
namespace {

class ThreadFimProcessorTest : public ::testing::Test {
 protected:
  MockIFimProcessor* worker_;
  boost::scoped_ptr<IThreadFimProcessor> proc_;

  ThreadFimProcessorTest()
      : proc_(kThreadFimProcessorMaker(worker_ = new MockIFimProcessor)) {}

  FIM_PTR<IFim> MakeFim(InodeNum inode) {
    ReqContext context = {100, 100, {0, 0}};
    FIM_PTR<GetattrFim> fim = GetattrFim::MakePtr();
    fim->set_req_id(100);
    (*fim)->inode = inode;
    (*fim)->context = context;
    return fim;
  }

  void WaitProcessed(boost::shared_ptr<MockIFimSocket> sender) {
    int count = 500;
    while (proc_->SocketPending(sender)) {
      if (--count < 0)
        throw std::runtime_error("Process timeout");
      Sleep(0.01)();
    }
  }
};

TEST_F(ThreadFimProcessorTest, Process) {
  FIM_PTR<IFim> fim = MakeFim(1);
  boost::shared_ptr<MockIFimSocket> sender =
      boost::make_shared<MockIFimSocket>();
  boost::scoped_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  EXPECT_CALL(*sender, GetReqTracker())
      .WillRepeatedly(Return(tracker.get()));
  EXPECT_CALL(*worker_, Accept(_))
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*worker_, Process(fim, boost::shared_ptr<IFimSocket>(sender)))
      .WillRepeatedly(DoAll(InvokeWithoutArgs(Sleep(0.02)),
                            Return(true)));
  EXPECT_CALL(*sender, ShutdownCalled())
      .WillRepeatedly(Return(false));

  EXPECT_CALL(*tracker, peer_client_num())
      .WillOnce(Return(1));

  proc_->Process(fim, sender);

  EXPECT_CALL(*tracker, peer_client_num())
      .WillOnce(Return(1));

  proc_->Process(fim, sender);

  Sleep(0.1)();
  EXPECT_TRUE(proc_->SocketPending(sender));
  proc_->Start();
  WaitProcessed(sender);
  // Stop processing
  proc_->Stop();
  proc_->Join();

  EXPECT_CALL(*tracker, peer_client_num())
      .WillOnce(Return(kNotClient));

  FIM_PTR<IFim> fim2 = MakeFim(2);
  proc_->Process(fim2, sender);
  Sleep(0.1)();
  EXPECT_TRUE(proc_->SocketPending(sender));
  // Restart processor
  EXPECT_CALL(*worker_, Process(fim2, boost::shared_ptr<IFimSocket>(sender)))
      .WillOnce(Return(true));
  proc_->Start();
  WaitProcessed(sender);
}

TEST_F(ThreadFimProcessorTest, Abnormal) {
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());

  proc_->Join();
  proc_->Start();
  proc_->Start();

  // Missing FIM processing
  FIM_PTR<IFim> fim = MakeFim(1);
  boost::shared_ptr<MockIFimSocket> sender =
      boost::make_shared<MockIFimSocket>();
  boost::scoped_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  EXPECT_CALL(*sender, GetReqTracker())
      .WillRepeatedly(Return(tracker.get()));
  EXPECT_CALL(*tracker, peer_client_num())
      .WillRepeatedly(Return(kNotClient));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Fim),
                   StartsWith("Unprocessed Fim #Q00000-000064(Getattr)"),
                   _));
  EXPECT_CALL(*worker_, Accept(_))
      .WillOnce(Return(true));
  EXPECT_CALL(*worker_, Process(_, _));
  EXPECT_CALL(*sender, ShutdownCalled())
      .WillRepeatedly(Return(false));

  proc_->Process(fim, sender);
  WaitProcessed(sender);
  Mock::VerifyAndClear(worker_);
  Mock::VerifyAndClear(&mock_log_callback);

  // FIM processing errors
  EXPECT_CALL(*worker_, Accept(_))
      .WillOnce(Return(true));
  EXPECT_CALL(*worker_, Process(fim, boost::shared_ptr<IFimSocket>(sender)))
      .WillOnce(Throw(std::runtime_error("Error thrown")));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Fim),
                   StartsWith("Exception processing Fim #Q00000-000064"
                              "(Getattr): "
                              "Error thrown"), _));
  proc_->Process(fim, sender);
  WaitProcessed(sender);
  Mock::VerifyAndClear(worker_);

  // Worker not accepting the Fim
  EXPECT_CALL(*worker_, Accept(_))
      .WillOnce(Return(false));
  proc_->Process(fim, sender);
  WaitProcessed(sender);
  Mock::VerifyAndClear(worker_);
}

TEST_F(ThreadFimProcessorTest, DeadSocket) {
  worker_ = new MockIFimProcessor;
  proc_.reset(kThreadFimProcessorMaker(worker_));
  proc_->Start();

  boost::shared_ptr<MockIFimSocket> sender =
    boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*worker_, Accept(_))
      .WillOnce(Return(true));
  boost::scoped_ptr<MockIReqTracker> tracker(new MockIReqTracker);
  EXPECT_CALL(*sender, GetReqTracker())
      .WillRepeatedly(Return(tracker.get()));
  EXPECT_CALL(*sender, ShutdownCalled())
      .WillOnce(Return(true));
  EXPECT_CALL(*tracker, peer_client_num())
      .WillRepeatedly(Return(kNotClient));

  proc_->Process(UnlinkFim::MakePtr(), sender);
  WaitProcessed(sender);

  Mock::VerifyAndClear(sender.get());
}

TEST_F(ThreadFimProcessorTest, BasicProcessors) {
  worker_ = new MockIFimProcessor;
  proc_.reset(kBasicThreadFimProcessorMaker(worker_));
  proc_->Start();

  boost::shared_ptr<MockIFimSocket> socket =
    boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*worker_, Accept(_))
      .WillOnce(Return(false));

  proc_->Process(UnlinkFim::MakePtr(), socket);
  WaitProcessed(socket);

  proc_->Process(HeartbeatFim::MakePtr(), socket);

  Mock::VerifyAndClear(socket.get());
}

}  // namespace
}  // namespace cpfs
