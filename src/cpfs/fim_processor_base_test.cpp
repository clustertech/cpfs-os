/* Copyright 2013 ClusterTech Ltd */
#include "fim_processor_base.hpp"

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "req_tracker_mock.hpp"
#include "time_keeper_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::Return;
using ::testing::StrEq;

namespace cpfs {
namespace {

TEST(BaseFimProcessorTest, Process) {
  BaseFimProcessor processor;
  MockITimeKeeper time_keeper;
  processor.SetTimeKeeper(&time_keeper);
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  boost::shared_ptr<MockIReqTracker> tracker =
      boost::make_shared<MockIReqTracker>();
  EXPECT_CALL(*fim_socket, GetReqTracker())
      .WillRepeatedly(Return(tracker.get()));

  // TestProcessor processes FIMs
  EXPECT_TRUE(processor.Accept(HeartbeatFim::MakePtr()));
  EXPECT_CALL(time_keeper, Update()).Times(3);
  EXPECT_TRUE(processor.Process(HeartbeatFim::MakePtr(), fim_socket));
  FIM_PTR<IFim> reply = ResultCodeReplyFim::MakePtr();
  EXPECT_CALL(*tracker, AddReply(reply));
  EXPECT_TRUE(processor.Process(reply, fim_socket));
  EXPECT_FALSE(processor.Process(ReadFim::MakePtr(), fim_socket));

  Mock::VerifyAndClear(fim_socket.get());
}

TEST(BaseFimProcessorTest, ProcessError) {
  BaseFimProcessor processor;
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();
  EXPECT_CALL(*fim_socket, GetReqTracker())
      .WillRepeatedly(Return(static_cast<IReqTracker*>(0)));
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Fim),
                   StrEq("Reply #P00000-000001(ResultCodeReply) "
                         "without tracker"),
                   _));

  FIM_PTR<ResultCodeReplyFim> fim = ResultCodeReplyFim::MakePtr();
  fim->set_req_id(1);
  processor.Process(fim, fim_socket);
}

}  // namespace
}  // namespace cpfs
