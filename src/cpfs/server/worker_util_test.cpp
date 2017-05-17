/* Copyright 2013 ClusterTech Ltd */
#include "server/worker_util.hpp"

#include <cerrno>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "fim_socket_mock.hpp"
#include "fims.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StartsWith;

namespace cpfs {
namespace server {
namespace {

class WorkerUtilTest : public ::testing::Test {
 protected:
  FIM_PTR<GetattrFim> req_;
  boost::shared_ptr<MockIFimSocket> sender_;

  WorkerUtilTest() : req_(new GetattrFim), sender_(new MockIFimSocket) {}
};

typedef MockFunction<void(int, FIM_PTR<IFim>)> MockReplierCallback;

class TestReplyOnExit : public BaseWorkerReplier {
 public:
  TestReplyOnExit(FIM_PTR<IFim> req,
                  boost::shared_ptr<IFimSocket> peer,
                  MockReplierCallback* callback)
      : BaseWorkerReplier(req, peer), callback_(callback) {}
  ~TestReplyOnExit() {
    MakeReply();
  }
  void ReplyComplete() {
    callback_->Call(err_no_, reply_);
  }
 private:
  MockReplierCallback* callback_;
};

TEST_F(WorkerUtilTest, NoIndication) {
  MockReplierCallback cb;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(cb, Call(EIO, _))
      .WillOnce(SaveArg<1>(&reply));
  {
    TestReplyOnExit re(req_, sender_, &cb);
  }
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(EIO), rreply->err_no);
}

TEST_F(WorkerUtilTest, SetSuccessResultOnly) {
  MockReplierCallback cb;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(cb, Call(0, _))
      .WillOnce(SaveArg<1>(&reply));
  {
    TestReplyOnExit re(req_, sender_, &cb);
    re.SetResult(1);
  }
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(0), rreply->err_no);
}

TEST_F(WorkerUtilTest, SetFailResult) {
  MockReplierCallback cb;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(cb, Call(2, _))
      .WillOnce(SaveArg<1>(&reply));
  {
    TestReplyOnExit re(req_, sender_, &cb);
    re.SetResult(1);
    re.SetResult(-2);
  }
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(2), rreply->err_no);
}

TEST_F(WorkerUtilTest, SetReplyOnly) {
  MockReplierCallback cb;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(cb, Call(-1, _))
      .WillOnce(SaveArg<1>(&reply));
  {
    TestReplyOnExit re(req_, sender_, &cb);
    re.SetNormalReply(DataReplyFim::MakePtr());
  }
  EXPECT_TRUE(dynamic_cast<DataReplyFim*>(reply.get()));
}

TEST_F(WorkerUtilTest, SetReplyAndSuccessResult) {
  MockReplierCallback cb;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(cb, Call(0, _))
      .WillOnce(SaveArg<1>(&reply));
  {
    TestReplyOnExit re(req_, sender_, &cb);
    re.SetNormalReply(DataReplyFim::MakePtr());
    re.SetResult(0);
  }
  EXPECT_TRUE(dynamic_cast<DataReplyFim*>(reply.get()));
}

TEST_F(WorkerUtilTest, SetReplyAndSuccessFailResult) {
  MockReplierCallback cb;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(cb, Call(2, _))
      .WillOnce(SaveArg<1>(&reply));
  {
    TestReplyOnExit re(req_, sender_, &cb);
    re.SetNormalReply(DataReplyFim::MakePtr());
    re.SetResult(-2);
    re.SetResult(0);
  }
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(2), rreply->err_no);
}

TEST_F(WorkerUtilTest, Throw) {
  MockReplierCallback cb;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(cb, Call(EIO, _))
      .WillOnce(SaveArg<1>(&reply));
  try {
    TestReplyOnExit re(req_, sender_, &cb);
    re.SetResult(1);
    throw std::exception();
  } catch (...) {
  }
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(EIO), rreply->err_no);
}

TEST_F(WorkerUtilTest, ReplyOnExit) {
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*sender_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  {
    req_->set_req_id(42U);
    ReplyOnExit re(req_, sender_);
    re.SetResult(-2);
  }
  ResultCodeReplyFim& rreply = static_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(42U, rreply.req_id());
  EXPECT_EQ(2U, rreply->err_no);
  EXPECT_TRUE(rreply.is_final());
}

TEST_F(WorkerUtilTest, ReplyDisabled) {
  TestReplyOnExit re(req_, sender_, 0);
  re.SetEnabled(false);
}

}  // namespace
}  // namespace server
}  // namespace cpfs
