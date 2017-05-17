/* Copyright 2013 ClusterTech Ltd */
#include "client/cpfs_fso.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>

#include <sys/uio.h>

#include <cstring>
#include <string>
#include <vector>

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "connector_mock.hpp"
#include "ds_iface.hpp"
#include "dsg_state.hpp"
#include "fims.hpp"
#include "fuseobj_mock.hpp"
#include "io_service_runner_mock.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "req_completion_mock.hpp"
#include "req_entry_mock.hpp"
#include "req_limiter_mock.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "client/base_client.hpp"
#include "client/cache_mock.hpp"
#include "client/conn_mgr_mock.hpp"
#include "client/file_handle.hpp"
#include "client/inode_usage_mock.hpp"

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::ByRef;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeArgument;
using ::testing::InvokeWithoutArgs;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnPointee;
using ::testing::ReturnRef;
using ::testing::SaveArg;
using ::testing::SaveArgPointee;
using ::testing::StartsWith;
using ::testing::StrEq;

namespace cpfs {

class IFimSocket;

namespace client {

// Explicit instantiate class template to better detect missing coverage
template class CpfsFuseObj<MockFuseMethodPolicyF>;

namespace {

/**
 * Prepare for checking a call to AddRequest().
 *
 * @param req_fim The location to save the Fim argument used to call
 * AddRequest()
 *
 * @param reply_fim The Fim to reply when the returned MockIReqEntry
 * has the WaitReply() method called.  If empty, do not expect a reply
 *
 * @param alloc_req_id The expected alloc_req_id argument of the call
 *
 * @param callback The location to save the ack callback added to
 * the returning entry.  If 0, do not expect ack callback to be set
 *
 * @return The request entry.  This entry should not be reset before
 * the request tracker is cleared with Mock::VerifyAndClear
 */
boost::shared_ptr<MockIReqEntry> PrepareAddRequest(
    boost::shared_ptr<MockIReqTracker> tracker,
    FIM_PTR<IFim>* req_fim,
    const FIM_PTR<IFim>& reply_fim = FIM_PTR<IFim>(),
    ReqAckCallback* callback = 0) {
  static const FIM_PTR<IFim> empty_ifim;
  boost::shared_ptr<MockIReqEntry> ret(new MockIReqEntry);
  EXPECT_CALL(*tracker, AddRequest(_, _))
      .WillOnce(DoAll(SaveArg<0>(req_fim),
                                 Return(ret)));
  if (callback)
    EXPECT_CALL(*ret, OnAck(_, false))
        .WillOnce(SaveArg<0>(callback));
  if (reply_fim)
    EXPECT_CALL(*ret, WaitReply())
        .WillOnce(ReturnRef(reply_fim ? reply_fim : empty_ifim));
  return ret;
}

class CpfsFsoTest : public ::testing::Test {
 protected:
  // FUSE method mocking
  BaseFSClient client_;
  boost::shared_ptr<MockFuseMethodPolicy> mock_fuse_method_;
  CpfsFuseObj<MockFuseMethodPolicyF> cpfs_fuse_obj_;  // The object tested
  boost::shared_ptr<MockIReqTracker> mock_ms_req_tracker_;
  boost::scoped_ptr<MockIReqLimiter> req_limiter_;
  boost::shared_ptr<MockIReqTracker> mock_ds_req_tracker1_;
  boost::scoped_ptr<MockIReqLimiter> req_limiter1_;
  boost::shared_ptr<MockIReqTracker> mock_ds_req_tracker2_;
  boost::scoped_ptr<MockIReqLimiter> req_limiter2_;
  MockITrackerMapper* mock_tracker_mapper_;
  MockICacheMgr* mock_cache_mgr_;
  MockIConnMgr* mock_conn_mgr_;
  MockIOServiceRunner* mock_io_service_runner_;
  MockIInodeUsageSet* inode_usage_set_;
  MockIReqCompletionCheckerSet* req_completion_checker_set_;
  MockIConnector* connector_;
  struct FakeFuseReq {
    int* ptr;
    uint64_t unique;
  } fake_req_;

  fuse_req_t fuse_req_;
  fuse_ctx ctx_;

  CpfsFsoTest() :
      mock_fuse_method_(boost::make_shared<MockFuseMethodPolicy>()),
      cpfs_fuse_obj_(mock_fuse_method_),
      mock_ms_req_tracker_(new MockIReqTracker),
      req_limiter_(new MockIReqLimiter),
      mock_ds_req_tracker1_(new MockIReqTracker),
      req_limiter1_(new MockIReqLimiter),
      mock_ds_req_tracker2_(new MockIReqTracker),
      req_limiter2_(new MockIReqLimiter),
      fuse_req_(reinterpret_cast<fuse_req_t>(&fake_req_)) {
    fake_req_.unique = 1;
    ctx_.uid = 12345;
    ctx_.gid = 67890;
    cpfs_fuse_obj_.SetClient(&client_);
    cpfs_fuse_obj_.SetUseXattr(false);
    client_.set_tracker_mapper(
        mock_tracker_mapper_ = new MockITrackerMapper);
    client_.set_cache_mgr(mock_cache_mgr_ = new MockICacheMgr);
    Mock::AllowLeak(mock_tracker_mapper_);
    EXPECT_CALL(*mock_tracker_mapper_, GetMSTracker())
        .WillRepeatedly(Return(mock_ms_req_tracker_));
    EXPECT_CALL(*mock_ms_req_tracker_, GetReqLimiter())
        .WillRepeatedly(Return(req_limiter_.get()));
    EXPECT_CALL(*mock_ds_req_tracker1_, GetReqLimiter())
        .WillRepeatedly(Return(req_limiter1_.get()));
    EXPECT_CALL(*mock_ds_req_tracker2_, GetReqLimiter())
        .WillRepeatedly(Return(req_limiter2_.get()));
    Mock::AllowLeak(&*mock_fuse_method_);
    EXPECT_CALL(*mock_fuse_method_, ReqCtx(fuse_req_))
        .WillRepeatedly(Return(&ctx_));
    client_.set_conn_mgr(mock_conn_mgr_ = new MockIConnMgr);
    client_.set_service_runner(
        mock_io_service_runner_ = new MockIOServiceRunner);
    client_.set_inode_usage_set(inode_usage_set_ = new MockIInodeUsageSet);
    client_.set_req_completion_checker_set(
        req_completion_checker_set_ = new MockIReqCompletionCheckerSet);
    client_.set_connector(connector_ = new MockIConnector);
    client_.set_dsg_state(0, kDSGReady, 0);
    client_.set_dsg_state(7, kDSGReady, 0);
  }

  virtual void TearDown() {
    Mock::VerifyAndClear(&*mock_fuse_method_);
    Mock::VerifyAndClear(mock_tracker_mapper_);
  }

  class ErrorTester {
   public:
    boost::shared_ptr<MockIReqTracker> tracker_;
    FIM_PTR<IFim> req_fim_;
    FIM_PTR<IFim> err_fim_;
    boost::shared_ptr<MockIReqEntry> entry_;

    ErrorTester(CpfsFsoTest* test_obj, int err_no,
                boost::shared_ptr<MockIReqTracker> tracker =
                boost::shared_ptr<MockIReqTracker>()) {
      FIM_PTR<ResultCodeReplyFim> err_fim
          = ResultCodeReplyFim::MakePtr();
      err_fim_ = err_fim;
      (*err_fim)->err_no = err_no;
      if (!tracker)
        tracker = test_obj->mock_ms_req_tracker_;
      tracker_ = tracker;
      entry_ = PrepareAddRequest(tracker, &req_fim_, err_fim_);
      EXPECT_CALL(*test_obj->mock_fuse_method_,
                  ReplyErr(test_obj->fuse_req_, err_no));
    }
    ~ErrorTester() {
      Mock::VerifyAndClear(tracker_.get());
    }
  };
};

TEST_F(CpfsFsoTest, FsArgs) {
  int status;
  char buffer[1024];
  char prog[] = "cpfs";

  // Test 1: nothing to parse
  char* argv1[] = { prog, 0 };
  fuse_args args1 = FUSE_ARGS_INIT(1, argv1);
  cpfs_fuse_obj_.ParseFsOpts(&args1);
  fuse_opt_free_args(&args1);

  // Test 2: parse -V
  if (ForkCaptureResult res = ForkCapture(buffer, 1024, &status)) {
    execl("tbuild/cpfs/client/cpfs_fso_test_parse", "cpfs_fso", "-V", NULL);
    exit(1);
  }
  EXPECT_EQ(0, WEXITSTATUS(status));
  EXPECT_THAT(buffer, HasSubstr("CPFS version"));

  // Test 3: parse -h
  if (ForkCaptureResult res = ForkCapture(buffer, 1024, &status)) {
    execl("tbuild/cpfs/client/cpfs_fso_test_parse", "cpfs_fso", "-h", NULL);
    exit(1);
  }
  EXPECT_EQ(0, WEXITSTATUS(status));
  EXPECT_THAT(buffer, HasSubstr("-o meta-server"));

  // Test 4: parse -o meta-server=SERVERS
  char* argv4[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("meta-server=abc\\,d"), 0 };
  fuse_args args4 = FUSE_ARGS_INIT(3, argv4);
  cpfs_fuse_obj_.ParseFsOpts(&args4);
  const std::vector<std::string>& ms = cpfs_fuse_obj_.GetMetaServers();
  EXPECT_THAT(ms, ElementsAre("abc", "d"));
  fuse_opt_free_args(&args4);

  // Test 5: parse -o
  if (ForkCaptureResult res = ForkCapture(buffer, 1024, &status)) {
    execl("tbuild/cpfs/client/cpfs_fso_test_parse", "cpfs_fso", "-o", NULL);
    exit(2);
  }
  EXPECT_EQ(1, WEXITSTATUS(status));
  EXPECT_NE(reinterpret_cast<char*>(0),
            std::strstr(buffer, "missing argument"));

  // Test 6: parse -o log-level=LEVEL
  char* argv6[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("log-level=2"), 0 };
  fuse_args args6 = FUSE_ARGS_INIT(3, argv6);
  cpfs_fuse_obj_.ParseFsOpts(&args6);
  EXPECT_STREQ("2", cpfs_fuse_obj_.GetLogLevel());
  fuse_opt_free_args(&args6);

  // Test 7: parse -o heartbeat-interval=x -o use-xattr=1
  char* argv7[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("heartbeat-interval=5.5"),
                    const_cast<char*>("-o"),
                    const_cast<char*>("disable-xattr=1"), 0 };
  fuse_args args7 = FUSE_ARGS_INIT(5, argv7);
  cpfs_fuse_obj_.ParseFsOpts(&args7);
  fuse_opt_free_args(&args7);

  // Test 8: parse -o socket-read-timeout=x
  char* argv8[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("socket-read-timeout=10"), 0 };
  fuse_args args8 = FUSE_ARGS_INIT(3, argv8);
  cpfs_fuse_obj_.ParseFsOpts(&args8);
  fuse_opt_free_args(&args8);

  // Test 9: parse -o invalid format: heartbeat-interval=char
  char* argv9[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("heartbeat-interval=malformed"), 0 };
  fuse_args args9 = FUSE_ARGS_INIT(3, argv9);
  EXPECT_THROW(cpfs_fuse_obj_.ParseFsOpts(&args9), std::runtime_error);
  fuse_opt_free_args(&args9);

  // Test 10: parse -o invalid format: socket-read-timeout=char
  char* argv10[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("socket-read-timeout=malformed"), 0 };
  fuse_args args10 = FUSE_ARGS_INIT(3, argv10);
  EXPECT_THROW(cpfs_fuse_obj_.ParseFsOpts(&args10), std::runtime_error);
  fuse_opt_free_args(&args10);

  // Test 11: parse -o log-path=x
  char* argv11[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("log-path=/dev/null"), 0 };
  fuse_args args11 = FUSE_ARGS_INIT(3, argv11);
  cpfs_fuse_obj_.ParseFsOpts(&args11);
  fuse_opt_free_args(&args11);

  // Test12: parse -o msg-buf-per-conn
  char* argv12[] = { prog, const_cast<char*>("-o"),
                     const_cast<char*>("msg-buf-per-conn=1024"), 0 };
  fuse_args args12 = FUSE_ARGS_INIT(3, argv12);

  EXPECT_CALL(*mock_tracker_mapper_, SetLimiterMaxSend(1024));

  cpfs_fuse_obj_.ParseFsOpts(&args12);
  fuse_opt_free_args(&args12);

  // Test13: parse -o init-conn-retry
  char* argv13[] = { prog, const_cast<char*>("-o"),
                     const_cast<char*>("init-conn-retry=10"), 0 };
  fuse_args args13 = FUSE_ARGS_INIT(3, argv13);

  EXPECT_CALL(*mock_conn_mgr_, SetInitConnRetry(10));

  cpfs_fuse_obj_.ParseFsOpts(&args13);
  fuse_opt_free_args(&args13);

  // Impossible code paths
  EXPECT_EQ(1, CpfsOptProc(0, 0, -1, 0));
}

TEST_F(CpfsFsoTest, Init) {
  // Init CacheMgr and ConnMgr
  // IOService is launch
  char prog[] = "cpfs";
  char* argv1[] = { prog, const_cast<char*>("-o"),
                    const_cast<char*>("meta-server=192.168.1.5:3000"), 0 };
  fuse_args args1 = FUSE_ARGS_INIT(3, argv1);
  cpfs_fuse_obj_.ParseFsOpts(&args1);
  fuse_opt_free_args(&args1);
  EXPECT_CALL(*mock_cache_mgr_, Init());
  EXPECT_CALL(*mock_cache_mgr_, SetFuseChannel(0));
  EXPECT_CALL(*connector_, set_heartbeat_interval(_));
  EXPECT_CALL(*connector_, set_socket_read_timeout(_));
  EXPECT_CALL(*mock_conn_mgr_, Init(_));
  EXPECT_CALL(*mock_io_service_runner_, Run());
  // In real usage, the fuse_chan is obtained by fuse_mount()
  cpfs_fuse_obj_.SetFuseChannel(0);
  cpfs_fuse_obj_.Init(0);
}

TEST_F(CpfsFsoTest, GetattrSuccess) {
  // Expectation of the Getattr call
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(1, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  ReqAckCallback callback;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  (*reply_fim)->dirty = false;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  struct stat stbuf;
  EXPECT_CALL(*mock_fuse_method_, ReplyAttr(fuse_req_, _, 3600.0))
      .WillOnce(DoAll(SaveArgPointee<1>(&stbuf),
                      Return(0)));

  // Actual call
  cpfs_fuse_obj_.Getattr(fuse_req_, 1, 0);

  // Verify arguments to mocked calls
  GetattrFim& rfim = dynamic_cast<GetattrFim&>(*req_fim);
  EXPECT_EQ(1U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(1U, stbuf.st_ino);
  EXPECT_EQ(1234U, stbuf.st_mode);
  EXPECT_EQ(34567U, stbuf.st_gid);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, GetattrError) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(1, _))
      .WillOnce(InvokeArgument<1>());
  ErrorTester et(this, ENOENT);
  cpfs_fuse_obj_.Getattr(fuse_req_, 1, 0);
}

TEST_F(CpfsFsoTest, GetattrStrange) {
  // Expectation of the Getattr call
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(1, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  FIM_PTR<GetattrFim> strange_fim = GetattrFim::MakePtr();
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, strange_fim);

  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EIO));

  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, FS),
                   StartsWith("FUSE 1: Unexpected reply"), _));

  // Actual call
  cpfs_fuse_obj_.Getattr(fuse_req_, 1, 0);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, SetattrSuccess) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = S_IFDIR | 01666;
  (*reply_fim)->dirty = false;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  struct stat stbuf;
  EXPECT_CALL(*mock_fuse_method_, ReplyAttr(fuse_req_, _, 3600.0))
      .WillOnce(DoAll(SaveArgPointee<1>(&stbuf),
                      Return(0)));
  EXPECT_CALL(*mock_cache_mgr_, InvalidateInode(3, true));

  // Actual call
  struct stat attr;
  attr.st_mode = 01775;
  cpfs_fuse_obj_.Setattr(fuse_req_, 3, &attr, FUSE_SET_ATTR_MODE, 0);

  // Verify arguments to mocked calls
  SetattrFim& rfim = dynamic_cast<SetattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(01775U, rfim->fa.mode);
  EXPECT_EQ(unsigned(FUSE_SET_ATTR_MODE), rfim->fa_mask);
  EXPECT_EQ(3U, stbuf.st_ino);
  EXPECT_EQ(S_IFDIR | 01666u, stbuf.st_mode);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, SetattrError) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  ErrorTester et(this, EPERM);
  struct stat attr;
  cpfs_fuse_obj_.Setattr(fuse_req_, 3, &attr, 0, 0);
}

TEST_F(CpfsFsoTest, SetxattrSuccess) {
  FIM_PTR<IFim> req_fim;
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));
  FIM_PTR<ResultCodeReplyFim> reply_fim = ResultCodeReplyFim::MakePtr();
  (*reply_fim)->err_no = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  // Real call
  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Setxattr(fuse_req_, 3, "user.CPFS.ndsg", "10", 3, 0);

  // Verify arguments to mocked calls
  SetxattrFim& rfim = dynamic_cast<SetxattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ("user.CPFS.ndsg", std::string(rfim.tail_buf(), 0, rfim->name_len));
  EXPECT_EQ("10",
      std::string(rfim.tail_buf() + rfim->name_len, 0, rfim->value_len));
  EXPECT_EQ(0, rfim->flags);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, SetxattrError) {
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EOPNOTSUPP));

  cpfs_fuse_obj_.Setxattr(fuse_req_, 3, "user.CPFS.ndsg", "10", 2, 0);

  ErrorTester et(this, EPERM);

  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Setxattr(fuse_req_, 3, "user.CPFS.ndsg", "10", 2, 0);
}

TEST_F(CpfsFsoTest, SetxattrSecurity) {
  EXPECT_CALL(*mock_fuse_method_,
              ReplyErr(fuse_req_, EOPNOTSUPP));
  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Setxattr(fuse_req_, 3, "security.selinux", "10", 2, 0);
}

TEST_F(CpfsFsoTest, GetxattrLengthSuccess) {
  FIM_PTR<IFim> req_fim;
  EXPECT_CALL(*mock_fuse_method_, ReplyXattr(fuse_req_, 200));
  FIM_PTR<GetxattrReplyFim> reply_fim = GetxattrReplyFim::MakePtr();
  (*reply_fim)->value_len = 200;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  // Read the attribute length only
  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Getxattr(fuse_req_, 3, "user.CPFS.ndsg", 0);

  // Verify arguments to mocked calls
  GetxattrFim& rfim = dynamic_cast<GetxattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(std::string("user.CPFS.ndsg"), rfim.tail_buf());
  EXPECT_EQ(0U, rfim->value_len);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, GetxattrValueSuccess) {
  FIM_PTR<IFim> req_fim;
  EXPECT_CALL(*mock_fuse_method_, ReplyBuf(fuse_req_, _, 3));
  FIM_PTR<GetxattrReplyFim> reply_fim = GetxattrReplyFim::MakePtr();
  (*reply_fim)->value_len = 3;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  // Read the attribute value
  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Getxattr(fuse_req_, 3, "user.CPFS.ndsg", 256);

  // Verify arguments to mocked calls
  GetxattrFim& rfim = dynamic_cast<GetxattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(std::string("user.CPFS.ndsg"), rfim.tail_buf());
  EXPECT_EQ(256U, rfim->value_len);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, GetxattrError) {
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EOPNOTSUPP));

  cpfs_fuse_obj_.Getxattr(fuse_req_, 3, "user.CPFS.ndsg", 2);

  ErrorTester et(this, EPERM);

  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Getxattr(fuse_req_, 3, "user.CPFS.ndsg", 2);
}

TEST_F(CpfsFsoTest, GetxattrSecurity) {
  EXPECT_CALL(*mock_fuse_method_,
              ReplyErr(fuse_req_, EOPNOTSUPP));
  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Getxattr(fuse_req_, 3, "security.selinux", 2);
}

TEST_F(CpfsFsoTest, ListxattrLengthSuccess) {
  FIM_PTR<IFim> req_fim;
  EXPECT_CALL(*mock_fuse_method_, ReplyXattr(fuse_req_, 200));
  FIM_PTR<ListxattrReplyFim> reply_fim = ListxattrReplyFim::MakePtr();
  (*reply_fim)->size = 200;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  // Read the attributes length only
  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Listxattr(fuse_req_, 3, 0);

  // Verify arguments to mocked calls
  ListxattrFim& rfim = dynamic_cast<ListxattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(0U, rfim->size);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, ListxattrValueSuccess) {
  FIM_PTR<IFim> req_fim;
  EXPECT_CALL(*mock_fuse_method_, ReplyBuf(fuse_req_, _, 2));
  FIM_PTR<ListxattrReplyFim> reply_fim = ListxattrReplyFim::MakePtr();
  (*reply_fim)->size = 2;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  // Read the attributes value
  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Listxattr(fuse_req_, 3, 2);

  // Verify arguments to mocked calls
  ListxattrFim& rfim = dynamic_cast<ListxattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(2U, rfim->size);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, ListxattrError) {
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EOPNOTSUPP));

  cpfs_fuse_obj_.Listxattr(fuse_req_, 3, 2);

  ErrorTester et(this, EPERM);

  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Listxattr(fuse_req_, 3, 2);
}

TEST_F(CpfsFsoTest, Removexattr) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> reply_fim = ResultCodeReplyFim::MakePtr();
  (*reply_fim)->err_no = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));

  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Removexattr(fuse_req_, 3, "user.CPFS.ndsg");
  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, RemovexattrBlock) {
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EOPNOTSUPP));

  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Removexattr(fuse_req_, 3, "security.selinux");
  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, RemovexattrError) {
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EOPNOTSUPP));

  cpfs_fuse_obj_.Removexattr(fuse_req_, 3, "user.CPFS.ndsg");

  ErrorTester et(this, EPERM);

  cpfs_fuse_obj_.SetUseXattr(true);
  cpfs_fuse_obj_.Removexattr(fuse_req_, 3, "user.CPFS.ndsg");
}

TEST_F(CpfsFsoTest, OpenSuccess) {
  // Expectation of the Open call
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim
      = DataReplyFim::MakePtr(2 * sizeof(GroupId));
  GroupId* grp_ptr = reinterpret_cast<GroupId*>(reply_fim->tail_buf());
  grp_ptr[0] = 4;
  grp_ptr[1] = 2;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  fuse_file_info fi;
  fi.flags = O_RDWR;
  EXPECT_CALL(*mock_fuse_method_, ReplyOpen(fuse_req_, &fi));
  EXPECT_CALL(*inode_usage_set_, UpdateOpened(3, true, _));

  // Actual call
  cpfs_fuse_obj_.Open(fuse_req_, 3, &fi);

  // Verify arguments to mocked calls
  OpenFim& rfim = dynamic_cast<OpenFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(O_RDWR, rfim->flags);

  // Cleanup
  DeleteFH(fi.fh);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, OpenError) {
  InSequence dummy;

  EXPECT_CALL(*inode_usage_set_, UpdateOpened(3, false, _));
  FIM_PTR<ResultCodeReplyFim> err_fim = ResultCodeReplyFim::MakePtr();
  (*err_fim)->err_no = EACCES;
  FIM_PTR<IFim> err_ifim = err_fim;
  FIM_PTR<IFim> req_fim1;
  boost::shared_ptr<MockIReqEntry> entry1 =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim1, err_ifim);
  EXPECT_CALL(*inode_usage_set_, UpdateClosed(3, false, _, _))
      .WillOnce(Return(1));
  FIM_PTR<IFim> req_fim2;
  boost::shared_ptr<MockIReqEntry> entry2 =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim2);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EACCES));

  struct fuse_file_info fi;
  fi.flags = O_RDONLY;
  cpfs_fuse_obj_.Open(fuse_req_, 3, &fi);
  EXPECT_EQ(kReleaseFim, req_fim2->type());
  ReleaseFim& r_req = dynamic_cast<ReleaseFim&>(*req_fim2);
  EXPECT_EQ(1, r_req->keep_read);

  // InSequence, need to clear all mocks to avoid deadlock
  Mock::VerifyAndClear(entry1.get());
  Mock::VerifyAndClear(entry2.get());
  Mock::VerifyAndClear(inode_usage_set_);
  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
  Mock::VerifyAndClear(&*mock_fuse_method_);
}

TEST_F(CpfsFsoTest, Access) {
  // Expectation of the Open call
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> reply_fim
      = ResultCodeReplyFim::MakePtr();
  (*reply_fim)->err_no = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));

  cpfs_fuse_obj_.Access(fuse_req_, 3, W_OK);
  AccessFim& rfim = dynamic_cast<AccessFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(W_OK, rfim->mask);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, CreateSuccess) {
  // Expectation of the Create call
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr(sizeof(GroupId));
  FIM_PTR<IFim> reply_ifim = reply_fim;
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  (*reply_fim)->inode = 5;
  GroupId group = 42;
  std::memcpy(reply_fim->tail_buf(), &group, sizeof(group));
  ReqAckCallback callback;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim, &callback);
  fuse_entry_param entry_param;
  fuse_file_info fi;
  fi.flags = O_RDONLY;
  EXPECT_CALL(*mock_fuse_method_, ReplyCreate(fuse_req_, _, &fi))
      .WillOnce(DoAll(
          SaveArgPointee<1>(&entry_param),
          Return(0)));
  EXPECT_CALL(*mock_cache_mgr_, AddEntry(1, "hello", false));
  EXPECT_CALL(*inode_usage_set_, UpdateOpened(5, false, _));

  // Actual call
  cpfs_fuse_obj_.Create(fuse_req_, 1, "hello", 0755, &fi);
  // Verify arguments to mocked calls
  CreateFim& rfim = dynamic_cast<CreateFim&>(*req_fim);
  EXPECT_EQ(1U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(5U, FHFileCoordManager(fi.fh)->inode());
  EXPECT_EQ(5U, entry_param.ino);
  EXPECT_EQ(1234U, entry_param.attr.st_mode);
  EXPECT_EQ(34567U, entry_param.attr.st_gid);

  EXPECT_CALL(*entry, request())
      .WillOnce(ReturnPointee(&req_fim));
  EXPECT_CALL(*(entry), reply())
      .WillOnce(ReturnRef(reply_ifim));

  // Callback sets the new inode number and group list back to request
  // asynchronously
  callback(entry);

  EXPECT_EQ(5U, rfim->req.new_inode);
  EXPECT_EQ(6 + sizeof(GroupId), rfim.tail_buf_size());
  EXPECT_EQ(1U, rfim->num_ds_groups);

  // Cleanup
  DeleteFH(fi.fh);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, CreateError) {
  // ErrorTester is not used so that we can test callback behavior
  ReqAckCallback callback;
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> err_fim(new ResultCodeReplyFim);
  (*err_fim)->err_no = EACCES;
  FIM_PTR<IFim> err_ifim = err_fim;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, err_fim, &callback);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EACCES));
  EXPECT_CALL(*entry, reply())
      .WillOnce(ReturnRef(err_ifim));

  struct fuse_file_info fi;
  cpfs_fuse_obj_.Create(fuse_req_, 1, "hello", 0755, &fi);
  callback(entry);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, CreateStrange) {
  // Expectation of the Create call
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> err_fim = ResultCodeReplyFim::MakePtr();
  (*err_fim)->err_no = 0;
  ReqAckCallback callback;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, err_fim, &callback);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EIO));

  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, FS),
                   StartsWith("FUSE 1: Unexpected zero error "), _));

  // Actual call
  struct fuse_file_info fi;
  cpfs_fuse_obj_.Create(fuse_req_, 1, "hello", 0755, &fi);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, Flush) {
  struct fuse_file_info fi;
  fi.flags = O_RDWR;
  fi.fh = MakeFH(3, 0, 0);
  FHSetErrno(fi.fh, ENOSPC);
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, ENOSPC));

  cpfs_fuse_obj_.Flush(fuse_req_, 3, &fi);
  DeleteFH(fi.fh);
}

TEST_F(CpfsFsoTest, Release) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  struct fuse_file_info fi[2];
  fi[0].flags = O_RDONLY;
  fi[0].fh = MakeFH(3, 0, 0);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));
  EXPECT_CALL(*inode_usage_set_, UpdateClosed(3, false, _, _))
      .WillOnce(Return(kClientAccessUnchanged));

  cpfs_fuse_obj_.Release(fuse_req_, 3, &fi[0]);
}

TEST_F(CpfsFsoTest, Fsync) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));

  cpfs_fuse_obj_.Fsync(fuse_req_, 3, 0, 0);
}

TEST_F(CpfsFsoTest, Link) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  (*reply_fim)->fa.nlink = 4;
  (*reply_fim)->dirty = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  fuse_entry_param entry_param;
  EXPECT_CALL(*mock_fuse_method_, ReplyEntry(fuse_req_, _))
      .WillOnce(DoAll(
          SaveArgPointee<1>(&entry_param),
          Return(0)));
  EXPECT_CALL(*mock_cache_mgr_, AddEntry(3, "hello", false));
  EXPECT_CALL(*mock_cache_mgr_, InvalidateInode(2, false));

  cpfs_fuse_obj_.Link(fuse_req_, 2, 3, "hello");
  EXPECT_EQ(2U, entry_param.ino);
  EXPECT_EQ(1234U, entry_param.attr.st_mode);
  EXPECT_EQ(34567U, entry_param.attr.st_gid);
  EXPECT_EQ(4U, entry_param.attr.st_nlink);
  EXPECT_EQ(3600.0, entry_param.attr_timeout);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, LinkError) {
  ErrorTester et(this, EACCES);
  cpfs_fuse_obj_.Link(fuse_req_, 2, 3, "hello");
}

TEST_F(CpfsFsoTest, Unlink) {
  // Expectation of the Create call
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> err_fim = ResultCodeReplyFim::MakePtr();
  (*err_fim)->err_no = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, err_fim);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));
  EXPECT_CALL(*mock_cache_mgr_, InvalidateInode(3, true));

  // Actual call
  cpfs_fuse_obj_.Unlink(fuse_req_, 3, "hello");

  // Verify arguments to mocked calls
  UnlinkFim& rfim = dynamic_cast<UnlinkFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, Rmdir) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> reply_fim
      = ResultCodeReplyFim::MakePtr();
  (*reply_fim)->err_no = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));

  cpfs_fuse_obj_.Rmdir(fuse_req_, 2, "foo");
  RmdirFim& rfim = dynamic_cast<RmdirFim&>(*req_fim);
  EXPECT_EQ(2U, rfim->parent);
  EXPECT_EQ(4U, rfim.tail_buf_size());
  EXPECT_EQ(0, std::memcmp(rfim.tail_buf(), "foo", 4));

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, Rename) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> reply_fim
      = ResultCodeReplyFim::MakePtr();
  (*reply_fim)->err_no = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, 0));

  cpfs_fuse_obj_.Rename(fuse_req_, 2, "foo", 3, "hello");
  RenameFim& rfim = dynamic_cast<RenameFim&>(*req_fim);
  EXPECT_EQ(2U, rfim->parent);
  EXPECT_EQ(3U, rfim->new_parent);
  EXPECT_EQ(10U, rfim.tail_buf_size());
  EXPECT_EQ(0, std::memcmp(rfim.tail_buf(), "foo\0hello", 10));

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, LookupSuccess) {
  // Expectation of the Lookup call
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->inode = 5;
  (*reply_fim)->dirty = 0;
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  fuse_entry_param entry_param;
  EXPECT_CALL(*mock_fuse_method_, ReplyEntry(fuse_req_, _))
      .WillOnce(DoAll(
          SaveArgPointee<1>(&entry_param),
          Return(0)));

  boost::shared_ptr<MockICacheInvRecord> inv_rec =
      boost::make_shared<MockICacheInvRecord>();
  EXPECT_CALL(*mock_cache_mgr_, StartLookup())
      .WillOnce(Return(inv_rec));
  MUTEX_DECL(mutex);
  EXPECT_CALL(*inv_rec, GetMutex())
      .WillOnce(Return(&mutex));
  EXPECT_CALL(*inv_rec, InodeInvalidated(1, true))
      .WillOnce(Return(false));
  EXPECT_CALL(*inv_rec, InodeInvalidated(5, false))
      .WillOnce(Return(false));
  EXPECT_CALL(*mock_cache_mgr_, AddEntry(1, "hello", true));

  // Actual call
  cpfs_fuse_obj_.Lookup(fuse_req_, 1, "hello");

  // Verify arguments to mocked calls
  LookupFim& rfim = dynamic_cast<LookupFim&>(*req_fim);
  EXPECT_EQ(1U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);

  EXPECT_EQ(5U, entry_param.ino);
  EXPECT_EQ(1234U, entry_param.attr.st_mode);
  EXPECT_EQ(34567U, entry_param.attr.st_gid);
  EXPECT_EQ(3600.0, entry_param.attr_timeout);

  Mock::VerifyAndClear(mock_cache_mgr_);
  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, LookupError) {
  boost::shared_ptr<MockICacheInvRecord> inv_rec =
      boost::make_shared<MockICacheInvRecord>();
  EXPECT_CALL(*mock_cache_mgr_, StartLookup())
      .WillOnce(Return(inv_rec));
  ErrorTester et(this, EACCES);
  cpfs_fuse_obj_.Lookup(fuse_req_, 1, "hello");
  Mock::VerifyAndClear(mock_cache_mgr_);
}

TEST_F(CpfsFsoTest, ReaddirEmpty) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim(new DataReplyFim);
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  EXPECT_CALL(*mock_fuse_method_, ReplyBuf(fuse_req_, _, 0));

  // Actual call
  fuse_file_info fi;
  cpfs_fuse_obj_.Readdir(fuse_req_, 1, 1024, 0, &fi);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, ReaddirNormal) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim(
      new DataReplyFim(sizeof(ReaddirRecord) * 2));
  ReaddirRecord* rec = reinterpret_cast<ReaddirRecord*>(reply_fim->tail_buf());
  rec[0].inode = 5;
  rec[0].cookie = 12345;
  rec[0].name_len = 1;
  rec[0].file_type = 3;
  strncpy(rec[0].name, "a", 2);
  rec[1].inode = 5;
  rec[1].cookie = 67890;
  rec[1].name_len = 1;
  strncpy(rec[1].name, "b", 2);
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  const char* pbuf1;
  const char* pbuf2;
  struct stat stbuf;

  EXPECT_CALL(*mock_fuse_method_, AddDirentry(fuse_req_, _, 21,
                                              rec[0].name, _, 12345))
      .WillOnce(DoAll(SaveArg<1>(&pbuf1),
                      SaveArgPointee<4>(&stbuf),
                      Return(20)));
  EXPECT_CALL(*mock_fuse_method_, AddDirentry(fuse_req_, _, 1,
                                              rec[1].name, _, 67890))
      .WillOnce(Return(20));

  EXPECT_CALL(*mock_fuse_method_, ReplyBuf(fuse_req_, _, 20))
      .WillOnce(DoAll(SaveArg<1>(&pbuf2),
                      Return(0)));

  // Actual call
  fuse_file_info fi;
  cpfs_fuse_obj_.Readdir(fuse_req_, 1, 21, 67890, &fi);

  // Verify arguments to mocked calls
  ReaddirFim& rfim = dynamic_cast<ReaddirFim&>(*req_fim);
  EXPECT_EQ(1U, rfim->inode);
  EXPECT_EQ(21U, rfim->size);
  EXPECT_EQ(67890, rfim->cookie);

  EXPECT_EQ(5U, stbuf.st_ino);
  EXPECT_EQ(3U << 12, stbuf.st_mode);
  EXPECT_EQ(pbuf1, pbuf2);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, ReaddirError1) {
  ErrorTester et(this, EACCES);
  fuse_file_info fi;
  cpfs_fuse_obj_.Readdir(fuse_req_, 1, 1024, 0, &fi);
}

TEST_F(CpfsFsoTest, ReaddirError2) {  // Due to insufficient space
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim(
      new DataReplyFim(sizeof(ReaddirRecord)));
  ReaddirRecord& rec = reinterpret_cast<ReaddirRecord&>(*reply_fim->tail_buf());
  rec.inode = 5;
  rec.cookie = 12345;
  rec.name_len = 1;
  strncpy(rec.name, "a", 2);
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  EXPECT_CALL(*mock_fuse_method_, AddDirentry(fuse_req_, _, 1,
                                              rec.name, _, 12345))
      .WillOnce(Return(20));

  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, EINVAL));

  // Actual call
  fuse_file_info fi;
  cpfs_fuse_obj_.Readdir(fuse_req_, 1, 1, 0, &fi);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, ReaddirException1) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim(new DataReplyFim(1));
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  // Actual call
  fuse_file_info fi;
  EXPECT_THROW(cpfs_fuse_obj_.Readdir(fuse_req_, 1, 1024, 0, &fi),
               std::runtime_error);
  // Trick GCC not to free reply_fim before
  EXPECT_TRUE(reply_fim);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, ReaddirException2) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim(
      new DataReplyFim(sizeof(ReaddirRecord)));
  ReaddirRecord& rec = reinterpret_cast<ReaddirRecord&>(*reply_fim->tail_buf());
  rec.inode = 5;
  rec.name_len = 6;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  // Actual call
  fuse_file_info fi;
  EXPECT_THROW(cpfs_fuse_obj_.Readdir(fuse_req_, 1, 1024, 0, &fi),
               std::runtime_error);
  // Trick GCC not to free reply_fim before
  EXPECT_TRUE(reply_fim);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

void Increment(int* val) {
  ++*val;
}

void AckCallback(bool* completed_ret) {
  *completed_ret = true;
}

TEST_F(CpfsFsoTest, Write) {
  boost::shared_ptr<MockIReqCompletionChecker> checker(
      new MockIReqCompletionChecker);
  EXPECT_CALL(*req_completion_checker_set_, Get(5))
      .WillOnce(Return(checker));
  EXPECT_CALL(*inode_usage_set_, SetDirty(5));
  InodeNum inode = kNumDSPerGroup;
  EXPECT_CALL(*mock_tracker_mapper_, GetDSTracker(7, 1))
      .WillOnce(Return(mock_ds_req_tracker1_));
  boost::shared_ptr<IReqEntry> entry;
  EXPECT_CALL(*req_limiter1_, Send(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry),
                      Return(true)));
  int num_cb_calls = 0;
  EXPECT_CALL(*req_completion_checker_set_,
              GetReqAckCallback(5, boost::shared_ptr<IFimSocket>()))
      .Times(1).WillRepeatedly(Return(boost::bind(&Increment, &num_cb_calls)));
  EXPECT_CALL(*checker, RegisterReq(_))
      .Times(1);
  EXPECT_CALL(*mock_fuse_method_, ReplyWrite(fuse_req_, 1024));

  // Actual call
  fuse_file_info fi;
  GroupId groups[] = { 7 };
  fi.fh = MakeFH(inode, groups, 1);
  char buf[1024];
  for (unsigned i = 0; i < 1024; ++i)
    buf[i] = char(i / 8);
  cpfs_fuse_obj_.Write(fuse_req_, inode, buf, 1024, 32768, &fi);

  WriteFim& rfim = dynamic_cast<WriteFim&>(*entry->request());
  EXPECT_EQ(0, std::memcmp(rfim.tail_buf(), buf, 1024));
  EXPECT_EQ(1024U, rfim.tail_buf_size());
  EXPECT_EQ(32768U, rfim->dsg_off);
  EXPECT_EQ(32768U + 1024U, rfim->last_off);

  // Check callback calling
  EXPECT_EQ(0, num_cb_calls);
  FIM_PTR<ResultCodeReplyFim> reply = ResultCodeReplyFim::MakePtr();
  (*reply)->err_no = ENOSPC;
  reply->set_final();
  entry->SetReply(reply, 1);
  Sleep(0.01)();
  EXPECT_EQ(1, num_cb_calls);
  EXPECT_EQ(ENOSPC, FHGetErrno(fi.fh, false));

  // Cleanup
  DeleteFH(fi.fh);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
  Mock::VerifyAndClear(mock_ds_req_tracker1_.get());
  Mock::VerifyAndClear(req_completion_checker_set_);
}

TEST_F(CpfsFsoTest, WriteDeferredErr) {
  EXPECT_CALL(*mock_fuse_method_, ReplyErr(fuse_req_, ENOSPC));

  InodeNum inode = 42;
  fuse_file_info fi;
  GroupId groups[] = { 7 };
  fi.fh = MakeFH(inode, groups, 1);
  FHSetErrno(fi.fh, ENOSPC);
  char buf[1024];
  for (unsigned i = 0; i < 1024; ++i)
    buf[i] = char(i / 8);
  cpfs_fuse_obj_.Write(fuse_req_, inode, buf, 1024, 32768, &fi);

  // Cleanup
  DeleteFH(fi.fh);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
  Mock::VerifyAndClear(mock_ds_req_tracker1_.get());
  Mock::VerifyAndClear(req_completion_checker_set_);
}

void CopyIov(struct iovec iov[], unsigned len, std::vector<std::string>* ret) {
  for (unsigned i = 0; i < len; ++i)
    ret->push_back(std::string(reinterpret_cast<char*>(iov[i].iov_base),
                               iov[i].iov_len));
}

TEST_F(CpfsFsoTest, Read) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim(new DataReplyFim(1024));
  InodeNum inode = kNumDSPerGroup;
  char* buf = reply_fim->tail_buf();
  for (unsigned i = 0; i < 1024; ++i)
    buf[i] = char(i / 8);
  EXPECT_CALL(*mock_tracker_mapper_, GetDSTracker(7, 1))
      .WillOnce(Return(mock_ds_req_tracker1_));
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ds_req_tracker1_, &req_fim, reply_fim);
  struct iovec iov[1];
  std::vector<std::string> data;
  EXPECT_CALL(*mock_fuse_method_, ReplyIov(fuse_req_, _, 1))
      .WillOnce(DoAll(SaveArgArray<1>(iov, 1),
                      InvokeWithoutArgs(boost::bind(CopyIov, iov, 1, &data)),
                      Return(0)));

  // Actual call
  fuse_file_info fi;
  GroupId groups[] = { 7 };
  fi.fh = MakeFH(inode, groups, 1);
  cpfs_fuse_obj_.Read(fuse_req_, inode, 1024, 32768, &fi);
  DeleteFH(fi.fh);

  // Verify arguments to mocked calls
  ReadFim& rfim = dynamic_cast<ReadFim&>(*req_fim);
  EXPECT_EQ(32768U, rfim->dsg_off);
  EXPECT_EQ(1024U, rfim->size);
  EXPECT_EQ(0, memcmp(buf, data[0].data(), 1024));
  EXPECT_EQ(4U, rfim->checksum_role);

  Mock::VerifyAndClear(mock_ds_req_tracker1_.get());
}

TEST_F(CpfsFsoTest, ReadError) {
  ErrorTester et(this, EACCES, mock_ds_req_tracker1_);
  fuse_file_info fi;
  GroupId groups[] = { 7 };
  fi.fh = MakeFH(3, groups, 1);
  EXPECT_CALL(*mock_tracker_mapper_, GetDSTracker(7, _))
      .WillOnce(Return(mock_ds_req_tracker1_));
  cpfs_fuse_obj_.Read(fuse_req_, 3, 1024, 32768, &fi);
  DeleteFH(fi.fh);
}

TEST_F(CpfsFsoTest, MkdirSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = 041755;
  (*reply_fim)->fa.nlink = 2U;
  (*reply_fim)->fa.uid = 20U;
  (*reply_fim)->fa.gid = 30U;
  (*reply_fim)->fa.size = 4096U;
  (*reply_fim)->inode = 5U;

  ReqAckCallback callback;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim, &callback);

  fuse_entry_param entry_param;
  EXPECT_CALL(*mock_fuse_method_, ReplyEntry(fuse_req_, _))
      .WillOnce(DoAll(
          SaveArgPointee<1>(&entry_param),
          Return(0)));
  EXPECT_CALL(*mock_cache_mgr_, AddEntry(1, "testd1", false));

  cpfs_fuse_obj_.Mkdir(fuse_req_, 1, "testd1", 0755);
  MkdirFim& rreq_fim = static_cast<MkdirFim&>(*req_fim);
  EXPECT_EQ(1U, rreq_fim->parent);
  EXPECT_EQ(0755U, rreq_fim->req.mode);
  EXPECT_EQ(5U, entry_param.ino);
  EXPECT_TRUE(S_ISDIR(entry_param.attr.st_mode));

  EXPECT_CALL(*entry, request())
      .WillOnce(ReturnPointee(&req_fim));
  FIM_PTR<IFim> reply_ifim = reply_fim;
  EXPECT_CALL(*(entry), reply())
      .WillOnce(ReturnRef(reply_ifim));

  // Callback sets the new inode number back to request asynchronously
  callback(entry);

  EXPECT_EQ(5U, rreq_fim->req.new_inode);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, MkdirError) {
  ErrorTester et(this, ENOENT, mock_ms_req_tracker_);
  EXPECT_CALL(*et.entry_, OnAck(_, false));

  cpfs_fuse_obj_.Mkdir(fuse_req_, 0, "testd1", 0755);
}

TEST_F(CpfsFsoTest, SymlinkSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = 041755;
  (*reply_fim)->fa.nlink = 2U;
  (*reply_fim)->fa.uid = 20U;
  (*reply_fim)->fa.gid = 30U;
  (*reply_fim)->fa.size = 4096U;
  (*reply_fim)->inode = 5U;

  ReqAckCallback callback;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim, &callback);

  fuse_entry_param entry_param;
  EXPECT_CALL(*mock_fuse_method_, ReplyEntry(fuse_req_, _))
      .WillOnce(DoAll(
          SaveArgPointee<1>(&entry_param),
          Return(0)));
  EXPECT_CALL(*mock_cache_mgr_, AddEntry(1, "test", false));

  cpfs_fuse_obj_.Symlink(fuse_req_, "target", 1, "test");

  SymlinkFim& rreq_fim = static_cast<SymlinkFim&>(*req_fim);
  EXPECT_EQ(1U, rreq_fim->inode);
  EXPECT_EQ(4U, rreq_fim->name_len);
  EXPECT_EQ(12U, rreq_fim.tail_buf_size());
  EXPECT_EQ(0, std::memcmp(rreq_fim.tail_buf(), "test\0target", 12));
  EXPECT_EQ(5U, entry_param.ino);

  EXPECT_CALL(*entry, request())
      .WillOnce(ReturnPointee(&req_fim));
  FIM_PTR<IFim> reply_ifim = reply_fim;
  EXPECT_CALL(*(entry), reply())
      .WillOnce(ReturnRef(reply_ifim));

  // Callback sets the new inode number back to request asynchronously
  callback(entry);

  EXPECT_EQ(5U, rreq_fim->new_inode);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, SymlinkError) {
  ErrorTester et(this, ENOENT, mock_ms_req_tracker_);
  EXPECT_CALL(*et.entry_, OnAck(_, false));

  cpfs_fuse_obj_.Symlink(fuse_req_, "target", 0, "testd1");
}

TEST_F(CpfsFsoTest, ReadlinkSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim(new DataReplyFim(7));
  std::memcpy(reply_fim->tail_buf(), "target", 7);

  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  EXPECT_CALL(*mock_fuse_method_, ReplyReadlink(fuse_req_, StrEq("target")));
  cpfs_fuse_obj_.Readlink(fuse_req_, 5);

  ReadlinkFim& rreq_fim = static_cast<ReadlinkFim&>(*req_fim);
  EXPECT_EQ(5U, rreq_fim->inode);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, ReadlinkError) {
  ErrorTester et(this, ENOENT, mock_ms_req_tracker_);
  cpfs_fuse_obj_.Readlink(fuse_req_, 5);
}

TEST_F(CpfsFsoTest, MknodSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = 041755;
  (*reply_fim)->fa.nlink = 2U;
  (*reply_fim)->fa.uid = 20U;
  (*reply_fim)->fa.gid = 30U;
  (*reply_fim)->fa.size = 4096U;
  (*reply_fim)->inode = 5U;

  ReqAckCallback callback;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim, &callback);

  fuse_entry_param entry_param;
  EXPECT_CALL(*mock_fuse_method_, ReplyEntry(fuse_req_, _))
      .WillOnce(DoAll(
          SaveArgPointee<1>(&entry_param),
          Return(0)));
  EXPECT_CALL(*mock_cache_mgr_, AddEntry(1, "test", false));

  cpfs_fuse_obj_.Mknod(fuse_req_, 1, "test", 2, 3);

  MknodFim& rreq_fim = static_cast<MknodFim&>(*req_fim);
  EXPECT_EQ(1U, rreq_fim->inode);
  EXPECT_EQ(2U, rreq_fim->mode);
  EXPECT_EQ(3U, rreq_fim->rdev);
  EXPECT_EQ(5U, rreq_fim.tail_buf_size());
  EXPECT_EQ(0, std::memcmp(rreq_fim.tail_buf(), "test", 5));
  EXPECT_EQ(5U, entry_param.ino);

  EXPECT_CALL(*entry, request())
      .WillOnce(ReturnPointee(&req_fim));
  FIM_PTR<IFim> reply_ifim = reply_fim;
  EXPECT_CALL(*(entry), reply())
      .WillOnce(ReturnRef(reply_ifim));

  // Callback sets the new inode number back to request asynchronously
  callback(entry);

  EXPECT_EQ(5U, rreq_fim->new_inode);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, MknodError) {
  ErrorTester et(this, ENOENT, mock_ms_req_tracker_);
  EXPECT_CALL(*et.entry_, OnAck(_, false));

  cpfs_fuse_obj_.Mknod(fuse_req_, 0, "testd1", 2, 3);
}

TEST_F(CpfsFsoTest, OpendirSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<ResultCodeReplyFim> reply_fim
      = ResultCodeReplyFim::MakePtr();
  (*reply_fim)->err_no = 0;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  fuse_file_info fi;
  EXPECT_CALL(*mock_fuse_method_, ReplyOpen(fuse_req_, &fi));

  // Actual call
  cpfs_fuse_obj_.Opendir(fuse_req_, 3, &fi);

  // Verify arguments to mocked calls
  OpendirFim& rfim = dynamic_cast<OpendirFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, OpendirError) {
  ErrorTester et(this, EACCES);
  fuse_file_info fi;
  cpfs_fuse_obj_.Opendir(fuse_req_, 3, &fi);
}

TEST_F(CpfsFsoTest, StatfsSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<FCStatFSReplyFim> reply_fim = FCStatFSReplyFim::MakePtr();
  (*reply_fim)->total_space = 100;
  (*reply_fim)->free_space = 50;
  (*reply_fim)->total_inodes = 10000;
  (*reply_fim)->free_inodes = 8000;
  boost::shared_ptr<MockIReqEntry> entry =
      PrepareAddRequest(mock_ms_req_tracker_, &req_fim, reply_fim);

  EXPECT_CALL(*mock_fuse_method_, ReplyStatfs(fuse_req_, _));

  // Actual call
  cpfs_fuse_obj_.Statfs(fuse_req_, 1);

  Mock::VerifyAndClear(mock_ms_req_tracker_.get());
}

TEST_F(CpfsFsoTest, StatfsError) {
  ErrorTester et(this, EACCES);

  cpfs_fuse_obj_.Statfs(fuse_req_, 1);
}

}  // namespace
}  // namespace client
}  // namespace cpfs
