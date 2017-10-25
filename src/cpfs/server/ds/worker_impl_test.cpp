/* Copyright 2013 ClusterTech Ltd */

#include "server/ds/worker_impl.hpp"

#include <stdint.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_set.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "common.hpp"
#include "config_mgr.hpp"
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "req_completion_mock.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"
#include "tracer_impl.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ccache_tracker_mock.hpp"
#include "server/ds/base_ds.hpp"
#include "server/ds/cleaner_mock.hpp"
#include "server/ds/degrade_mock.hpp"
#include "server/ds/store_mock.hpp"
#include "server/worker.hpp"

using ::testing::_;
using ::testing::ByRef;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Eq;
using ::testing::IgnoreResult;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Ref;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StartsWith;
using ::testing::StrEq;
using ::testing::Throw;
using ::testing::Truly;

namespace cpfs {
namespace server {
namespace ds {
namespace {

typedef MockFunction<void(const boost::shared_ptr<IReqEntry>&)>
MockAckCallback;

template <typename TVal>
bool EqWhenRun(TVal p1, TVal* p2) {
  return p1 == *p2;
}

class DSWorkerTest : public ::testing::Test {
 protected:
  BaseDataServer data_server_;
  boost::scoped_ptr<IWorker> worker_;
  MockIFimProcessor queuer_;
  MockIStore* store_;
  MockICleaner* cleaner_;
  MockITrackerMapper* tracker_mapper_;
  MockICCacheTracker* cache_tracker_;
  MockIReqCompletionCheckerSet* req_completion_checker_set_;
  MockIDataRecoveryMgr* rec_mgr_;
  MockIDegradedCache* degraded_cache_;

  boost::shared_ptr<MockIReqCompletionChecker> req_completion_checker_;
  boost::shared_ptr<MockIReqTracker> c_tracker_;  // For client
  boost::shared_ptr<MockIReqTracker> m_tracker_;  // For MS
  boost::shared_ptr<MockIReqTracker> d_tracker_;  // For DS
  boost::shared_ptr<MockIFimSocket> c_fim_socket_;  // For client
  boost::shared_ptr<MockIFimSocket> m_fim_socket_;  // For MS
  boost::shared_ptr<MockIFimSocket> d_fim_socket_;  // For DS

  MockFunction<void(InodeNum, bool)> inv_func_;

  DSWorkerTest()
      : data_server_(ConfigMgr()),
        worker_(MakeWorker()),
        req_completion_checker_(new MockIReqCompletionChecker),
        c_tracker_(new MockIReqTracker),
        m_tracker_(new MockIReqTracker),
        d_tracker_(new MockIReqTracker),
        c_fim_socket_(new MockIFimSocket),
        m_fim_socket_(new MockIFimSocket),
        d_fim_socket_(new MockIFimSocket) {
    worker_->SetQueuer(&queuer_);
    data_server_.set_cleaner(cleaner_ = new MockICleaner);
    data_server_.set_store(store_ = new MockIStore);
    data_server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    data_server_.set_cache_tracker(cache_tracker_ = new MockICCacheTracker);
    data_server_.set_data_recovery_mgr(rec_mgr_ = new MockIDataRecoveryMgr);
    data_server_.set_degraded_cache(degraded_cache_ = new MockIDegradedCache);
    data_server_.set_req_completion_checker_set(
        req_completion_checker_set_ = new MockIReqCompletionCheckerSet);
    data_server_.set_tracer(MakeSmallTracer());
    worker_->set_server(&data_server_);
    worker_->SetCacheInvalFunc(
        boost::bind(GetMockCall(inv_func_), &inv_func_, _1, _2));
    EXPECT_CALL(*tracker_mapper_, GetDSTracker(4, 2))
        .WillRepeatedly(Return(d_tracker_));
    EXPECT_CALL(*c_fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(c_tracker_.get()));
    EXPECT_CALL(*c_tracker_, peer_client_num())
        .WillRepeatedly(Return(7));
    EXPECT_CALL(*m_fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(m_tracker_.get()));
    EXPECT_CALL(*m_tracker_, peer_client_num())
        .WillRepeatedly(Return(kNotClient));
    EXPECT_CALL(*d_fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(d_tracker_.get()));
    EXPECT_CALL(*d_tracker_, peer_client_num())
        .WillRepeatedly(Return(kNotClient));
    EXPECT_CALL(*store_, ds_group())
        .WillRepeatedly(Return(4));
  }

  ~DSWorkerTest() {
    Mock::VerifyAndClear(m_fim_socket_.get());
    Mock::VerifyAndClear(c_fim_socket_.get());
    Mock::VerifyAndClear(tracker_mapper_);
    Mock::VerifyAndClear(req_completion_checker_set_);
  }

  void PrepareCalls(GroupRole my_role, InodeNum inode) {
    (void) inode;
    EXPECT_CALL(*store_, ds_role())
        .WillRepeatedly(Return(my_role));
  }

  void PrepareInvalidation(InodeNum inode, ClientNum client) {
    EXPECT_CALL(inv_func_, Call(inode, true));
    EXPECT_CALL(*cache_tracker_, InvGetClients(inode, client, _));
    if (client != kNotClient)
      EXPECT_CALL(*cache_tracker_, SetCache(inode, client));
  }

  static FIM_PTR<WriteFim> MakeWriteFim(InodeNum inode, uint64_t dsg_off,
                                        uint64_t last_off, GroupRole cs_role,
                                        const char* msg) {
    FIM_PTR<WriteFim> fim(new WriteFim(std::strlen(msg) + 1));
    (*fim)->inode = inode;
    (*fim)->dsg_off = dsg_off;
    (*fim)->last_off = last_off;
    (*fim)->target_role = 0;
    (*fim)->checksum_role = cs_role;
    std::memcpy(fim->tail_buf(), msg, std::strlen(msg) + 1);
    return fim;
  }

  void PrepareWriteRepl(InodeNum inode,
                        boost::shared_ptr<IReqEntry>* req_entry,
                        MockAckCallback* ack_callback,
                        boost::shared_ptr<IFimSocket> peer) {
    EXPECT_CALL(*req_completion_checker_set_, Get(inode))
        .WillOnce(Return(req_completion_checker_));
    EXPECT_CALL(*d_tracker_, AddRequestEntry(_, _))
        .WillOnce(DoAll(SaveArg<0>(req_entry),
                        Return(true)));
    EXPECT_CALL(*req_completion_checker_set_, GetReqAckCallback(inode, peer))
        .WillOnce(Return(
            boost::bind(&MockAckCallback::Call, ack_callback, _1)));
    EXPECT_CALL(*req_completion_checker_,
                RegisterReq(Truly(boost::bind(
                    &EqWhenRun<boost::shared_ptr<IReqEntry> >,
                    _1, req_entry))));
  }

  void DistressTestPrepareProcess(int msg_len,
                                  boost::shared_ptr<IReqEntry>* req_entry_ret,
                                  MockAckCallback* ack_callback);

  void DistressTestCUComplete(boost::shared_ptr<IReqEntry> req_entry,
                              MockAckCallback* ack_callback);
};

TEST_F(DSWorkerTest, WorkerDefer) {
  // Set recovering state
  data_server_.set_dsg_state(2, kDSGRecovering, 2);

  // Client initiated Fims processing will be suspended
  FIM_PTR<ReadFim> fim(new ReadFim);
  (*fim)->inode = 132;
  (*fim)->dsg_off = 100;
  (*fim)->size = 8;
  (*fim)->checksum_role = 2;

  worker_->Process(fim, c_fim_socket_);

  // DS-initiated Fims processing will continue as usual
  FIM_PTR<ChecksumUpdateFim> cs_fim(new ChecksumUpdateFim(6));
  (*cs_fim)->inode = 132;
  (*cs_fim)->dsg_off = 100;
  (*cs_fim)->last_off = 1000;
  std::memcpy(cs_fim->tail_buf(), "abcde", 6);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 0, false))
      .WillOnce(Return(boost::shared_ptr<IDataRecoveryHandle>()));
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, ApplyDelta(132, Ref((*cs_fim)->optime), 1000, 100,
                                  cs_fim->tail_buf(), 6, true));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*d_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  worker_->Process(cs_fim, d_fim_socket_);

  // Defer reset Fims when DSG state not yet changed
  FIM_PTR<DeferResetFim> reset_fim = DeferResetFim::MakePtr();
  worker_->Process(reset_fim, m_fim_socket_);

  // Ready again, but still queueing Fims
  data_server_.set_dsg_state(3, kDSGReady, 0);
  FIM_PTR<OpenFim> o_fim(new OpenFim);
  (*o_fim)->inode = 132;
  worker_->Process(o_fim, c_fim_socket_);

  // Defer reset Fims after DSG state changed
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, Read(132, false, 100, _, 8))
      .WillOnce(Return(0));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*cache_tracker_, SetCache(132, 7));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Fim),
                   StartsWith("Unprocessed Fim "),
                   _));

  worker_->Process(reset_fim, m_fim_socket_);
}

TEST_F(DSWorkerTest, WorkerDeferInode) {
  // Set recovering state
  data_server_.set_dsg_state(2, kDSGResync, 2);
  std::vector<InodeNum> resyncing;
  resyncing.push_back(132);
  data_server_.set_dsg_inodes_resyncing(resyncing);

  // Client initiated Fims processing of that inode will be suspended
  FIM_PTR<ReadFim> fim(new ReadFim);
  (*fim)->inode = 132;
  (*fim)->dsg_off = 100;
  (*fim)->size = 8;
  (*fim)->checksum_role = 2;

  worker_->Process(fim, c_fim_socket_);

  // Client initiated Fims processing of other inode is not affected
  PrepareCalls(1, 133);
  EXPECT_CALL(*store_, Read(133, false, 100, _, 8))
      .WillOnce(Return(0));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*cache_tracker_, SetCache(133, 7));

  FIM_PTR<ReadFim> fim2(new ReadFim);
  (*fim2)->inode = 133;
  (*fim2)->dsg_off = 100;
  (*fim2)->size = 8;
  (*fim2)->checksum_role = 2;

  worker_->Process(fim2, c_fim_socket_);

  // DS-initiated Fims processing will continue as usual
  FIM_PTR<ChecksumUpdateFim> cs_fim(new ChecksumUpdateFim(6));
  (*cs_fim)->inode = 132;
  (*cs_fim)->dsg_off = 100;
  (*cs_fim)->last_off = 1000;
  std::memcpy(cs_fim->tail_buf(), "abcde", 6);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 0, false))
      .WillOnce(Return(boost::shared_ptr<IDataRecoveryHandle>()));
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, ApplyDelta(132, Ref((*cs_fim)->optime), 1000, 100,
                                  cs_fim->tail_buf(), 6, true));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*d_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  worker_->Process(cs_fim, d_fim_socket_);

  // Old defer reset Fims gets ignored
  FIM_PTR<DeferResetFim> reset_fim = DeferResetFim::MakePtr();
  worker_->Process(reset_fim, m_fim_socket_);

  // Ready again, but still queueing Fims
  data_server_.set_dsg_state(3, kDSGReady, 0);
  FIM_PTR<OpenFim> o_fim(new OpenFim);
  (*o_fim)->inode = 132;
  worker_->Process(o_fim, c_fim_socket_);

  // New defer reset Fims causes unqueue
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, Read(132, false, 100, _, 8))
      .WillOnce(Return(0));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_));
  EXPECT_CALL(*cache_tracker_, SetCache(132, 7));
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(warning, Fim),
                   StartsWith("Unprocessed Fim "),
                   _));

  worker_->Process(reset_fim, m_fim_socket_);
}

TEST_F(DSWorkerTest, WriteFim) {
  FIM_PTR<WriteFim> fim = MakeWriteFim(132, 100, 1000, 2, "abcde");
  PrepareCalls(1, 132);
  FSTime& optime = (*fim)->optime;
  void* ptr;
  EXPECT_CALL(*store_, Write(132, Ref(optime), 1000,
                             100, fim->tail_buf(), 6, _))
      .WillOnce(DoAll(SaveArg<6>(&ptr),
                      Return(100)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  PrepareInvalidation(132, 7);
  boost::shared_ptr<IReqEntry> req_entry;
  MockAckCallback ack_callback;
  PrepareWriteRepl(132, &req_entry, &ack_callback, c_fim_socket_);

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(fim, c_fim_socket_);
  EXPECT_FALSE(reply->is_final());
  ChecksumUpdateFim& rcs_req =
      dynamic_cast<ChecksumUpdateFim&>(*req_entry->request());
  EXPECT_EQ(132U, rcs_req->inode);
  EXPECT_EQ(100U, rcs_req->dsg_off);
  EXPECT_EQ(6U, rcs_req.tail_buf_size());
  EXPECT_EQ(ptr, rcs_req.tail_buf());

  // On reply
  EXPECT_CALL(ack_callback, Call(req_entry));

  FIM_PTR<ResultCodeReplyFim> cs_reply = ResultCodeReplyFim::MakePtr();
  (*cs_reply)->err_no = 0;
  req_entry->SetReply(cs_reply, 42);
}

TEST_F(DSWorkerTest, WriteFimError) {
  FIM_PTR<WriteFim> fim = MakeWriteFim(132, 100, 1000, 2, "abcde");
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, Write(132, _, 1000, 100, fim->tail_buf(), 6, _))
      .WillOnce(Return(-EIO));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(fim, c_fim_socket_);
  EXPECT_TRUE(reply->is_final());
  ResultCodeReplyFim& rep = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(EIO, int(rep->err_no));
}

TEST_F(DSWorkerTest, WriteFimSkipChecksum) {
  FIM_PTR<WriteFim> fim = MakeWriteFim(132, 100, 1000, 2, "abcde");
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, Write(132, _, 1000, 100, fim->tail_buf(), 6, _))
      .WillOnce(Return(100));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  PrepareInvalidation(132, 7);

  data_server_.set_dsg_state(2, kDSGDegraded, 2);
  worker_->Process(fim, c_fim_socket_);
  EXPECT_TRUE(reply->is_final());
}

TEST_F(DSWorkerTest, WriteFimFailChecksumSending) {
  data_server_.set_dsg_state(1, kDSGReady, 0);

  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, Write(132, _, 1000, 100, _, 6, _))
      .WillOnce(Return(100));
  FIM_PTR<IFim> second_reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(DoDefault())
      .WillOnce(SaveArg<0>(&second_reply));
  PrepareInvalidation(132, 7);
  EXPECT_CALL(*req_completion_checker_set_, Get(132))
      .WillOnce(Return(req_completion_checker_));
  EXPECT_CALL(*d_tracker_, AddRequestEntry(_, _))
      .WillOnce(Return(false));

  FIM_PTR<WriteFim> fim = MakeWriteFim(132, 100, 1000, 2, "abcde");
  fim->set_req_id(42);
  worker_->Process(fim, c_fim_socket_);
  FinalReplyFim& r_reply = static_cast<FinalReplyFim&>(*second_reply);
  EXPECT_TRUE(r_reply.is_final());
  EXPECT_EQ(42U, r_reply.req_id());
}

TEST_F(DSWorkerTest, WriteFimDegradeDefer) {
  FIM_PTR<WriteFim> fim = MakeWriteFim(132, kSegmentSize + 100,
                                       10000, 2, "abcde");
  PrepareCalls(2, 132);

  // Degraded, not yet initialized and recovery in progress, will defer
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(false));
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 0, true))
      .WillRepeatedly(Return(rec_handle));
  EXPECT_CALL(*rec_handle, SetStarted())
      .WillOnce(Return(true));
  EXPECT_CALL(*rec_handle, QueueFim(
      boost::static_pointer_cast<IFim>(fim),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)));

  data_server_.set_dsg_state(2, kDSGDegraded, 0);
  worker_->Process(fim, c_fim_socket_);

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, WriteFimDegradeWrite) {
  FIM_PTR<WriteFim> fim = MakeWriteFim(132, kSegmentSize + 100,
                                       kSegmentSize + 1000, 2, "abcde");
  PrepareCalls(2, 132);
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(true));
  char* wcbuf;
  EXPECT_CALL(*cache_handle, Write(100, _, 6, _))
      .WillOnce(SaveArg<3>(&wcbuf));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  PrepareInvalidation(132, 7);
  EXPECT_CALL(*store_, ApplyDelta(132, _, kSegmentSize + 1000,
                                  100, Eq(ByRef(wcbuf)), 6, true))
      .WillOnce(Return(0));

  data_server_.set_dsg_state(2, kDSGResync, 0);
  boost::unordered_set<InodeNum> to_resync;
  to_resync.insert(132);
  data_server_.set_dsg_inodes_to_resync(&to_resync);
  worker_->Process(fim, c_fim_socket_);
  EXPECT_TRUE(reply->is_final());
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, WriteFimDegradeWriteWithErr) {
  FIM_PTR<WriteFim> fim = MakeWriteFim(132, kSegmentSize + 100,
                                       kSegmentSize + 1000, 2, "abcde");
  PrepareCalls(2, 132);
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(true));
  char* wcbuf;
  EXPECT_CALL(*cache_handle, Write(100, _, 6, _))
      .WillOnce(SaveArg<3>(&wcbuf));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  EXPECT_CALL(*store_, ApplyDelta(132, _, kSegmentSize + 1000,
                                  100, Eq(ByRef(wcbuf)), 6, true))
      .WillOnce(Return(-ENOSPC));
  EXPECT_CALL(*cache_handle, RevertWrite(100, Eq(ByRef(wcbuf)), 6));

  data_server_.set_dsg_state(2, kDSGDegraded, 0);
  worker_->Process(fim, c_fim_socket_);
  EXPECT_TRUE(reply->is_final());
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(ENOSPC), rreply->err_no);

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, WriteFimChecksumENOSPC) {
  PrepareCalls(1, 132);
  char change[] = "\x01\x02\x03\x04\x05";
  EXPECT_CALL(*store_, Write(132, _, 1000, 100, _, 6, _))
      .WillOnce(DoAll(
          IgnoreResult(Invoke(boost::bind(&std::memcpy, _7, change, 6))),
          Return(100)));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_));
  PrepareInvalidation(132, 7);
  boost::shared_ptr<IReqEntry> req_entry;
  MockAckCallback ack_callback;
  PrepareWriteRepl(132, &req_entry, &ack_callback, c_fim_socket_);

  data_server_.set_dsg_state(1, kDSGReady, 0);
  FIM_PTR<WriteFim> fim = MakeWriteFim(132, 100, 1000, 2, "abcde");
  worker_->Process(fim, c_fim_socket_);

  // On ENOSPC reply, send a Fim to the thread group
  FIM_PTR<IFim> revert_fim;
  EXPECT_CALL(queuer_, Process(_, boost::shared_ptr<IFimSocket>()))
      .WillOnce(DoAll(SaveArg<0>(&revert_fim),
                      Return(true)));

  FIM_PTR<ResultCodeReplyFim> cs_reply = ResultCodeReplyFim::MakePtr();
  (*cs_reply)->err_no = ENOSPC;
  req_entry->SetReply(cs_reply, 42);

  // The Fim get processed: revert by checksum
  char cs_updated[7] = {'\0'};
  EXPECT_CALL(*store_, ApplyDelta(132, _, 1000, 100, _, 6, false))
      .WillOnce(DoAll(
          IgnoreResult(Invoke(boost::bind(&std::memcpy, cs_updated, _5, 6))),
          Return(6)));
  EXPECT_CALL(ack_callback, Call(req_entry));

  worker_->Process(revert_fim, boost::shared_ptr<IFimSocket>());
  EXPECT_STREQ(change, cs_updated);

  // Incorrect message won't blow it up
  worker_->Process(revert_fim, boost::shared_ptr<IFimSocket>());
}

void DSWorkerTest::DistressTestPrepareProcess(
    int msg_len,
    boost::shared_ptr<IReqEntry>* req_entry_ret,
    MockAckCallback* ack_callback) {
  EXPECT_CALL(*store_, Write(132, _, 1000, 100, _, msg_len, _))
      .WillOnce(Return(msg_len));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_));
  PrepareInvalidation(132, 7);
  PrepareWriteRepl(132, req_entry_ret, ack_callback, c_fim_socket_);
}

void DSWorkerTest::DistressTestCUComplete(
    boost::shared_ptr<IReqEntry> req_entry,
    MockAckCallback* ack_callback) {
  FIM_PTR<IFim> cfim;
  EXPECT_CALL(queuer_, Process(_, boost::shared_ptr<IFimSocket>()))
      .WillOnce(DoAll(SaveArg<0>(&cfim),
                      Return(true)));
  EXPECT_CALL(*ack_callback, Call(req_entry));

  FIM_PTR<ResultCodeReplyFim> cs_reply = ResultCodeReplyFim::MakePtr();
  (*cs_reply)->err_no = 0;
  req_entry->SetReply(cs_reply, 42);
  worker_->Process(cfim, boost::shared_ptr<IFimSocket>());
}

TEST_F(DSWorkerTest, DistressedReadBasic) {
  // Set distressed mode
  FIM_PTR<DSGDistressModeChangeFim> dfim = DSGDistressModeChangeFim::MakePtr();
  (*dfim)->distress = 1;

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(dfim, c_fim_socket_);
  PrepareCalls(1, 132);

  // Handling of the first write is done immediately
  boost::shared_ptr<IReqEntry> req_entry1;
  MockAckCallback ack_callback1;
  DistressTestPrepareProcess(1, &req_entry1, &ack_callback1);

  worker_->Process(MakeWriteFim(132, 100, 1000, 2, ""), c_fim_socket_);

  // Handling of read now needs to wait
  FIM_PTR<ReadFim> rfim(new ReadFim);
  (*rfim)->inode = 132;
  (*rfim)->dsg_off = 100;
  (*rfim)->size = 1;
  (*rfim)->checksum_role = 2;

  worker_->Process(rfim, c_fim_socket_);

  // On completion of write, will perform the read
  EXPECT_CALL(*cache_tracker_, SetCache(132, 7));
  EXPECT_CALL(*store_, Read(132, false, 100, _, 1))
      .WillOnce(Return(0));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_));

  DistressTestCUComplete(req_entry1, &ack_callback1);
}

TEST_F(DSWorkerTest, DistressedWriteBasic) {
  // Set distressed mode
  FIM_PTR<DSGDistressModeChangeFim> dfim = DSGDistressModeChangeFim::MakePtr();
  (*dfim)->distress = 1;

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(dfim, c_fim_socket_);
  PrepareCalls(1, 132);

  // Handling of the first write is done immediately
  boost::shared_ptr<IReqEntry> req_entry1;
  MockAckCallback ack_callback1;
  DistressTestPrepareProcess(1, &req_entry1, &ack_callback1);

  worker_->Process(MakeWriteFim(132, 100, 1000, 2, ""), c_fim_socket_);

  // Handling of the second and third write is deferred
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "a"), c_fim_socket_);
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "ab"), c_fim_socket_);

  // On reply and notification, trigger next Fim processing
  boost::shared_ptr<IReqEntry> req_entry2;
  MockAckCallback ack_callback2;
  DistressTestPrepareProcess(2, &req_entry2, &ack_callback2);

  DistressTestCUComplete(req_entry1, &ack_callback1);

  // On reply and notification, trigger next Fim processing
  boost::shared_ptr<IReqEntry> req_entry3;
  MockAckCallback ack_callback3;
  DistressTestPrepareProcess(3, &req_entry3, &ack_callback3);

  DistressTestCUComplete(req_entry2, &ack_callback2);

  // On reply and notification, allow immediate send again
  DistressTestCUComplete(req_entry3, &ack_callback3);

  boost::shared_ptr<IReqEntry> req_entry4;
  MockAckCallback ack_callback4;
  DistressTestPrepareProcess(4, &req_entry4, &ack_callback4);

  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "abc"), c_fim_socket_);

  // Set distress off, the next Fim can be sent without waiting as well
  (*dfim)->distress = 0;
  worker_->Process(dfim, c_fim_socket_);

  boost::shared_ptr<IReqEntry> req_entry5;
  MockAckCallback ack_callback5;
  DistressTestPrepareProcess(5, &req_entry5, &ack_callback5);

  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "abcd"), c_fim_socket_);
}

TEST_F(DSWorkerTest, DistressedWriteEndClearance) {
  // Set distressed mode
  FIM_PTR<DSGDistressModeChangeFim> dfim = DSGDistressModeChangeFim::MakePtr();
  (*dfim)->distress = 1;

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(dfim, c_fim_socket_);
  PrepareCalls(1, 132);

  // Handling of the first write is done immediately
  boost::shared_ptr<IReqEntry> req_entry1;
  MockAckCallback ack_callback1;
  DistressTestPrepareProcess(1, &req_entry1, &ack_callback1);

  worker_->Process(MakeWriteFim(132, 100, 1000, 2, ""), c_fim_socket_);

  // Handling of the second write is deferred
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "a"), c_fim_socket_);

  // Clear completely the queue upon switch to non-distress
  boost::shared_ptr<IReqEntry> req_entry2;
  MockAckCallback ack_callback2;
  DistressTestPrepareProcess(2, &req_entry2, &ack_callback2);

  (*dfim)->distress = 0;
  worker_->Process(dfim, c_fim_socket_);
}

TEST_F(DSWorkerTest, DistressedWriteWithErr) {
  // Set distressed mode
  FIM_PTR<DSGDistressModeChangeFim> dfim = DSGDistressModeChangeFim::MakePtr();
  (*dfim)->distress = 1;

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(dfim, c_fim_socket_);
  PrepareCalls(1, 132);

  // Handling of the first write is done immediately
  boost::shared_ptr<IReqEntry> req_entry1;
  MockAckCallback ack_callback1;
  DistressTestPrepareProcess(1, &req_entry1, &ack_callback1);

  worker_->Process(MakeWriteFim(132, 100, 1000, 2, ""), c_fim_socket_);

  // Handling of the second and third write is deferred
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "a"), c_fim_socket_);
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "ab"), c_fim_socket_);

  // Clear completely the queue upon completion as all leads to error
  EXPECT_CALL(*store_, Write(132, _, 1000, 100, _, 2, _))
      .WillOnce(Return(-ENOSPC));
  EXPECT_CALL(*store_, Write(132, _, 1000, 100, _, 3, _))
      .WillOnce(Return(-ENOSPC));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .Times(2);

  DistressTestCUComplete(req_entry1, &ack_callback1);
}

TEST_F(DSWorkerTest, DistressedWriteWithRevert) {
  // Set distressed mode
  FIM_PTR<DSGDistressModeChangeFim> dfim = DSGDistressModeChangeFim::MakePtr();
  (*dfim)->distress = 1;

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(dfim, c_fim_socket_);
  PrepareCalls(1, 132);

  // Handling of the first write is done immediately
  boost::shared_ptr<IReqEntry> req_entry1;
  MockAckCallback ack_callback1;
  DistressTestPrepareProcess(1, &req_entry1, &ack_callback1);

  worker_->Process(MakeWriteFim(132, 100, 1000, 2, ""), c_fim_socket_);

  // Handling of the second write is deferred
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "a"), c_fim_socket_);

  // Trigger reply with error
  FIM_PTR<IFim> cfim;
  EXPECT_CALL(queuer_, Process(_, boost::shared_ptr<IFimSocket>()))
      .WillOnce(DoAll(SaveArg<0>(&cfim),
                      Return(true)));
  EXPECT_CALL(ack_callback1, Call(req_entry1));

  FIM_PTR<ResultCodeReplyFim> cs_reply = ResultCodeReplyFim::MakePtr();
  (*cs_reply)->err_no = ENOSPC;
  req_entry1->SetReply(cs_reply, 42);

  // Revert and arrange the next Fim processing
  EXPECT_CALL(*store_, ApplyDelta(132, _, 1000, 100, _, 1, false));
  boost::shared_ptr<IReqEntry> req_entry2;
  MockAckCallback ack_callback2;
  DistressTestPrepareProcess(2, &req_entry2, &ack_callback2);

  worker_->Process(cfim, boost::shared_ptr<IFimSocket>());
}

TEST_F(DSWorkerTest, DistressedWriteInodeLockUnlock) {
  // Set distressed mode
  FIM_PTR<DSGDistressModeChangeFim> dfim = DSGDistressModeChangeFim::MakePtr();
  (*dfim)->distress = 1;
  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(dfim, c_fim_socket_);
  PrepareCalls(0, 132);

  // Handling of the first write is done immediately
  boost::shared_ptr<IReqEntry> req_entry1;
  MockAckCallback ack_callback1;
  DistressTestPrepareProcess(1, &req_entry1, &ack_callback1);
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, ""), c_fim_socket_);
  // Handling of the second write is deferred
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "a"), c_fim_socket_);

  // On reply and notification, trigger next Fim processing
  boost::shared_ptr<IReqEntry> req_entry2;
  MockAckCallback ack_callback2;
  DistressTestPrepareProcess(2, &req_entry2, &ack_callback2);
  DistressTestCUComplete(req_entry1, &ack_callback1);

  // Set recovering state, defer all is set
  data_server_.set_dsg_state(1, kDSGRecovering, 2);
  // Handling of the third write is deferred
  worker_->Process(MakeWriteFim(132, 100, 1000, 2, "ab"), c_fim_socket_);

  // Got replication reply while DS recovery in progress
  FIM_PTR<IFim> repl_fim;
  EXPECT_CALL(queuer_, Process(_, _))
      .WillOnce(DoAll(SaveArg<0>(&repl_fim),
                      Return(true)));
  EXPECT_CALL(ack_callback2, Call(req_entry2));
  FIM_PTR<ResultCodeReplyFim> cs_reply = ResultCodeReplyFim::MakePtr();
  (*cs_reply)->err_no = 0;
  // Trigger WriteReplCallback
  req_entry2->SetReply(cs_reply, 42);
  // Trigger Distressed callback, processing of WriteFim is suspended
  worker_->Process(repl_fim, m_fim_socket_);

  // Lock received
  FIM_PTR<DSInodeLockFim> fim_lock(new DSInodeLockFim);
  (*fim_lock)->inode = 132;
  (*fim_lock)->lock = 1;
  EXPECT_CALL(*req_completion_checker_set_, Get(132))
      .WillOnce(Return(req_completion_checker_));
  ReqCompletionCallback req_completion_callback;
  EXPECT_CALL(*req_completion_checker_, OnCompleteAll(_))
      .WillOnce(SaveArg<0>(&req_completion_callback));
  worker_->Process(fim_lock, m_fim_socket_);

  // Unlock received
  FIM_PTR<DSInodeLockFim> fim_unlock(new DSInodeLockFim);
  (*fim_unlock)->inode = 132;
  (*fim_unlock)->lock = 0;

  // Unlock Reply
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_));
  worker_->Process(fim_unlock, m_fim_socket_);

  // Completing DS recovery
  boost::shared_ptr<IReqEntry> req_entry3;
  MockAckCallback ack_callback3;
  DistressTestPrepareProcess(3, &req_entry3, &ack_callback3);

  data_server_.set_dsg_state(1, kDSGReady, 0);
  FIM_PTR<DeferResetFim> reset_fim = DeferResetFim::MakePtr();
  worker_->Process(reset_fim, m_fim_socket_);
}

TEST_F(DSWorkerTest, ReadFim) {
  FIM_PTR<ReadFim> fim(new ReadFim);
  (*fim)->inode = 132;
  (*fim)->dsg_off = 100;
  (*fim)->size = 8;
  (*fim)->checksum_role = 2;
  PrepareCalls(1, 132);
  void* buf;
  EXPECT_CALL(*store_, Read(132, false, 100, _, 8))
      .WillOnce(DoAll(SaveArg<3>(&buf),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  EXPECT_CALL(*cache_tracker_, SetCache(132, 7));

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(fim, c_fim_socket_);
  EXPECT_EQ(reply->tail_buf(), buf);
  EXPECT_TRUE(reply->is_final());
}

TEST_F(DSWorkerTest, ReadFimDegradeDefer) {
  // Setup
  FIM_PTR<ReadFim> fim(new ReadFim);
  fim->set_req_id(42);
  (*fim)->inode = 132;
  (*fim)->dsg_off = kSegmentSize + 100;
  (*fim)->size = 8;
  (*fim)->target_role = 0;
  (*fim)->checksum_role = 1;
  PrepareCalls(1, 132);

  // Not degraded
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  data_server_.set_dsg_state(1, kDSGReady, 0);
  worker_->Process(fim, c_fim_socket_);
  NotDegradedFim& redirect_fim = static_cast<NotDegradedFim&>(*reply);
  EXPECT_EQ(42U, redirect_fim->redirect_req);

  // Degraded, not yet initialized and recovery in progress, will defer
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(false));
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 0, true))
      .WillRepeatedly(Return(rec_handle));
  EXPECT_CALL(*rec_handle, SetStarted())
      .WillOnce(Return(true));
  EXPECT_CALL(*rec_handle, QueueFim(
      boost::static_pointer_cast<IFim>(fim),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)));

  data_server_.set_dsg_state(2, kDSGDegraded, 0);
  worker_->Process(fim, c_fim_socket_);

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, ReadFimDegradeRead) {
  // Setup
  FIM_PTR<ReadFim> fim(new ReadFim);
  (*fim)->inode = 132;
  (*fim)->dsg_off = kSegmentSize + 100;
  (*fim)->size = 8;
  (*fim)->target_role = 0;
  (*fim)->checksum_role = 1;
  PrepareCalls(1, 132);
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(true));
  char* buf;
  EXPECT_CALL(*cache_handle, Read(100, _, 8))
      .WillOnce(SaveArg<1>(&buf));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  EXPECT_CALL(*cache_tracker_, SetCache(132, 7));

  data_server_.set_dsg_state(2, kDSGDegraded, 0);
  worker_->Process(fim, c_fim_socket_);
  EXPECT_TRUE(reply->is_final());

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, RecoveryInit) {
  // Setup
  FIM_PTR<ReadFim> fim(new ReadFim);
  InodeNum inode = (*fim)->inode = kNumDSPerGroup + 2;
  std::size_t start_dsg_off = (kNumDSPerGroup - 2) * kSegmentSize;
  (*fim)->dsg_off = start_dsg_off + 100;
  (*fim)->size = 8;
  (*fim)->target_role = 0;
  (*fim)->checksum_role = 1;
  PrepareCalls(1, inode);

  // Degraded, not yet initialized and recovery not started
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(inode, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(false));
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(inode, 0, true))
      .WillRepeatedly(Return(rec_handle));
  EXPECT_CALL(*rec_handle, SetStarted())
      .WillOnce(Return(false));
  boost::shared_ptr<MockIFimSocket> ds_sockets[kNumDSPerGroup];
  FIM_PTR<IFim> rec_fim[kNumDSPerGroup];
  for (GroupRole r = 2; r < kNumDSPerGroup; ++r) {
    ds_sockets[r] = boost::make_shared<MockIFimSocket>();
    EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(4, r))
        .WillOnce(Return(ds_sockets[r]));
    EXPECT_CALL(*ds_sockets[r], WriteMsg(_))
        .WillOnce(SaveArg<0>(&rec_fim[r]));
  }
  EXPECT_CALL(*rec_handle, QueueFim(
      boost::static_pointer_cast<IFim>(fim),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)));

  data_server_.set_dsg_state(2, kDSGDegraded, 0);
  worker_->Process(fim, c_fim_socket_);
  for (GroupRole r = 2; r < kNumDSPerGroup; ++r) {
    EXPECT_EQ(inode, dynamic_cast<DataRecoveryFim&>(*rec_fim[r])->inode);
    EXPECT_EQ((r - 2) * kSegmentSize,
              dynamic_cast<DataRecoveryFim&>(*rec_fim[r])->dsg_off);
  }

  Mock::VerifyAndClear(tracker_mapper_);
  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, RecoveryInitError) {
  // Setup
  FIM_PTR<ReadFim> fim(new ReadFim);
  InodeNum inode = (*fim)->inode = kNumDSPerGroup + 2;
  std::size_t start_dsg_off = (kNumDSPerGroup - 2) * kSegmentSize;
  (*fim)->dsg_off = start_dsg_off + 100;
  (*fim)->size = 8;
  (*fim)->target_role = 0;
  (*fim)->checksum_role = 1;
  PrepareCalls(1, inode);

  // Degraded, not yet initialized and recovery not started
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(inode, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(false));
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(inode, 0, true))
      .WillRepeatedly(Return(rec_handle));
  EXPECT_CALL(*rec_handle, SetStarted())
      .WillOnce(Return(false));
  EXPECT_CALL(*tracker_mapper_, GetDSFimSocket(4, 2))
      .WillOnce(Return(boost::shared_ptr<IFimSocket>()));
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback, Call(PLEVEL(error, Degraded),
      StartsWith("Connection to peer DS is dropped during data recovery"), _));
  EXPECT_CALL(*rec_mgr_, DropHandle(inode, 0));
  data_server_.set_dsg_state(2, kDSGDegraded, 0);
  worker_->Process(fim, c_fim_socket_);

  Mock::VerifyAndClear(tracker_mapper_);
  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, DataRecoveryFim) {
  FIM_PTR<DataRecoveryFim> fim(new DataRecoveryFim);
  (*fim)->inode = 132;
  (*fim)->dsg_off = 3 * kNumDSPerGroup * kSegmentSize;
  FIM_PTR<IFim> reply;
  void* buf;
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, Read(132, false, 3 * kSegmentSize, _, kSegmentSize))
      .WillOnce(DoAll(SaveArg<3>(&buf),
                      Return(kSegmentSize)));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  worker_->Process(fim, c_fim_socket_);
  EXPECT_EQ(reply->tail_buf(), buf);
}

TEST_F(DSWorkerTest, DataRecoveryFimEmpty) {
  FIM_PTR<DataRecoveryFim> fim(new DataRecoveryFim);
  (*fim)->inode = 132;
  (*fim)->dsg_off = 3 * kNumDSPerGroup * kSegmentSize;
  FIM_PTR<IFim> reply;
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, Read(132, false, 3 * kSegmentSize, _, kSegmentSize))
      .WillOnce(Return(0));
  EXPECT_CALL(*c_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  worker_->Process(fim, c_fim_socket_);
  EXPECT_EQ(0U, reply->tail_buf_size());
}

TEST_F(DSWorkerTest, RecoveryDataFimPendingRecovery) {
  PrepareCalls(1, 132);

  // No recovery handle, do nothing
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 3 * kChecksumGroupSize, false))
      .WillOnce(Return(boost::shared_ptr<IDataRecoveryHandle>()));

  FIM_PTR<RecoveryDataFim> fim(new RecoveryDataFim);
  (*fim)->inode = 132;
  (*fim)->dsg_off = 3 * kChecksumGroupSize;
  (*fim)->result = 0;
  EXPECT_TRUE(worker_->Process(fim, c_fim_socket_));

  // Has recovery handle, AddData says not completed
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 3 * kChecksumGroupSize, false))
      .WillRepeatedly(Return(rec_handle));
  EXPECT_CALL(*rec_handle, AddData(
      reinterpret_cast<char*>(0),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)))
      .WillOnce(Return(false));

  EXPECT_TRUE(worker_->Process(fim, c_fim_socket_));

  // Same, non-null
  FIM_PTR<RecoveryDataFim> fim2(new RecoveryDataFim(kSegmentSize));
  (*fim2)->inode = 132;
  (*fim2)->dsg_off = 3 * kNumDSPerGroup * kSegmentSize;
  (*fim2)->result = 0;
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 3 * kChecksumGroupSize, false))
      .WillRepeatedly(Return(rec_handle));
  EXPECT_CALL(*rec_handle, AddData(
      fim2->tail_buf(),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)))
      .WillOnce(Return(false));

  EXPECT_TRUE(worker_->Process(fim2, c_fim_socket_));

  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, RecoveryDataFimRecoveryDone) {
  // Setup
  PrepareCalls(1, 132);
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(132,
                                   kNumDSPerGroup * kChecksumGroupSize, false))
      .WillRepeatedly(Return(rec_handle));
  FIM_PTR<RecoveryDataFim> fim(new RecoveryDataFim(kSegmentSize));
  (*fim)->inode = 132;
  (*fim)->dsg_off = kNumDSPerGroup * kChecksumGroupSize;
  (*fim)->result = 0;
  EXPECT_CALL(*rec_handle, AddData(
      fim->tail_buf(),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)))
      .WillRepeatedly(Return(true));
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132,
                                          kNumDSPerGroup * kChecksumGroupSize))
      .WillRepeatedly(Return(cache_handle));

  // Found totally empty data; no Fim to replay
  void* buf;
  EXPECT_CALL(*store_, Read(132, true, kSegmentSize, _, kSegmentSize))
      .WillOnce(DoAll(SaveArg<3>(&buf),
                      Return(0)));
  EXPECT_CALL(*rec_handle, RecoverData(Eq(ByRef(buf))))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_handle, Initialize());
  EXPECT_CALL(*rec_handle, UnqueueFim(_))
      .WillOnce(Return(FIM_PTR<IFim>()));
  EXPECT_CALL(*rec_mgr_, DropHandle(132, kNumDSPerGroup * kChecksumGroupSize));

  EXPECT_TRUE(worker_->Process(fim, c_fim_socket_));

  // Found nonempty data; has Fim to process
  EXPECT_CALL(*store_, Read(132, true, kSegmentSize, _, kSegmentSize))
      .WillOnce(Return(0));
  EXPECT_CALL(*rec_handle, RecoverData(_))
      .WillOnce(Return(true));
  EXPECT_CALL(*cache_handle, Allocate());
  char data[kSegmentSize];
  EXPECT_CALL(*cache_handle, data())
      .WillOnce(Return(data));
  FIM_PTR<OpenFim> qfim(new OpenFim);  // Fake, to ease test
  (*qfim)->inode = 132;
  EXPECT_CALL(*rec_handle, UnqueueFim(_))
      .WillOnce(Return(qfim))
      .WillOnce(Return(FIM_PTR<IFim>()));
  EXPECT_CALL(*rec_mgr_, DropHandle(132, kNumDSPerGroup * kChecksumGroupSize));

  EXPECT_TRUE(worker_->Process(fim, c_fim_socket_));

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, RecoveryDataFimRecoveryException) {
  // Setup
  PrepareCalls(0, 132);
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, kNumDSPerGroup * kChecksumGroupSize,
                                   false))
      .WillRepeatedly(Return(rec_handle));
  FIM_PTR<RecoveryDataFim> fim(new RecoveryDataFim(kSegmentSize));
  (*fim)->inode = 132;
  (*fim)->dsg_off = kNumDSPerGroup * kChecksumGroupSize;
  (*fim)->result = 0;
  EXPECT_CALL(*rec_handle, AddData(
      fim->tail_buf(),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)))
      .WillRepeatedly(Return(true));
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132,
                                          kNumDSPerGroup * kChecksumGroupSize))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*store_, Read(132, true, kSegmentSize, _, kSegmentSize))
      .WillOnce(Return(0));
  EXPECT_CALL(*rec_handle, RecoverData(_))
      .WillOnce(Return(true));
  EXPECT_CALL(*cache_handle, Allocate());
  char data[kSegmentSize];
  EXPECT_CALL(*cache_handle, data())
      .WillOnce(Return(data));
  boost::shared_ptr<MockIFimSocket> peer(new MockIFimSocket);
  EXPECT_CALL(*peer, GetReqTracker())
      .WillOnce(Return(c_tracker_.get()));
  FIM_PTR<ReadFim> qfim(new ReadFim);
  (*qfim)->inode = 132;
  (*qfim)->dsg_off = 0;
  (*qfim)->size = 1024;
  (*qfim)->checksum_role = 1;
  EXPECT_CALL(*rec_handle, UnqueueFim(_))
      .WillOnce(DoAll(SetArgPointee<0>(peer),
                      Return(qfim)))
      .WillOnce(Return(FIM_PTR<IFim>()));
  EXPECT_CALL(*store_, Read(132, false, 0, _, 1024))
      .WillOnce(Throw(std::runtime_error("error")));
  EXPECT_CALL(*peer, WriteMsg(_));  // Send error reply
  EXPECT_CALL(*rec_mgr_, DropHandle(132, kNumDSPerGroup * kChecksumGroupSize));
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Degraded),
                   StartsWith("Exception when handling deferred Fim "),
                   _));

  EXPECT_TRUE(worker_->Process(fim, c_fim_socket_));

  Mock::VerifyAndClear(rec_handle.get());
  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, RecoveryDataFimResultError) {
  FIM_PTR<RecoveryDataFim> fim(new RecoveryDataFim(kSegmentSize));
  (*fim)->inode = 132;
  (*fim)->dsg_off = 3 * kChecksumGroupSize;
  (*fim)->result = 1;
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback, Call(PLEVEL(error, Degraded),
              StartsWith("Error while collecting recovery data, errno: 1"),
              _));
  EXPECT_CALL(*rec_mgr_, DropHandle(132, 3 * kChecksumGroupSize));

  EXPECT_TRUE(worker_->Process(fim, c_fim_socket_));
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, RecoverDataFimReadError) {
  // Setup
  PrepareCalls(0, 132);
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, kNumDSPerGroup * kChecksumGroupSize,
                                   false))
      .WillRepeatedly(Return(rec_handle));
  FIM_PTR<RecoveryDataFim> fim(new RecoveryDataFim(kSegmentSize));
  (*fim)->inode = 132;
  (*fim)->dsg_off = kNumDSPerGroup * kChecksumGroupSize;
  (*fim)->result = 0;
  EXPECT_CALL(*rec_handle, AddData(
      fim->tail_buf(),
      boost::static_pointer_cast<IFimSocket>(c_fim_socket_)))
      .WillRepeatedly(Return(true));
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(132,
                                          kNumDSPerGroup * kChecksumGroupSize))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*store_, Read(132, true, kSegmentSize, _, kSegmentSize))
      .WillOnce(Return(-1));
  MockLogCallback mock_log_callback;
  LogRoute log_route(mock_log_callback.GetLogCallback());
  EXPECT_CALL(mock_log_callback,
              Call(PLEVEL(error, Degraded),
                   StartsWith("Error while reading recovery data, errno: -1"),
                   _));
  EXPECT_CALL(*rec_mgr_, DropHandle(132, kNumDSPerGroup * kChecksumGroupSize));

  EXPECT_TRUE(worker_->Process(fim, c_fim_socket_));
  Mock::VerifyAndClear(rec_handle.get());
  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, ChecksumUpdateFim) {
  FIM_PTR<ChecksumUpdateFim> fim(new ChecksumUpdateFim(6));
  (*fim)->inode = 132;
  (*fim)->last_off = 1000;
  (*fim)->dsg_off = 100;
  std::memcpy(fim->tail_buf(), "abcde", 6);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 0, false))
      .WillOnce(Return(boost::shared_ptr<IDataRecoveryHandle>()));
  PrepareCalls(1, 132);
  EXPECT_CALL(*store_, ApplyDelta(132, Ref((*fim)->optime), 1000,
                                  100, fim->tail_buf(), 6, true));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*d_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  worker_->Process(fim, d_fim_socket_);
  EXPECT_TRUE(reply->is_final());
}

TEST_F(DSWorkerTest, ChecksumUpdateFimDefer) {
  FIM_PTR<ChecksumUpdateFim> fim(new ChecksumUpdateFim(6));
  (*fim)->inode = 132;
  (*fim)->dsg_off = 100;
  std::memcpy(fim->tail_buf(), "abcde", 6);
  // During data recovery, ...
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(132, 0, false))
      .WillOnce(Return(rec_handle));
  FIM_PTR<IFim> ifim = fim;
  boost::shared_ptr<IFimSocket> isocket = d_fim_socket_;
  // after data is sent for the socket, ...
  EXPECT_CALL(*rec_handle, DataSent(
      boost::static_pointer_cast<IFimSocket>(isocket)))
      .WillOnce(Return(true));
  PrepareCalls(1, 132);
  // checksum updates will be deferred
  EXPECT_CALL(*rec_handle, QueueFim(ifim, isocket));

  worker_->Process(fim, d_fim_socket_);

  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, TruncateDataFimSelf) {  // Truncate offset is myself
  FIM_PTR<TruncateDataFim> fim(new TruncateDataFim);
  InodeNum inode = (*fim)->inode = kNumDSPerGroup;
  (*fim)->dsg_off = kSegmentSize + 100;
  (*fim)->checksum_role = kNumDSPerGroup - 1;
  FIM_PTR<IFim> reply;
  FSTime& optime = (*fim)->optime;
  char* ptr;
  PrepareCalls(1, inode);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(4, kNumDSPerGroup - 1))
      .WillOnce(Return(d_tracker_));
  EXPECT_CALL(*store_, TruncateData(inode, Ref(optime), 100, 0,
                                    kSegmentSize - 100, _))
      .WillOnce(DoAll(SaveArg<5>(&ptr),
                      Return(0)));
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  PrepareInvalidation(inode, kNotClient);
  boost::shared_ptr<IReqEntry> req_entry;
  MockAckCallback ack_callback;
  PrepareWriteRepl(inode, &req_entry, &ack_callback, m_fim_socket_);

  worker_->Process(fim, m_fim_socket_);
  EXPECT_FALSE(reply->is_final());
  ChecksumUpdateFim& rcs_req =
      dynamic_cast<ChecksumUpdateFim&>(*req_entry->request());
  EXPECT_EQ(inode, rcs_req->inode);
  EXPECT_EQ(kSegmentSize + 100U, rcs_req->dsg_off);
  EXPECT_EQ(kSegmentSize - 100U, rcs_req.tail_buf_size());
  EXPECT_EQ(ptr, rcs_req.tail_buf());
}

TEST_F(DSWorkerTest, TruncateDataFimAfter) {  // Truncate offset is after me
  FIM_PTR<TruncateDataFim> fim(new TruncateDataFim);
  InodeNum inode = (*fim)->inode = kNumDSPerGroup;
  (*fim)->dsg_off = kChecksumGroupSize + kSegmentSize + 100;
  (*fim)->checksum_role = kNumDSPerGroup - 2;
  FIM_PTR<IFim> reply;
  FSTime& optime = (*fim)->optime;
  char* ptr;
  PrepareCalls(kNumDSPerGroup - 1, inode);
  EXPECT_CALL(*store_, TruncateData(inode, Ref(optime), kSegmentSize,
                                    kSegmentSize, 0, _))
      .WillOnce(DoAll(SaveArg<5>(&ptr),
                      Return(0)));
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  PrepareInvalidation(inode, kNotClient);

  worker_->Process(fim, m_fim_socket_);
  EXPECT_TRUE(reply->is_final());
}

TEST_F(DSWorkerTest, TruncateDataFimFailChecksumSending) {
  InodeNum inode = kNumDSPerGroup;
  PrepareCalls(1, inode);
  EXPECT_CALL(*store_, TruncateData(inode, _, 100, 0, kSegmentSize - 100, _))
      .WillOnce(Return(0));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(4, kNumDSPerGroup - 1))
      .WillOnce(Return(d_tracker_));
  FIM_PTR<IFim> second_reply;
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(DoDefault())
      .WillOnce(SaveArg<0>(&second_reply));
  PrepareInvalidation(inode, kNotClient);
  EXPECT_CALL(*req_completion_checker_set_, Get(inode))
      .WillOnce(Return(req_completion_checker_));
  EXPECT_CALL(*d_tracker_, AddRequestEntry(_, _))
      .WillOnce(Return(false));

  FIM_PTR<TruncateDataFim> fim(new TruncateDataFim);
  fim->set_req_id(42);
  (*fim)->inode = inode;
  (*fim)->dsg_off = kSegmentSize + 100;
  (*fim)->checksum_role = kNumDSPerGroup - 1;
  worker_->Process(fim, m_fim_socket_);
  FinalReplyFim& r_reply = static_cast<FinalReplyFim&>(*second_reply);
  EXPECT_TRUE(r_reply.is_final());
  EXPECT_EQ(42U, r_reply.req_id());
}

TEST_F(DSWorkerTest, TruncateDataFimChecksumDegraded) {
  FIM_PTR<IFim> reply;
  InodeNum inode = kNumDSPerGroup;
  PrepareCalls(1, inode);
  EXPECT_CALL(*store_, TruncateData(inode, _, 100, 0, kSegmentSize - 100, _))
      .WillOnce(Return(0));
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  PrepareInvalidation(inode, kNotClient);

  FIM_PTR<TruncateDataFim> fim(new TruncateDataFim);
  (*fim)->inode = inode;
  (*fim)->dsg_off = kSegmentSize + 100;
  (*fim)->checksum_role = kNumDSPerGroup - 1;
  data_server_.set_dsg_state(2, kDSGDegraded, kNumDSPerGroup - 1);
  worker_->Process(fim, m_fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
  EXPECT_TRUE(reply->is_final());
}

TEST_F(DSWorkerTest, TruncateDataFimDefer) {
  FIM_PTR<TruncateDataFim> fim(new TruncateDataFim);
  InodeNum inode = (*fim)->inode = kNumDSPerGroup;
  (*fim)->dsg_off = kSegmentSize + 100;
  (*fim)->target_role = 1;
  (*fim)->checksum_role = kNumDSPerGroup - 1;

  PrepareCalls(kNumDSPerGroup - 1, inode);
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(inode, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(false));
  boost::shared_ptr<MockIDataRecoveryHandle> rec_handle(
      new MockIDataRecoveryHandle);
  EXPECT_CALL(*rec_mgr_, GetHandle(inode, 0, true))
      .WillRepeatedly(Return(rec_handle));
  EXPECT_CALL(*rec_handle, SetStarted())
      .WillOnce(Return(true));
  EXPECT_CALL(*rec_handle, QueueFim(
      boost::static_pointer_cast<IFim>(fim),
      boost::static_pointer_cast<IFimSocket>(m_fim_socket_)));

  data_server_.set_dsg_state(2, kDSGDegraded, 1);
  worker_->Process(fim, m_fim_socket_);

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, TruncateDataFimDegraded) {
  InodeNum inode = kNumDSPerGroup;
  PrepareCalls(kNumDSPerGroup - 1, inode);

  // Case 1: no data to do checksum for
  EXPECT_CALL(*degraded_cache_,
              Truncate(inode, (kNumDSPerGroup + 1) * kSegmentSize));
  PrepareInvalidation(inode, kNotClient);
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  FIM_PTR<TruncateDataFim> fim(new TruncateDataFim);
  (*fim)->inode = inode;
  (*fim)->last_off = kSegmentSize + 1000;
  (*fim)->dsg_off = 2 * kSegmentSize;
  (*fim)->target_role = 1;
  (*fim)->checksum_role = kNumDSPerGroup - 1;
  data_server_.set_dsg_state(2, kDSGDegraded, 1);
  worker_->Process(fim, m_fim_socket_);
  EXPECT_TRUE(reply->is_final());

  // Case 2: need checksum
  boost::shared_ptr<MockIDegradedCacheHandle> cache_handle(
      new MockIDegradedCacheHandle);
  EXPECT_CALL(*degraded_cache_, GetHandle(inode, 0))
      .WillRepeatedly(Return(cache_handle));
  EXPECT_CALL(*cache_handle, initialized())
      .WillOnce(Return(true));
  EXPECT_CALL(*degraded_cache_, Truncate(inode, kSegmentSize + 100));
  PrepareInvalidation(inode, kNotClient);
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  char* csu_buf;
  EXPECT_CALL(*cache_handle, Write(100, _, kSegmentSize - 100, _))
      .WillOnce(SaveArg<3>(&csu_buf));
  const void* cs_buf;
  EXPECT_CALL(*store_, ApplyDelta(inode, Ref((*fim)->optime),
                                  kSegmentSize + 1000,
                                  kSegmentSize + 100, _,
                                  kSegmentSize - 100, true))
      .WillOnce(DoAll(SaveArg<4>(&cs_buf),
                      Return(0)));

  (*fim)->dsg_off = kSegmentSize + 100;
  worker_->Process(fim, m_fim_socket_);
  EXPECT_TRUE(reply->is_final());

  Mock::VerifyAndClear(degraded_cache_);
  Mock::VerifyAndClear(rec_mgr_);
}

TEST_F(DSWorkerTest, MtimeUpdateFim) {
  FIM_PTR<MtimeUpdateFim> fim(new MtimeUpdateFim);
  (*fim)->inode = 132;
  (*fim)->mtime.sec = 123456789;
  (*fim)->mtime.ns = 12345;
  EXPECT_CALL(*store_, UpdateMtime(132, (*fim)->mtime, _))
      .WillOnce(DoAll(SetArgPointee<2>(42),
                      Return(0)));
  FIM_PTR<IFim> reply;
  PrepareCalls(0, 132);
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  worker_->Process(fim, m_fim_socket_);
  AttrUpdateReplyFim& rreply = dynamic_cast<AttrUpdateReplyFim&>(*reply);
  EXPECT_EQ(42U, rreply->size);
}

TEST_F(DSWorkerTest, FreeDataFim) {
  FIM_PTR<FreeDataFim> fim(new FreeDataFim);
  (*fim)->inode = 132;
  EXPECT_CALL(*degraded_cache_, FreeInode(132));
  EXPECT_CALL(*store_, FreeData(132, false));
  EXPECT_CALL(*cleaner_, RemoveInodeLater(132));
  FIM_PTR<IFim> reply;
  PrepareCalls(0, 132);
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  worker_->Process(fim, m_fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(DSWorkerTest, AttrUpdateFim) {
  // Normal case
  PrepareCalls(0, 132);
  FSTime mtime = {123456789U, 123456};
  EXPECT_CALL(*store_, GetAttr(132, _, _))
      .WillOnce(DoAll(SetArgPointee<1>(mtime),
                      SetArgPointee<2>(12345),
                      Return(-2)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  data_server_.set_dsg_state(1, kDSGResync, 0, 0);
  boost::unordered_set<InodeNum> inodes;
  data_server_.set_dsg_inodes_to_resync(&inodes);
  FIM_PTR<AttrUpdateFim> fim(new AttrUpdateFim);
  (*fim)->inode = 132;
  worker_->Process(fim, m_fim_socket_);
  AttrUpdateReplyFim& rreply = dynamic_cast<AttrUpdateReplyFim&>(*reply);
  EXPECT_EQ(123456789U, rreply->mtime.sec);
  EXPECT_EQ(123456U, rreply->mtime.ns);
  EXPECT_EQ(12345U, rreply->size);

  // Error case: Just reply 0
  PrepareCalls(0, 132);
  EXPECT_CALL(*store_, GetAttr(132, _, _))
      .WillOnce(Return(-2));
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  worker_->Process(fim, m_fim_socket_);
  AttrUpdateReplyFim& rreply2 = dynamic_cast<AttrUpdateReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply2->mtime.sec);
  EXPECT_EQ(0U, rreply2->mtime.ns);
  EXPECT_EQ(0U, rreply2->size);
}

TEST_F(DSWorkerTest, LockUnlock) {
  // When lock received, arrange a callback on completion of all pending Fims
  FIM_PTR<DSInodeLockFim> fim(new DSInodeLockFim);
  (*fim)->inode = 132;
  (*fim)->lock = 1;
  PrepareCalls(0, 132);
  EXPECT_CALL(*req_completion_checker_set_, Get(132))
      .WillOnce(Return(req_completion_checker_));
  ReqCompletionCallback req_completion_callback;
  EXPECT_CALL(*req_completion_checker_, OnCompleteAll(_))
      .WillOnce(SaveArg<0>(&req_completion_callback));

  worker_->Process(fim, m_fim_socket_);

  // Once callback is called, reply
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  req_completion_callback();
  ResultCodeReplyFim& rreply1 = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply1->err_no);

  // When locked, defer FC requests to a queue
  FIM_PTR<OpenFim> open_fim(new OpenFim(6));
  (*open_fim)->inode = 132;
  worker_->Process(open_fim, c_fim_socket_);

  // When unlock received, clear off the queue and reply
  FIM_PTR<IFim> reply2;
  (*fim)->lock = 0;
  {
    InSequence _seq;
    // OpenFim cannot be processed
    MockLogCallback mock_log_callback;
    LogRoute log_route(mock_log_callback.GetLogCallback());
    EXPECT_CALL(mock_log_callback,
                Call(PLEVEL(warning, Fim),
                     StrEq("Unprocessed Fim #Q00000-000000(Open) "
                           "on DS unlock"),
                     _));

    // Unlock
    EXPECT_CALL(*m_fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply2));

    worker_->Process(fim, m_fim_socket_);
    ResultCodeReplyFim& rreply2 = dynamic_cast<ResultCodeReplyFim&>(*reply2);
    EXPECT_EQ(0U, rreply2->err_no);
  }
}

TEST_F(DSWorkerTest, LockUnlockAll) {
  // When lock received, arrange a callback on completion of all pending Fims
  FIM_PTR<DSInodeLockFim> fim(new DSInodeLockFim);
  (*fim)->inode = 132;
  (*fim)->lock = 1;
  PrepareCalls(0, 132);
  EXPECT_CALL(*req_completion_checker_set_, Get(132))
      .WillOnce(Return(req_completion_checker_));
  ReqCompletionCallback req_completion_callback;
  EXPECT_CALL(*req_completion_checker_, OnCompleteAll(_))
      .WillOnce(SaveArg<0>(&req_completion_callback));

  worker_->Process(fim, m_fim_socket_);

  // Once callback is called, reply
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*m_fim_socket_, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  req_completion_callback();
  ResultCodeReplyFim& rreply1 = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply1->err_no);

  // When locked, defer FC requests to a queue
  FIM_PTR<OpenFim> open_fim(new OpenFim(6));
  (*open_fim)->inode = 132;
  worker_->Process(open_fim, c_fim_socket_);

  // When unlock all received, clear off the queue
  (*fim)->lock = 2;
  {
    InSequence _seq;
    // OpenFim cannot be processed
    MockLogCallback mock_log_callback;
    LogRoute log_route(mock_log_callback.GetLogCallback());
    EXPECT_CALL(mock_log_callback,
                Call(PLEVEL(warning, Fim),
                     StrEq("Unprocessed Fim #Q00000-000000(Open) "
                           "on DS unlock"),
                     _));

    // Unlock
    worker_->Process(fim, boost::shared_ptr<IFimSocket>());
  }
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
