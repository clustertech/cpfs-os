/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/worker_impl.hpp"

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>

#include <fuse/fuse_lowlevel.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
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
#include "dsg_state.hpp"
#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "log_testlib.hpp"
#include "mock_actions.hpp"
#include "mutex_util.hpp"
#include "op_completion_mock.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker_mock.hpp"
#include "tracer_impl.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ccache_tracker_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/dirty_inode_mock.hpp"
#include "server/ms/ds_locker_mock.hpp"
#include "server/ms/failover_mgr_mock.hpp"
#include "server/ms/inode_mutex.hpp"  // IWYU pragma: keep
#include "server/ms/inode_src_mock.hpp"
#include "server/ms/inode_usage_mock.hpp"
#include "server/ms/replier_mock.hpp"
#include "server/ms/startup_mgr_mock.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/store_mock.hpp"
#include "server/ms/topology_mock.hpp"
#include "server/reply_set_mock.hpp"
#include "server/worker.hpp"

using ::testing::_;
using ::testing::Contains;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::IgnoreResult;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Not;
using ::testing::Pointee;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SaveArgPointee;
using ::testing::StartsWith;
using ::testing::StrEq;
using ::testing::Throw;

namespace cpfs {
namespace server {
namespace ms {
namespace {

class MSWorkerTest : public ::testing::Test {
 protected:
  MockIFimProcessor queuer_;
  MockBaseMetaServer server_;
  MockIStateMgr* state_mgr_;
  MockIStore* store_;
  MockITrackerMapper* tracker_mapper_;
  MockICCacheTracker* cache_tracker_;
  MockIDSLocker* ds_locker_;
  MockIReplier* replier_;
  MockIReplySet* recent_reply_set_;
  MockIInodeSrc* inode_src_;
  MockIInodeUsage* inode_usage_;
  MockIDirtyInodeMgr* dirty_inode_mgr_;
  MockIAttrUpdater* attr_updater_;
  MockITopologyMgr* topology_mgr_;
  MockIFailoverMgr* failover_mgr_;
  MockIStartupMgr* startup_mgr_;
  MockIOpCompletionCheckerSet* ds_completion_checker_set_;

  boost::shared_ptr<MockIFimSocket> fim_socket_;
  boost::shared_ptr<MockIReqTracker> tracker_;
  boost::shared_ptr<MockIFimSocket> ms_fim_socket_;
  boost::shared_ptr<MockIReqTracker> ms_tracker_;
  boost::shared_ptr<MockIOpCompletionChecker> completion_checker_;
  const FIM_PTR<IFim> empty_ifim_;

  MockFunction<void(InodeNum, bool)> inv_func_;

  boost::scoped_ptr<IWorker> worker_;

  MSWorkerTest()
      : fim_socket_(new MockIFimSocket),
        tracker_(new MockIReqTracker),
        ms_fim_socket_(new MockIFimSocket),
        ms_tracker_(new MockIReqTracker),
        completion_checker_(new MockIOpCompletionChecker),
        worker_(MakeWorker()) {
    worker_->SetQueuer(&queuer_);
    server_.set_state_mgr(state_mgr_ = new MockIStateMgr);
    server_.set_store(store_ = new MockIStore);
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    server_.set_cache_tracker(cache_tracker_ = new MockICCacheTracker);
    server_.set_ds_locker(ds_locker_ = new MockIDSLocker);
    server_.set_replier(replier_ = new MockIReplier);
    server_.set_recent_reply_set(recent_reply_set_ = new MockIReplySet);
    server_.set_inode_src(inode_src_ = new MockIInodeSrc);
    server_.set_inode_usage(inode_usage_ = new MockIInodeUsage);
    server_.set_dirty_inode_mgr(dirty_inode_mgr_ = new MockIDirtyInodeMgr);
    server_.set_attr_updater(attr_updater_ = new MockIAttrUpdater);
    server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    server_.set_failover_mgr(failover_mgr_ = new MockIFailoverMgr);
    server_.set_startup_mgr(startup_mgr_ = new MockIStartupMgr);
    server_.set_ds_completion_checker_set(
        ds_completion_checker_set_ = new MockIOpCompletionCheckerSet);
    server_.set_tracer(MakeSmallTracer());
    EXPECT_CALL(*fim_socket_, GetReqTracker())
        .WillRepeatedly(Return(tracker_.get()));
    EXPECT_CALL(*tracker_, peer_client_num())
        .WillRepeatedly(Return(10));
    EXPECT_CALL(*tracker_mapper_, GetFCFimSocket(10))
        .WillRepeatedly(Return(fim_socket_));
    EXPECT_CALL(*tracker_mapper_, GetMSTracker())
        .WillRepeatedly(Return(ms_tracker_));
    EXPECT_CALL(*ms_tracker_, GetFimSocket())
        .WillRepeatedly(Return(ms_fim_socket_));
    EXPECT_CALL(*recent_reply_set_, FindReply(_))
        .WillRepeatedly(Return(FIM_PTR<IFim>()));
    EXPECT_CALL(*topology_mgr_, GetDSGState(2, _))
        .WillRepeatedly(Return(kDSGReady));
    EXPECT_CALL(*topology_mgr_, GetDSGState(3, _))
        .WillRepeatedly(Return(kDSGReady));
    EXPECT_CALL(*ds_completion_checker_set_, Get(_))
        .WillRepeatedly(Return(completion_checker_));
    worker_->set_server(&server_);
    worker_->SetCacheInvalFunc(
        boost::bind(GetMockCall(inv_func_), &inv_func_, _1, _2));
  }

  ~MSWorkerTest() {
    Mock::VerifyAndClear(ds_completion_checker_set_);
    Mock::VerifyAndClear(fim_socket_.get());
    Mock::VerifyAndClear(tracker_mapper_);
  }

  struct DSTruncateMocks {
    MSWorkerTest* test_;
    boost::shared_ptr<MockIReqTracker> tracker[kNumDSPerGroup];
    FIM_PTR<IFim> req[kNumDSPerGroup];
    boost::shared_ptr<MockIReqEntry> entry[kNumDSPerGroup];

    DSTruncateMocks(MSWorkerTest* test, GroupId group,
                    GroupRole except = kNumDSPerGroup)
        : test_(test) {
      EXPECT_CALL(*test_->completion_checker_, RegisterOp(_));
      for (GroupRole i = 0; i < kNumDSPerGroup; ++i) {
        if (i == except)
          continue;
        // Would call tracker_maper_->GetDSTracker to get trackers
        tracker[i] = boost::make_shared<MockIReqTracker>();
        EXPECT_CALL(*test->tracker_mapper_, GetDSTracker(group, i))
            .WillOnce(Return(tracker[i]));
        // Would call ds_tracker[i]->AddRequest to send requests, and then
        // wait for their replies using req_entry[i]->WaitReply
        entry[i] = boost::make_shared<MockIReqEntry>();
        EXPECT_CALL(*tracker[i], AddRequest(_, _))
            .WillOnce(DoAll(SaveArg<0>(&req[i]),
                            Return(entry[i])));
        if (i != 1)
          EXPECT_CALL(*entry[i], WaitReply())
              .WillOnce(ReturnRef(test_->empty_ifim_));
        else  // Test exception path
          EXPECT_CALL(*entry[i], WaitReply())
              .WillOnce(Throw(std::runtime_error("Test exception")));
      }
      EXPECT_CALL(*test_->ds_completion_checker_set_, CompleteOp(_, _));
    }

    ~DSTruncateMocks() {
      Mock::VerifyAndClear(test_->tracker_mapper_);
      for (GroupRole i = 0; i < kNumDSPerGroup; ++i) {
        Mock::VerifyAndClear(entry[i].get());
        Mock::VerifyAndClear(tracker[i].get());
      }
    }
  };

  struct DSFreeInodeMocks {
    MSWorkerTest* test_;
    boost::shared_ptr<MockIReqTracker> tracker[kNumDSPerGroup];
    boost::shared_ptr<IReqEntry> entry[kNumDSPerGroup];

    explicit DSFreeInodeMocks(MSWorkerTest* test, GroupId group,
                              GroupRole except = kNumDSPerGroup)
        : test_(test) {
      for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
        if (r == except)
          continue;
        tracker[r].reset(new MockIReqTracker);
        EXPECT_CALL(*test->tracker_mapper_, GetDSTracker(group, r))
            .WillOnce(Return(tracker[r]));
        EXPECT_CALL(*tracker[r], AddRequestEntry(_, _))
            .WillOnce(DoAll(SaveArg<0>(&entry[r]),
                            Return(true)));
      }
    }

    ~DSFreeInodeMocks() {
      Mock::VerifyAndClear(test_->tracker_mapper_);
      for (GroupRole r = 0; r < kNumDSPerGroup; ++r)
        Mock::VerifyAndClear(tracker[r].get());
    }
  };
};

MATCHER_P(HasReqContext, context, "") {
  return arg->req_context == context;
}

// Generic inode handling

TEST_F(MSWorkerTest, GetattrClean) {
  FIM_PTR<GetattrFim> fim(new GetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 2;
  FileAttr* attr;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 2,
                            reinterpret_cast<FileAttr*>(0), 0, true, _))
      .WillOnce(DoAll(SaveArg<5>(&attr),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(2, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_tracker_, SetCache(2, 10));
  FIM_PTR<IFim> reply;
  boost::function<void()> cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&cb)));

  worker_->Process(fim, fim_socket_);

  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(2U, rreply->inode);
  EXPECT_EQ(attr, &rreply->fa);
  EXPECT_FALSE(cb);
}

TEST_F(MSWorkerTest, GetattrDirty) {
  FIM_PTR<GetattrFim> fim(new GetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 2;
  FileAttr target_attr;
  target_attr.mtime.sec = 1234567890;
  target_attr.mtime.ns = 12345;
  target_attr.size = 123456;
  FileAttr* attr;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 2,
                            reinterpret_cast<FileAttr*>(0), 0, true, _))
      .WillOnce(DoAll(SaveArg<5>(&attr),
                      SetArgPointee<5>(target_attr),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(2, _))
      .WillOnce(Return(true));
  AttrUpdateHandler handler;
  EXPECT_CALL(*attr_updater_, AsyncUpdateAttr(2, target_attr.mtime, 123456, _))
      .WillOnce(DoAll(SaveArg<3>(&handler),
                      Return(true)));

  worker_->Process(fim, fim_socket_);

  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  FSTime new_mtime = {1234567891ULL, 12346};
  handler(new_mtime, 123457);
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(new_mtime, rreply->fa.mtime);
  EXPECT_EQ(123457U, rreply->fa.size);
}

TEST_F(MSWorkerTest, GetattrFailure) {
  FIM_PTR<GetattrFim> fim(new GetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 2;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 2,
                            reinterpret_cast<FileAttr*>(0), 0, true, _))
      .WillOnce(Return(-ENOENT));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(ENOENT), rreply->err_no);
}

TEST_F(MSWorkerTest, Setattr) {
  FIM_PTR<SetattrFim> fim(new SetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 2;
  (*fim)->fa.mode = 0775;
  (*fim)->fa_mask = FUSE_SET_ATTR_MODE;
  (*fim)->locked = 0;
  FileAttr attr;
  attr.size = 12345;
  attr.mtime.sec = 1234567890;
  attr.mtime.ns = 42;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 2, &(*fim)->fa,
                            FUSE_SET_ATTR_MODE, false, _))
      .WillOnce(DoAll(SetArgPointee<5>(attr),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*cache_tracker_, SetCache(2, 10));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(2, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_tracker_, InvGetClients(2, 10, _));
  EXPECT_CALL(inv_func_, Call(2, false));

  worker_->Process(fim, fim_socket_);

  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(2U, rreply->inode);
  EXPECT_EQ(12345U, rreply->fa.size);
  EXPECT_EQ(1234567890U, rreply->fa.mtime.sec);
  EXPECT_EQ(12345U, (*fim)->fa.size);
  EXPECT_EQ(1234567890U, (*fim)->fa.mtime.sec);
}

TEST_F(MSWorkerTest, SetattrRepl) {
  EXPECT_CALL(*tracker_, peer_client_num())
      .WillRepeatedly(Return(kNotClient));
  FIM_PTR<SetattrFim> fim(new SetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 2;
  (*fim)->fa.mode = 0775;
  (*fim)->fa_mask = FUSE_SET_ATTR_MODE;
  (*fim)->fa.size = 12345;
  (*fim)->fa.mtime.sec = 1234567890;
  (*fim)->fa.mtime.ns = 42;
  (*fim)->locked = 0;
  FileAttr attr;
  attr.size = 1234;
  attr.mtime.sec = 1234567891;
  attr.mtime.ns = 142;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 2, &(*fim)->fa,
                            FUSE_SET_ATTR_MODE, true, _))
      .WillOnce(DoAll(SetArgPointee<5>(attr),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(2, _))
      .WillOnce(Return(false));

  worker_->Process(fim, fim_socket_);

  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(2U, rreply->inode);
  EXPECT_EQ(12345U, rreply->fa.size);
  EXPECT_EQ(1234567890U, rreply->fa.mtime.sec);
  EXPECT_EQ(42U, rreply->fa.mtime.ns);
}

void DoReply(boost::shared_ptr<IReqEntry> entry, FIM_PTR<IFim> reply) {
  entry->SetReply(reply, 42);
}

TEST_F(MSWorkerTest, SetattrDirty) {
  FIM_PTR<SetattrFim> fim(new SetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 2;
  (*fim)->fa.mode = 0775;
  (*fim)->fa_mask = FUSE_SET_ATTR_MODE;
  (*fim)->locked = 0;
  FileAttr attr;
  attr.mode = 0101755;
  attr.size = 1234;
  attr.mtime.sec = 1234567890;
  attr.mtime.ns = 12345678;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 2, &(*fim)->fa,
                            FUSE_SET_ATTR_MODE, false, _))
      .WillOnce(DoAll(SetArgPointee<5>(attr),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(2, _))
      .WillOnce(Return(true));
  std::vector<GroupId> groups;
  groups.push_back(3);
  EXPECT_CALL(*store_, GetFileGroupIds(2, _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  boost::shared_ptr<MockIReqTracker> tracker[kNumDSPerGroup];
  // Arrange for AttrUpdateFim to DS
  boost::shared_ptr<IReqEntry> entry[kNumDSPerGroup];
  for (GroupRole i = 0; i < kNumDSPerGroup; ++i) {
    // Would call tracker_maper_->GetDSTracker to get trackers
    tracker[i] = boost::make_shared<MockIReqTracker>();
    EXPECT_CALL(*tracker_mapper_, GetDSTracker(3, i))
        .WillOnce(Return(tracker[i]));
    FIM_PTR<AttrUpdateReplyFim> ds_reply = AttrUpdateReplyFim::MakePtr();
    (*ds_reply)->mtime.sec = 1234567891;
    (*ds_reply)->mtime.ns = 2;
    (*ds_reply)->size = 2048;
    // Would call ds_tracker[i]->AddRequestEntry to send requests
    EXPECT_CALL(*tracker[i], AddRequestEntry(_, 0))
        .WillOnce(DoAll(SaveArg<0>(&entry[i]),
                        Invoke(boost::bind(&DoReply, _1,
                                           i ? ds_reply : FIM_PTR<IFim>())),
                        Return(true)));
  }
  EXPECT_CALL(*cache_tracker_, InvGetClients(2, 10, _));
  EXPECT_CALL(inv_func_, Call(2, false));

  worker_->Process(fim, fim_socket_);
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(2U, rreply->inode);
  EXPECT_EQ(0101755U, rreply->fa.mode);
  EXPECT_EQ(1234567891U, rreply->fa.mtime.sec);
  EXPECT_EQ(2U, rreply->fa.mtime.ns);
  EXPECT_EQ(2048U, rreply->fa.size);

  for (GroupRole i = 0; i < kNumDSPerGroup; ++i)
    Mock::VerifyAndClear(tracker[i].get());
  Mock::VerifyAndClear(replier_);
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSWorkerTest, SetattrTruncate) {
  FIM_PTR<SetattrFim> fim(new SetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 1;
  (*fim)->context.optime.sec = 1234567890ULL;
  (*fim)->context.optime.ns = 987654321;
  (*fim)->fa.size = 32768 + 1024;
  (*fim)->fa_mask = FUSE_SET_ATTR_SIZE;
  (*fim)->locked = 0;
  FileAttr* attr;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_SIZE, false, _))
      .WillOnce(Throw(MDNeedLock("msg")));
  // Would call store_->GetFileGroupIds to get groups
  std::vector<GroupId> groups;
  groups.push_back(3);
  EXPECT_CALL(*store_, GetFileGroupIds(1, _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  EXPECT_CALL(*ds_locker_, Lock(1, _, _));
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_SIZE, true, _))
      .WillOnce(DoAll(SaveArg<5>(&attr),
                      Return(0)));
  DSTruncateMocks truncate_mocks(this, 3);
  EXPECT_CALL(*dirty_inode_mgr_, NotifyAttrSet(1));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*cache_tracker_, SetCache(1, 10));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(1, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, false));

  worker_->Process(fim, fim_socket_);

  // Normal checks
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(attr, &rreply->fa);
  // Check two truncate data fim to ensure correct role get correct Fim
  TruncateDataFim& rreq0 =  // checksum
      dynamic_cast<TruncateDataFim&>(*truncate_mocks.req[0]);
  EXPECT_EQ(1234567890ULL, rreq0->optime.sec);
  EXPECT_EQ(987654321U, rreq0->optime.ns);
  EXPECT_EQ(32768U + 1024U, rreq0->last_off);
  EXPECT_EQ(0U, rreq0->target_role);
  EXPECT_EQ(0U, rreq0->checksum_role);
  EXPECT_EQ(32768U + 1024U, rreq0->dsg_off);
  TruncateDataFim& rreq2 =
      dynamic_cast<TruncateDataFim&>(*truncate_mocks.req[2]);
  EXPECT_EQ(1234567890ULL, rreq2->optime.sec);
  EXPECT_EQ(987654321U, rreq2->optime.ns);
  EXPECT_EQ(32768U + 1024U, rreq2->last_off);
  EXPECT_EQ(2U, rreq2->target_role);
  EXPECT_EQ(0U, rreq2->checksum_role);
  EXPECT_EQ(32768U + 1024U, rreq2->dsg_off);

  Mock::VerifyAndClear(replier_);
}

TEST_F(MSWorkerTest, SetattrMtime) {
  FIM_PTR<SetattrFim> fim(new SetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 1;
  FSTime mtime = {1234567891ULL, 987654322};
  (*fim)->context.optime.sec = 1234567890ULL;
  (*fim)->context.optime.ns = 987654321;
  (*fim)->fa.mtime = mtime;
  (*fim)->fa_mask = FUSE_SET_ATTR_MTIME;
  (*fim)->locked = 0;
  FileAttr attr;
  attr.size = 1024;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_MTIME, false, _))
      .WillOnce(Throw(MDNeedLock("msg")));
  // Would call store_->GetFileGroupIds to get groups
  std::vector<GroupId> groups;
  groups.push_back(3);
  EXPECT_CALL(*store_, GetFileGroupIds(1, _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  EXPECT_CALL(*ds_locker_, Lock(1, _, _));
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_MTIME, true, _))
      .WillOnce(DoAll(SetArgPointee<5>(attr),
                      Return(0)));
  boost::shared_ptr<MockIReqTracker> tracker[kNumDSPerGroup];
  // Arrange for MtimeUpdateFim to DS
  boost::shared_ptr<IReqEntry> entry[kNumDSPerGroup];
  for (GroupRole i = 0; i < kNumDSPerGroup; ++i) {
    // Would call tracker_maper_->GetDSTracker to get trackers
    tracker[i] = boost::make_shared<MockIReqTracker>();
    EXPECT_CALL(*tracker_mapper_, GetDSTracker(3, i))
        .WillOnce(Return(tracker[i]));
    FIM_PTR<AttrUpdateReplyFim> ds_reply = AttrUpdateReplyFim::MakePtr();
    (*ds_reply)->size = 2048;
    // Would call ds_tracker[i]->AddRequestEntry to send requests
    EXPECT_CALL(*tracker[i], AddRequestEntry(_, 0))
        .WillOnce(DoAll(SaveArg<0>(&entry[i]),
                        Invoke(boost::bind(&DoReply, _1,
                                           i ? ds_reply : FIM_PTR<IFim>())),
                        Return(true)));
  }
  EXPECT_CALL(*dirty_inode_mgr_, NotifyAttrSet(1));
  MockLogCallback callback;
  EXPECT_CALL(callback, Call(
      _,
      StartsWith("DS connection lost when waiting for inode 1 "),
      _));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(1, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, false));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  LogRoute route(callback.GetLogCallback());
  worker_->Process(fim, fim_socket_);
  ASSERT_EQ(kMtimeUpdateFim, entry[1]->request()->type());
  MtimeUpdateFim& rreq = reinterpret_cast<MtimeUpdateFim&>(
      *entry[1]->request());
  EXPECT_EQ(1U, rreq->inode);
  EXPECT_EQ(mtime, rreq->mtime);
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(1U, rreply->dirty);
  EXPECT_EQ(2048U, rreply->fa.size);

  for (GroupRole i = 0; i < kNumDSPerGroup; ++i)
    Mock::VerifyAndClear(tracker[i].get());
  Mock::VerifyAndClear(replier_);
  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(MSWorkerTest, SetattrTruncateDegradedChecksum) {
  FIM_PTR<SetattrFim> fim(new SetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 1;
  (*fim)->fa.size = 32768 + 1024;
  (*fim)->fa_mask = FUSE_SET_ATTR_SIZE;
  (*fim)->locked = 1;
  EXPECT_CALL(*topology_mgr_, GetDSGState(3, _))
      .WillRepeatedly(DoAll(SetArgPointee<1>(0),
                            Return(kDSGDegraded)));
  FileAttr* attr;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_SIZE, false, _))
      .WillOnce(Throw(MDNeedLock("msg")));
  // Would call store_->GetFileGroupIds to get groups
  std::vector<GroupId> groups;
  groups.push_back(3);
  EXPECT_CALL(*store_, GetFileGroupIds(1, _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  EXPECT_CALL(*inode_usage_, IsSoleWriter(1, 10))
      .WillOnce(Return(true));
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_SIZE, true, _))
      .WillOnce(DoAll(SaveArg<5>(&attr),
                      Return(0)));
  DSTruncateMocks truncate_mocks(this, 3, 0);  // Won't send to 0
  EXPECT_CALL(*dirty_inode_mgr_, NotifyAttrSet(1));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(1, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_tracker_, SetCache(1, 10));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, false));

  worker_->Process(fim, fim_socket_);

  // Normal checks
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(attr, &rreply->fa);
  // Check truncate data fim to ensure correct role get correct Fim
  TruncateDataFim& rreq2 =
      dynamic_cast<TruncateDataFim&>(*truncate_mocks.req[2]);
  EXPECT_EQ(0U, rreq2->checksum_role);
  EXPECT_EQ(kSegmentSize + 1024U, rreq2->dsg_off);

  Mock::VerifyAndClear(replier_);
}

TEST_F(MSWorkerTest, SetattrTruncateDegradedActual) {
  FIM_PTR<SetattrFim> fim(new SetattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 1;
  (*fim)->fa.size = 32768 + 1024;
  (*fim)->fa_mask = FUSE_SET_ATTR_SIZE;
  (*fim)->locked = 0;
  EXPECT_CALL(*topology_mgr_, GetDSGState(3, _))
      .WillRepeatedly(DoAll(SetArgPointee<1>(2),  // 2 failed
                            Return(kDSGDegraded)));
  FileAttr* attr;
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_SIZE, false, _))
      .WillOnce(Throw(MDNeedLock("msg")));
  // Would call store_->GetFileGroupIds to get groups
  std::vector<GroupId> groups;
  groups.push_back(3);
  EXPECT_CALL(*store_, GetFileGroupIds(1, _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  EXPECT_CALL(*ds_locker_, Lock(1, _, _));
  EXPECT_CALL(*store_, Attr(HasReqContext(&(*fim)->context), 1, &(*fim)->fa,
                            FUSE_SET_ATTR_SIZE, true, _))
      .WillOnce(DoAll(SaveArg<5>(&attr),
                      Return(0)));
  DSTruncateMocks truncate_mocks(this, 3, 2);  // Won't send to 2
  EXPECT_CALL(*dirty_inode_mgr_, NotifyAttrSet(1));
  // Send once more to 0
  // Note: reversed order due to how gmock works.
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(3, 0))
      .WillOnce(Return(truncate_mocks.tracker[0]))
      .RetiresOnSaturation();
  boost::shared_ptr<MockIReqEntry> earlier_entry(new MockIReqEntry);
  EXPECT_CALL(*earlier_entry, WaitReply())
      .WillOnce(ReturnRef(empty_ifim_));
  FIM_PTR<IFim> earlier_fim;
  EXPECT_CALL(*truncate_mocks.tracker[0], AddRequest(_, _))
      .WillOnce(DoAll(SaveArg<0>(&earlier_fim),
                      Return(earlier_entry)))
      .RetiresOnSaturation();

  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(1, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_tracker_, SetCache(1, 10));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, false));

  worker_->Process(fim, fim_socket_);

  // Normal checks
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(attr, &rreply->fa);
  // Check truncate data fim to ensure correct role get correct Fim
  TruncateDataFim& rreq2 =
      dynamic_cast<TruncateDataFim&>(*truncate_mocks.req[0]);
  EXPECT_EQ(0U, rreq2->checksum_role);
  EXPECT_EQ(kSegmentSize + 1024U, rreq2->dsg_off);

  Mock::VerifyAndClear(truncate_mocks.tracker[0].get());
  Mock::VerifyAndClear(tracker_mapper_);
  Mock::VerifyAndClear(replier_);
}

TEST_F(MSWorkerTest, Setxattr) {
  const char* attr_name = "user.test";
  const char* attr_value = "10";
  std::size_t attr_name_size = strlen(attr_name) + 1;
  std::size_t attr_value_size = strlen(attr_value) + 1;
  FIM_PTR<SetxattrFim> fim(new SetxattrFim(attr_name_size + attr_value_size));
  fim->set_req_id(3);
  (*fim)->inode = 3;
  std::strncpy(fim->tail_buf(), attr_name, attr_name_size);
  std::strncpy(fim->tail_buf() + attr_name_size, attr_value, attr_value_size);
  (*fim)->name_len = attr_name_size;
  (*fim)->value_len = attr_value_size;
  (*fim)->flags = 0;

  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  EXPECT_CALL(*store_,
      SetXattr(HasReqContext(&(*fim)->context), (*fim)->inode,
               StrEq(attr_name), StrEq(attr_value),
               attr_value_size, int((*fim)->flags)));

  EXPECT_CALL(*cache_tracker_, InvGetClients(3, 10, _));
  EXPECT_CALL(inv_func_, Call(3, false));

  worker_->Process(fim, fim_socket_);
}

TEST_F(MSWorkerTest, Getxattr) {
  const char* attr_name = "user.test";
  std::size_t attr_name_len = strlen(attr_name) + 1;
  FIM_PTR<GetxattrFim> fim(new GetxattrFim(attr_name_len));
  fim->set_req_id(3);
  (*fim)->inode = 3;
  std::memcpy(fim->tail_buf(), attr_name, attr_name_len);
  (*fim)->value_len = 256;

  const char msg[2] = {'1', '2'};
  EXPECT_CALL(*store_, GetXattr(HasReqContext(&(*fim)->context), (*fim)->inode,
                                fim->tail_buf(), _, 256))
      .WillOnce(DoAll(IgnoreResult(Invoke(boost::bind(&std::memcpy, _4,
                                                      msg, 2))),
                      Return(2)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  GetxattrReplyFim& rreply = dynamic_cast<GetxattrReplyFim&>(*reply);
  EXPECT_EQ(2U, rreply->value_len);
  ASSERT_EQ(2U, rreply.tail_buf_size());
  EXPECT_EQ("12", std::string(rreply.tail_buf(), 2));
}

TEST_F(MSWorkerTest, Listxattr) {
  FIM_PTR<ListxattrFim> fim(new ListxattrFim);
  fim->set_req_id(3);
  (*fim)->inode = 3;
  (*fim)->size = 2;

  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  char buf[2] = {'a', 'b'};
  EXPECT_CALL(*store_,
              ListXattr(HasReqContext(&(*fim)->context), (*fim)->inode, _, 2))
      .WillOnce(DoAll(IgnoreResult(Invoke(boost::bind(&std::memcpy, _3,
                                                      buf, 2))),
                      Return(2)));

  worker_->Process(fim, fim_socket_);
  ListxattrReplyFim& rreply = dynamic_cast<ListxattrReplyFim&>(*reply);
  EXPECT_EQ(2U, rreply->size);
  EXPECT_EQ(2U, rreply.tail_buf_size());
  EXPECT_EQ(std::string("ab"), std::string(rreply.tail_buf(), 2));
}

TEST_F(MSWorkerTest, Removexattr) {
  const char* attr_name = "user.test";
  std::size_t attr_name_len = strlen(attr_name) + 1;
  FIM_PTR<RemovexattrFim> fim(new RemovexattrFim(attr_name_len));
  fim->set_req_id(3);
  (*fim)->inode = 3;
  std::memcpy(fim->tail_buf(), attr_name, attr_name_len);

  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  EXPECT_CALL(*store_,
      RemoveXattr(HasReqContext(&(*fim)->context), 3, StrEq(attr_name)))
      .WillOnce(Return(0));

  worker_->Process(fim, fim_socket_);
}

// Non-directory inode handling

TEST_F(MSWorkerTest, Open) {
  FIM_PTR<OpenFim> fim(new OpenFim);
  fim->set_req_id((10ULL << 44) | 3ULL);
  (*fim)->inode = 5;
  (*fim)->flags = O_RDONLY;
  std::vector<GroupId> groups;
  groups.push_back(5);
  EXPECT_CALL(*store_, Open(HasReqContext(&(*fim)->context), 5, O_RDONLY, _,
                            _))
      .WillOnce(DoAll(SetArgPointee<3>(groups),
                      SetArgPointee<4>(false),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*inode_usage_, SetFCOpened(10U, 5U, kInodeReadAccess));

  worker_->Process(fim, fim_socket_);
  DataReplyFim& rreply = dynamic_cast<DataReplyFim&>(*reply);
  ASSERT_EQ(sizeof(GroupId), rreply.tail_buf_size());
  GroupId* groups_replied = reinterpret_cast<GroupId*>(rreply.tail_buf());
  EXPECT_EQ(5U, groups_replied[0]);
}

TEST_F(MSWorkerTest, OpenTruncate) {
  FIM_PTR<OpenFim> fim(new OpenFim);
  fim->set_req_id((10ULL << 44) | 3ULL);
  (*fim)->inode = 5;
  (*fim)->flags = O_RDWR | O_TRUNC;
  std::vector<GroupId> groups;
  groups.push_back(3);
  EXPECT_CALL(*store_, Open(HasReqContext(&(*fim)->context), 5,
                            O_RDWR | O_TRUNC, _, _))
      .WillOnce(DoAll(SetArgPointee<3>(groups),
                      SetArgPointee<4>(true),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(5, 0))
      .WillOnce(Return(false));
  EXPECT_CALL(*ds_locker_, Lock(5, _, _));
  DSTruncateMocks truncate_mocks(this, 3);
  EXPECT_CALL(*dirty_inode_mgr_, NotifyAttrSet(5));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*inode_usage_, SetFCOpened(10U, 5U, kInodeWriteAccess));
  EXPECT_CALL(*cache_tracker_, InvGetClients(5, 10, _));
  EXPECT_CALL(inv_func_, Call(5, false));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(5U, true, true, 0));

  worker_->Process(fim, fim_socket_);
  DataReplyFim& rreply = dynamic_cast<DataReplyFim&>(*reply);
  ASSERT_EQ(sizeof(GroupId), rreply.tail_buf_size());
  GroupId* groups_replied = reinterpret_cast<GroupId*>(rreply.tail_buf());
  EXPECT_EQ(3U, groups_replied[0]);
}

TEST_F(MSWorkerTest, OpenError) {
  FIM_PTR<OpenFim> fim(new OpenFim);
  fim->set_req_id((10ULL << 44) | 3ULL);
  (*fim)->inode = 5;
  (*fim)->flags = O_RDONLY;
  EXPECT_CALL(*store_, Open(HasReqContext(&(*fim)->context), 5, O_RDONLY, _,
                            _))
      .WillOnce(Return(-ENOENT));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(ENOENT), rreply->err_no);
}

TEST_F(MSWorkerTest, Access) {
  FIM_PTR<AccessFim> fim(new AccessFim);
  fim->set_req_id((10ULL << 44) | 3ULL);
  (*fim)->inode = 5;
  (*fim)->mask = W_OK;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*store_, Access(HasReqContext(&(*fim)->context), 5, W_OK))
      .WillOnce(Return(0));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, AdviseWrite) {
  FIM_PTR<AdviseWriteFim> fim(new AdviseWriteFim);
  fim->set_req_id(3);
  (*fim)->inode = 2;
  (*fim)->off = 1024;
  EXPECT_CALL(*store_, AdviseWrite(HasReqContext(&(*fim)->context), 2,
                                   1024));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*cache_tracker_, InvGetClients(2, 10, _));
  EXPECT_CALL(inv_func_, Call(2, true));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

// Continue writing to the inode, no cleaning needed yet
TEST_F(MSWorkerTest, ReleaseContWriting) {
  FIM_PTR<IFim> reply;
  FIM_PTR<ReleaseFim> fim(new ReleaseFim);
  fim->set_req_id(3);
  (*fim)->inode = 5;
  (*fim)->keep_read = 1;
  (*fim)->clean = false;
  EXPECT_CALL(*store_, OperateInode(_, 5));
  EXPECT_CALL(*inode_usage_, SetFCClosed(_, 5, true))
      .WillOnce(Return(false));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(5, true, false, _))
      .WillOnce(DoAll(SetArgPointee<3>(42),
                      Return(false)));
  EXPECT_CALL(*inode_usage_, IsPendingUnlink(5)).WillOnce(Return(true));
  EXPECT_CALL(*inode_usage_, IsOpened(5, false)).WillOnce(Return(false));
  std::vector<GroupId> groups;
  groups.push_back(2);
  EXPECT_CALL(*store_, GetFileGroupIds(InodeNum(5), _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  EXPECT_CALL(*store_, FreeInode(5));
  EXPECT_CALL(*inode_usage_, RemovePendingUnlink(5));
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

// Stop writing to the inode, trigger cleaning
TEST_F(MSWorkerTest, ReleaseStopWriting) {
  FIM_PTR<IFim> reply;
  FIM_PTR<ReleaseFim> fim(new ReleaseFim);
  fim->set_req_id(3);
  (*fim)->inode = 5;
  (*fim)->keep_read = 1;
  (*fim)->clean = false;
  EXPECT_CALL(*store_, OperateInode(_, 5));
  EXPECT_CALL(*inode_usage_, SetFCClosed(_, 5, true))
      .WillOnce(Return(true));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(5, false, false, _))
      .WillOnce(DoAll(SetArgPointee<3>(42),
                      Return(false)));
  EXPECT_CALL(*inode_usage_, IsPendingUnlink(5)).WillOnce(Return(true));
  EXPECT_CALL(*inode_usage_, IsOpened(5, false)).WillOnce(Return(false));
  std::vector<GroupId> groups;
  groups.push_back(2);
  EXPECT_CALL(*store_, GetFileGroupIds(InodeNum(5), _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  EXPECT_CALL(*store_, FreeInode(5));
  EXPECT_CALL(*inode_usage_, RemovePendingUnlink(5));
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));
  FileAttr fa;
  fa.mtime.sec = 123456789;
  fa.mtime.ns = 12345;
  fa.size = 1234;
  EXPECT_CALL(*store_, GetInodeAttr(5, _))
      .WillOnce(DoAll(SetArgPointee<1>(fa),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, StartCleaning(5))
      .WillOnce(Return(true));
  AttrUpdateHandler update_complete_handler;
  EXPECT_CALL(*attr_updater_, AsyncUpdateAttr(5, fa.mtime, 1234, _))
      .WillOnce(DoAll(SaveArg<3>(&update_complete_handler),
                      Return(true)));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);

  // Upon reception of reply, free the inode
  DSFreeInodeMocks free_mocks(this, 2);

  active_complete_cb();

  // Upon attribute update completion, enqueue a Fim to the thread group
  FIM_PTR<IFim> int_fim;
  EXPECT_CALL(queuer_, Process(_, boost::shared_ptr<IFimSocket>()))
      .WillOnce(DoAll(SaveArg<0>(&int_fim),
                      Return(true)));

  update_complete_handler(fa.mtime, 123456);
  AttrUpdateCompletionFim& rint_fim =
      dynamic_cast<AttrUpdateCompletionFim&>(*int_fim);
  EXPECT_EQ(fa.mtime, rint_fim->mtime);
  EXPECT_EQ(123456U, rint_fim->size);
  EXPECT_EQ(42U, rint_fim->gen);
}

// Stopped writing to the inode but clean, skips cleaning
TEST_F(MSWorkerTest, ReleaseStopWritingClean) {
  FIM_PTR<IFim> reply;
  FIM_PTR<ReleaseFim> fim(new ReleaseFim);
  fim->set_req_id(3);
  (*fim)->inode = 5;
  (*fim)->keep_read = 1;
  (*fim)->clean = true;
  EXPECT_CALL(*store_, OperateInode(_, 5));
  EXPECT_CALL(*inode_usage_, SetFCClosed(_, 5, true))
      .WillOnce(Return(true));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(5, false, true, _))
      .WillOnce(DoAll(SetArgPointee<3>(42),
                      Return(true)));
  EXPECT_CALL(*inode_usage_, IsPendingUnlink(5)).WillOnce(Return(false));
  EXPECT_CALL(*dirty_inode_mgr_, Clean(5, Pointee(42), 0));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ReleaseLost) {
  // Corner case: inode lost before release

  // This shouldn't actually happen, but is added into the production
  // code defensively, so a test is added for coverage
  FIM_PTR<IFim> reply;
  FIM_PTR<ReleaseFim> fim(new ReleaseFim);
  fim->set_req_id(3);
  (*fim)->inode = 5;
  (*fim)->keep_read = 1;
  (*fim)->clean = true;
  EXPECT_CALL(*store_, OperateInode(_, 5));
  EXPECT_CALL(*inode_usage_, SetFCClosed(_, 5, true))
      .WillOnce(Return(true));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(5, false, true, _))
      .WillOnce(DoAll(SetArgPointee<3>(42),
                      Return(false)));
  EXPECT_CALL(*inode_usage_, IsPendingUnlink(5)).WillOnce(Return(false));
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));
  EXPECT_CALL(*store_, GetInodeAttr(5, _))
      .WillOnce(Return(-ENOENT));
  EXPECT_CALL(*dirty_inode_mgr_, Clean(5, Pointee(uint64_t(-1)), 0))
      .WillOnce(Return(true));

  worker_->Process(fim, fim_socket_);
  EXPECT_FALSE(active_complete_cb);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ReleaseRepeated) {
  // Corner case: inode repeated release
  FIM_PTR<IFim> reply;
  FIM_PTR<ReleaseFim> fim(new ReleaseFim);
  fim->set_req_id(3);
  (*fim)->inode = 5;
  (*fim)->keep_read = 1;
  (*fim)->clean = false;
  EXPECT_CALL(*store_, OperateInode(_, 5));
  EXPECT_CALL(*inode_usage_, SetFCClosed(_, 5, true))
      .WillOnce(Return(true));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(5, false, false, _))
      .WillOnce(DoAll(SetArgPointee<3>(42),
                      Return(false)));
  EXPECT_CALL(*inode_usage_, IsPendingUnlink(5)).WillOnce(Return(false));
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));
  EXPECT_CALL(*store_, GetInodeAttr(5, _))
      .WillOnce(Return(0));
  EXPECT_CALL(*dirty_inode_mgr_, StartCleaning(5))
      .WillOnce(Return(false));

  worker_->Process(fim, fim_socket_);
  EXPECT_FALSE(active_complete_cb);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ReleaseDegraded) {
  FIM_PTR<IFim> reply;
  FIM_PTR<ReleaseFim> fim(new ReleaseFim);
  fim->set_req_id(3);
  (*fim)->inode = 5;
  (*fim)->keep_read = 0;
  (*fim)->clean = true;
  EXPECT_CALL(*store_, OperateInode(_, 5));
  EXPECT_CALL(*inode_usage_, SetFCClosed(_, 5, false));
  EXPECT_CALL(*inode_usage_, IsPendingUnlink(5)).WillOnce(Return(true));
  EXPECT_CALL(*inode_usage_, IsOpened(5, false)).WillOnce(Return(false));
  std::vector<GroupId> groups;
  groups.push_back(2);
  EXPECT_CALL(*store_, GetFileGroupIds(InodeNum(5), _))
      .WillOnce(DoAll(SetArgPointee<1>(groups),
                      Return(0)));
  EXPECT_CALL(*store_, FreeInode(5));
  EXPECT_CALL(*inode_usage_, RemovePendingUnlink(5));
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
  EXPECT_CALL(*topology_mgr_, GetDSGState(2, _))
      .WillRepeatedly(DoAll(SetArgPointee<1>(0),
                            Return(kDSGDegraded)));

  DSFreeInodeMocks free_mocks(this, 2, 0);
  active_complete_cb();
}

TEST_F(MSWorkerTest, AttrUpdateCompletion) {
  FSTime mtime = {123456789, 12345};
  EXPECT_CALL(*dirty_inode_mgr_,
              Clean(5, Pointee(42),
                    Not(reinterpret_cast<
                        boost::unique_lock<MUTEX_TYPE>*>(0))))
      .WillOnce(Return(true));
  EXPECT_CALL(*store_, UpdateInodeAttr(5, mtime, 1234));
  EXPECT_CALL(*state_mgr_, GetState())
      .WillOnce(Return(kStateActive));
  FIM_PTR<AttrUpdateCompletionFim> fim = AttrUpdateCompletionFim::MakePtr();
  FIM_PTR<IFim> ifim = fim;
  EXPECT_CALL(*ms_fim_socket_, WriteMsg(ifim));

  (*fim)->inode = 5;
  (*fim)->mtime = mtime;
  (*fim)->size = 1234;
  (*fim)->gen = 42;
  worker_->Process(fim, fim_socket_);
}

TEST_F(MSWorkerTest, AttrUpdateCompletionRetry) {
  FSTime mtime = {123456789, 12345};
  EXPECT_CALL(*dirty_inode_mgr_, Clean(5, Pointee(42), _))
      .WillOnce(DoAll(SetArgPointee<1>(43),
                      Return(false)));
  EXPECT_CALL(*store_, GetInodeAttr(5, _))
      .WillOnce(Return(0));
  EXPECT_CALL(*dirty_inode_mgr_, StartCleaning(5))
      .WillOnce(Return(false));

  FIM_PTR<AttrUpdateCompletionFim> fim = AttrUpdateCompletionFim::MakePtr();
  (*fim)->inode = 5;
  (*fim)->mtime = mtime;
  (*fim)->size = 1234;
  (*fim)->gen = 42;
  worker_->Process(fim, fim_socket_);
}

TEST_F(MSWorkerTest, AttrUpdateCompletionRetrySkip) {
  FSTime mtime = {123456789, 12345};
  EXPECT_CALL(*dirty_inode_mgr_, Clean(5, Pointee(42), _))
      .WillOnce(DoAll(SetArgPointee<1>(0),
                      Return(false)));

  FIM_PTR<AttrUpdateCompletionFim> fim = AttrUpdateCompletionFim::MakePtr();
  (*fim)->inode = 5;
  (*fim)->mtime = mtime;
  (*fim)->size = 1234;
  (*fim)->gen = 42;
  worker_->Process(fim, fim_socket_);
}

TEST_F(MSWorkerTest, Readlink) {
  std::vector<char> link_ret;
  link_ret.resize(6);
  std::memcpy(&link_ret[0], "target", 6);
  FIM_PTR<ReadlinkFim> fim(new ReadlinkFim);
  EXPECT_CALL(*store_, Readlink(HasReqContext(&(*fim)->context), 5, _))
      .WillOnce(DoAll(SetArgPointee<2>(link_ret),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  fim->set_req_id(3);
  (*fim)->inode = 5;
  worker_->Process(fim, fim_socket_);
  DataReplyFim& rreply = dynamic_cast<DataReplyFim&>(*reply);
  EXPECT_EQ(7U, rreply.tail_buf_size());
  EXPECT_EQ(0, std::strncmp(rreply.tail_buf(), "target", 7));
}

// Read directories

TEST_F(MSWorkerTest, LookupClean) {
  FIM_PTR<LookupFim> fim(new LookupFim(10));
  fim->set_req_id(3);
  (*fim)->inode = 2;
  std::strncpy(fim->tail_buf(), "hello.txt", 10);
  EXPECT_CALL(*dirty_inode_mgr_, version())
      .WillOnce(Return(42));
  FileAttr* attr;
  EXPECT_CALL(*store_, Lookup(HasReqContext(&(*fim)->context), 2,
                              StrEq("hello.txt"), _, _))
      .WillOnce(DoAll(SaveArg<4>(&attr),
                      SetArgPointee<3>(InodeNum(7)),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(7, _))
      .WillOnce(DoAll(SetArgPointee<1>(42),
                      Return(false)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*cache_tracker_, SetCache(2, 10));
  EXPECT_CALL(*cache_tracker_, SetCache(7, 10));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);

  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(7U, rreply->inode);
  EXPECT_EQ(attr, &rreply->fa);
  EXPECT_EQ(0U, rreply->dirty);
}

TEST_F(MSWorkerTest, LookupDirty) {
  FIM_PTR<LookupFim> fim(new LookupFim(10));
  fim->set_req_id(3);
  (*fim)->inode = 2;
  std::strncpy(fim->tail_buf(), "hello.txt", 10);
  FileAttr target_attr;
  target_attr.mtime.sec = 1234567890;
  target_attr.mtime.ns = 12345;
  target_attr.size = 123456;
  EXPECT_CALL(*dirty_inode_mgr_, version())
      .WillOnce(Return(42));
  FileAttr* attr;
  EXPECT_CALL(*store_, Lookup(HasReqContext(&(*fim)->context), 2,
                              StrEq("hello.txt"), _, _))
      .WillOnce(DoAll(SetArgPointee<3>(InodeNum(7)),
                      SaveArg<4>(&attr),
                      SetArgPointee<4>(target_attr),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(7, _))
      .WillOnce(Return(true));
  AttrUpdateHandler handler;
  EXPECT_CALL(*attr_updater_, AsyncUpdateAttr(7, target_attr.mtime, 123456, _))
      .WillOnce(DoAll(SaveArg<3>(&handler),
                      Return(true)));

  worker_->Process(fim, fim_socket_);

  FIM_PTR<IFim> reply;
  EXPECT_CALL(*cache_tracker_, SetCache(2, 10));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  FSTime new_mtime = {1234567891ULL, 12346};
  handler(new_mtime, 123457);
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(7U, rreply->inode);
  EXPECT_EQ(new_mtime, rreply->fa.mtime);
  EXPECT_EQ(1U, rreply->dirty);
}

TEST_F(MSWorkerTest, LookupFalseClean) {
  FIM_PTR<LookupFim> fim(new LookupFim(10));
  fim->set_req_id(3);
  (*fim)->inode = 2;
  std::strncpy(fim->tail_buf(), "hello.txt", 10);
  EXPECT_CALL(*dirty_inode_mgr_, version())
      .WillOnce(Return(42));
  FileAttr* attr;
  EXPECT_CALL(*store_, Lookup(HasReqContext(&(*fim)->context), 2,
                              StrEq("hello.txt"), _, _))
      .WillOnce(DoAll(SetArgPointee<3>(InodeNum(7)),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(7, _))
      .WillOnce(DoAll(SetArgPointee<1>(43),
                      Return(false)));
  EXPECT_CALL(*store_, GetInodeAttr(7, _))
      .WillOnce(DoAll(SaveArg<1>(&attr),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*cache_tracker_, SetCache(2, 10));
  EXPECT_CALL(*cache_tracker_, SetCache(7, 10));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);

  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(7U, rreply->inode);
  EXPECT_EQ(attr, &rreply->fa);
  EXPECT_EQ(0U, rreply->dirty);
}

TEST_F(MSWorkerTest, Opendir) {
  FIM_PTR<OpendirFim> fim(new OpendirFim());
  fim->set_req_id(3);
  (*fim)->inode = 8;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*store_, Opendir(HasReqContext(&(*fim)->context), 8));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ReaddirSuccess) {
  FIM_PTR<ReaddirFim> fim(new ReaddirFim());
  fim->set_req_id(3);
  (*fim)->inode = 2;
  (*fim)->size = 1024;
  (*fim)->cookie = 12345;
  std::vector<char> data(1024);
  const char* ret = "Hello world!";
  std::size_t ret_size = std::strlen(ret) + 1;
  std::copy(ret, ret + ret_size, data.begin());
  EXPECT_CALL(*store_, Readdir(_, 2, 12345, _))
      .WillOnce(DoAll(SetArgPointee<3>(data),
                      Return(std::strlen(ret) + 1)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  DataReplyFim& rreply = dynamic_cast<DataReplyFim&>(*reply);
  EXPECT_EQ(ret_size, rreply.tail_buf_size());
  EXPECT_EQ(0, std::memcmp(ret, rreply.tail_buf(), ret_size));
}

TEST_F(MSWorkerTest, ReaddirFailure) {
  FIM_PTR<ReaddirFim> fim(new ReaddirFim());
  fim->set_req_id(3);
  (*fim)->inode = 2;
  (*fim)->size = 1024;
  (*fim)->cookie = 12345;
  EXPECT_CALL(*store_, Readdir(_, 2, 12345, _))
      .WillOnce(Return(-EACCES));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(EACCES), rreply->err_no);
}

TEST_F(MSWorkerTest, ReaddirHuge) {
  FIM_PTR<ReaddirFim> fim(new ReaddirFim());
  fim->set_req_id(3);
  (*fim)->inode = 2;
  (*fim)->size = 33 * 1024 * 1024;
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(EIO), rreply->err_no);
}

// Create directory entries
TEST_F(MSWorkerTest, Create) {
  FIM_PTR<CreateFim> fim(new CreateFim(10));
  fim->set_req_id((10ULL << 44) | 3ULL);
  (*fim)->inode = 2;
  (*fim)->req.new_inode = 0;
  (*fim)->req.flags = O_RDWR;
  (*fim)->num_ds_groups = 0;
  std::strncpy(fim->tail_buf(), "hello.txt", 10);
  FileAttr attr;
  std::memset(&attr, 0, sizeof(attr));
  std::vector<GroupId> groups;
  groups.push_back(5);
  RemovedInodeInfo* removed_info = new RemovedInodeInfo;
  removed_info->inode = 42;
  removed_info->to_free = true;
  removed_info->groups.reset(new std::vector<GroupId>);
  removed_info->groups->push_back(3);
  EXPECT_CALL(*inode_src_, Allocate(2, true))
      .WillOnce(Return(7));
  EXPECT_CALL(*store_, Create(HasReqContext(&(*fim)->context), 2,
                              StrEq("hello.txt"), &(*fim)->req, _, _, _))
      .WillOnce(DoAll(SetArgPointee<4>(groups),
                      SetArgPointee<5>(attr),
                      ResetSmartPointerArg<6>(removed_info),
                      Return(0)));
  EXPECT_CALL(*cache_tracker_, SetCache(2, 10));
  FIM_PTR<IFim> reply;
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));
  EXPECT_CALL(*inode_usage_, SetFCOpened(10U, 7U, kInodeWriteAccess));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(7U, true, true, 0));
  EXPECT_CALL(*cache_tracker_, InvGetClients(42, 10, _));
  EXPECT_CALL(inv_func_, Call(42, false));

  worker_->Process(fim, fim_socket_);
  EXPECT_EQ(fim->tail_buf_size(), 10 + sizeof(GroupId));
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(7U, rreply->inode);
  EXPECT_EQ(1U, rreply->dirty);
  EXPECT_EQ(7U, (*fim)->req.new_inode);
  EXPECT_EQ(0, std::memcmp(&attr, (&rreply->fa), sizeof(attr)));
  ASSERT_EQ(sizeof(GroupId), rreply.tail_buf_size());
  GroupId* groups_replied = reinterpret_cast<GroupId*>(rreply.tail_buf());
  EXPECT_EQ(5U, groups_replied[0]);

  DSFreeInodeMocks free_mocks(this, 3);

  active_complete_cb();
}

TEST_F(MSWorkerTest, CreateAllocated) {
  FIM_PTR<CreateFim> fim(new CreateFim(10 + 2 * sizeof(GroupId)));
  fim->set_req_id((10ULL << 44) | 3ULL);
  (*fim)->inode = 2;
  (*fim)->req.new_inode = 7;
  (*fim)->req.flags = O_RDWR;
  (*fim)->num_ds_groups = 2;
  std::strncpy(fim->tail_buf(), "hello.txt", 10);
  GroupId sent_groups[] = {4, 2};
  std::memcpy(fim->tail_buf() + 10, sent_groups, sizeof(sent_groups));
  FileAttr attr;
  std::memset(&attr, 0, sizeof(attr));
  RemovedInodeInfo* removed_info = new RemovedInodeInfo;
  removed_info->inode = 42;
  removed_info->to_free = true;
  removed_info->groups.reset(new std::vector<GroupId>);
  removed_info->groups->push_back(3);
  EXPECT_CALL(*store_, Create(HasReqContext(&(*fim)->context), 2,
                              StrEq("hello.txt"), &(*fim)->req,
                              Pointee(ElementsAre(4, 2)), _, _))
      .WillOnce(DoAll(SetArgPointee<5>(attr),
                      ResetSmartPointerArg<6>(removed_info),
                      Return(0)));
  EXPECT_CALL(*cache_tracker_, SetCache(2, 10));
  FIM_PTR<IFim> reply;
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));
  EXPECT_CALL(*inode_usage_, SetFCOpened(10U, 7U, kInodeWriteAccess));
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(7U, true, true, 0));
  EXPECT_CALL(*cache_tracker_, InvGetClients(42, 10, _));
  EXPECT_CALL(inv_func_, Call(42, false));

  worker_->Process(fim, fim_socket_);
  EXPECT_EQ(fim->tail_buf_size(), 10 + 2 * sizeof(GroupId));
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(0, std::memcmp(&attr, (&rreply->fa), sizeof(attr)));
  ASSERT_EQ(2 * sizeof(GroupId), rreply.tail_buf_size());
  GroupId* groups_replied = reinterpret_cast<GroupId*>(rreply.tail_buf());
  EXPECT_EQ(4U, groups_replied[0]);
  EXPECT_EQ(2U, groups_replied[1]);

  DSFreeInodeMocks free_mocks(this, 3);

  active_complete_cb();
}

TEST_F(MSWorkerTest, Mkdir) {
  FileAttr* attr;
  FIM_PTR<MkdirFim> fim(new MkdirFim(6));
  fim->set_req_id(3);
  (*fim)->req.new_inode = 0;
  (*fim)->parent = 1;
  std::strncpy(fim->tail_buf(), "testd", 6);
  EXPECT_CALL(*inode_src_, Allocate(1, false))
      .WillOnce(Return(7));
  EXPECT_CALL(*store_,
              Mkdir(HasReqContext(&(*fim)->context), 1, StrEq("testd"),
                    &(*fim)->req, _))
      .WillOnce(DoAll(SaveArg<4>(&attr), Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, true));

  worker_->Process(fim, fim_socket_);

  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(attr, &rreply->fa);
  EXPECT_EQ(7U, rreply->inode);
  EXPECT_EQ(0U, rreply->dirty);
  EXPECT_EQ(7U, (*fim)->req.new_inode);
}

TEST_F(MSWorkerTest, MkdirReconfirm) {
  FIM_PTR<MkdirFim> fim(new MkdirFim(6));
  fim->set_req_id(3);
  (*fim)->parent = 1;
  std::strncpy(fim->tail_buf(), "testd", 6);
  Mock::VerifyAndClear(recent_reply_set_);
  FIM_PTR<IFim> reply(new AttrReplyFim);
  EXPECT_CALL(*recent_reply_set_, FindReply(3))
      .WillOnce(Return(reply));
  EXPECT_CALL(*fim_socket_, WriteMsg(reply));

  worker_->Process(fim, fim_socket_);

  EXPECT_TRUE(reply->is_final());
}

TEST_F(MSWorkerTest, Symlink) {
  EXPECT_CALL(*inode_src_, Allocate(1, true))
      .WillOnce(Return(5));
  FileAttr* attr;
  FIM_PTR<SymlinkFim> fim(new SymlinkFim(6));
  EXPECT_CALL(*store_,
              Symlink(HasReqContext(&(*fim)->context), 1, StrEq("lnk"), 5,
                      StrEq("t"), _))
      .WillOnce(DoAll(SaveArg<5>(&attr),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, true));

  fim->set_req_id(3);
  (*fim)->inode = 1;
  (*fim)->new_inode = 0;
  (*fim)->name_len = 3;
  std::memcpy(fim->tail_buf(), "lnk\0t", 6);
  worker_->Process(fim, fim_socket_);
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(5U, rreply->inode);
  EXPECT_EQ(0U, rreply->dirty);
  EXPECT_EQ(5U, (*fim)->new_inode);
  EXPECT_EQ(attr, &rreply->fa);
}

TEST_F(MSWorkerTest, SymlinkFailure) {
  FIM_PTR<IFim> reply;
  FIM_PTR<SymlinkFim> fim(new SymlinkFim(6));
  fim->set_req_id(3);
  (*fim)->inode = 1;
  (*fim)->new_inode = 5;
  (*fim)->name_len = 3;
  std::memcpy(fim->tail_buf(), "lnkat", 6);
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(EIO), rreply->err_no);
}

TEST_F(MSWorkerTest, Mknod) {
  EXPECT_CALL(*inode_src_, Allocate(1, true))
      .WillOnce(Return(5));
  FIM_PTR<MknodFim> fim(new MknodFim(4));
  std::vector<GroupId> groups;
  groups.push_back(5);
  EXPECT_CALL(*store_,
              Mknod(HasReqContext(&(*fim)->context), 1, StrEq("nod"), 5, 6, 7,
                    _, _))
      .WillOnce(DoAll(SetArgPointee<6>(groups),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, true));

  fim->set_req_id(3);
  (*fim)->inode = 1;
  (*fim)->new_inode = 0;
  (*fim)->mode = 6;
  (*fim)->rdev = 7;
  (*fim)->num_ds_groups = 0;
  std::memcpy(fim->tail_buf(), "nod", 4);

  worker_->Process(fim, fim_socket_);
  EXPECT_EQ(4U + sizeof(GroupId), fim->tail_buf_size());
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(5U, rreply->inode);
  EXPECT_EQ(0U, rreply->dirty);
  EXPECT_EQ(5U, (*fim)->new_inode);
  EXPECT_EQ(sizeof(GroupId), rreply.tail_buf_size());
}

TEST_F(MSWorkerTest, MknodAllocated) {
  FIM_PTR<MknodFim> fim(new MknodFim(4 + sizeof(GroupId)));
  EXPECT_CALL(*store_,
              Mknod(HasReqContext(&(*fim)->context), 1, StrEq("nod"), 5, 6, 7,
                    Pointee(ElementsAre(8)), _))
      .WillOnce(Return(0));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*cache_tracker_, InvGetClients(1, 10, _));
  EXPECT_CALL(inv_func_, Call(1, true));

  fim->set_req_id(3);
  (*fim)->inode = 1;
  (*fim)->new_inode = 5;
  (*fim)->mode = 6;
  (*fim)->rdev = 7;
  (*fim)->num_ds_groups = 1;
  std::memcpy(fim->tail_buf(), "nod", 4);
  GroupId gid = 8;
  std::memcpy(fim->tail_buf() + 4, &gid, sizeof(GroupId));

  worker_->Process(fim, fim_socket_);
  EXPECT_EQ(4U + sizeof(GroupId), fim->tail_buf_size());
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(sizeof(GroupId), rreply.tail_buf_size());
}

// Manipulate directory entries

TEST_F(MSWorkerTest, Link) {
  FIM_PTR<LinkFim> fim(new LinkFim(6));
  fim->set_req_id(3);
  (*fim)->inode = 8;
  (*fim)->dir_inode = 3;
  std::memcpy(fim->tail_buf(), "lnkat", 6);
  FileAttr* attr;
  std::memset(&attr, 0, sizeof(attr));
  EXPECT_CALL(*store_, Link(HasReqContext(&(*fim)->context), 8, 3,
                            StrEq("lnkat"), _))
      .WillOnce(DoAll(SaveArg<4>(&attr),
                      Return(0)));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*dirty_inode_mgr_, IsVolatile(8, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_tracker_, InvGetClients(3, 10, _));
  EXPECT_CALL(*cache_tracker_, InvGetClients(8, 10, _));
  EXPECT_CALL(inv_func_, Call(8, false));
  EXPECT_CALL(inv_func_, Call(3, true));
  EXPECT_CALL(*cache_tracker_, SetCache(8, 10));

  worker_->Process(fim, fim_socket_);
  AttrReplyFim& rreply = dynamic_cast<AttrReplyFim&>(*reply);
  EXPECT_EQ(8U, rreply->inode);
  EXPECT_EQ(0U, rreply->dirty);
  EXPECT_EQ(attr, &rreply->fa);
}

TEST_F(MSWorkerTest, LinkFailure) {
  FIM_PTR<LinkFim> fim(new LinkFim(6));
  fim->set_req_id(3);
  (*fim)->inode = 8;
  (*fim)->dir_inode = 3;
  std::memcpy(fim->tail_buf(), "lnkat", 6);
  EXPECT_CALL(*store_, Link(HasReqContext(&(*fim)->context), 8, 3,
                            StrEq("lnkat"), _))
      .WillOnce(Return(-EACCES));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(EACCES), rreply->err_no);
}

TEST_F(MSWorkerTest, Rename) {
  FIM_PTR<RenameFim> fim(new RenameFim(8));
  fim->set_req_id(3);
  (*fim)->parent = 8;
  (*fim)->new_parent = 3;
  (*fim)->name_len = 3;
  FIM_PTR<IFim> reply;

  // Incorrect tail buffer
  std::memcpy(fim->tail_buf(), "thisisat", 8);
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(unsigned(EIO), rreply->err_no);

  // Normal case
  std::memcpy(fim->tail_buf(), "old\0new", 8);
  RemovedInodeInfo* removed_info = new RemovedInodeInfo;
  removed_info->inode = 42;
  removed_info->to_free = true;
  removed_info->groups.reset(new std::vector<GroupId>);
  removed_info->groups->push_back(3);
  EXPECT_CALL(*store_, Rename(HasReqContext(&(*fim)->context), 8,
                              StrEq("old"), 3, StrEq("new"), _, _))
      .WillOnce(DoAll(SetArgPointee<5>(15),
                      ResetSmartPointerArg<6>(removed_info),
                      Return(0)));
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));
  EXPECT_CALL(*cache_tracker_, InvGetClients(42, 10, _));
  EXPECT_CALL(*cache_tracker_, InvGetClients(15, 10, _));
  EXPECT_CALL(*cache_tracker_, InvGetClients(3, 10, _));
  EXPECT_CALL(*cache_tracker_, InvGetClients(8, 10, _));
  EXPECT_CALL(inv_func_, Call(42, false));
  EXPECT_CALL(inv_func_, Call(15, false));
  EXPECT_CALL(inv_func_, Call(8, true));
  EXPECT_CALL(inv_func_, Call(3, true));

  fim->set_req_id(4);
  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply2 = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply2->err_no);

  DSFreeInodeMocks free_mocks(this, 3);

  active_complete_cb();
}

// Remove directory entries

TEST_F(MSWorkerTest, Unlink) {
  FIM_PTR<UnlinkFim> fim(new UnlinkFim(10));
  fim->set_req_id(3);
  (*fim)->inode = 2;
  std::strncpy(fim->tail_buf(), "hello.txt", 10);
  RemovedInodeInfo* removed_info = new RemovedInodeInfo;
  removed_info->inode = 42;
  removed_info->to_free = true;
  removed_info->groups.reset(new std::vector<GroupId>);
  removed_info->groups->push_back(3);
  EXPECT_CALL(*store_, Unlink(HasReqContext(&(*fim)->context), 2,
                              StrEq("hello.txt"), _))
      .WillOnce(DoAll(ResetSmartPointerArg<3>(removed_info),
                      Return(0)));
  FIM_PTR<IFim> reply;
  boost::function<void()> active_complete_cb;
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(DoAll(SaveArg<1>(&reply),
                      SaveArg<4>(&active_complete_cb)));
  EXPECT_CALL(*cache_tracker_, InvGetClients(42, 10, _));
  EXPECT_CALL(*cache_tracker_, InvGetClients(2, 10, _));
  EXPECT_CALL(inv_func_, Call(42, false));
  EXPECT_CALL(inv_func_, Call(2, true));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);

  DSFreeInodeMocks free_mocks(this, 3);

  active_complete_cb();
  for (GroupRole r = 0; r < kNumDSPerGroup; ++r) {
    FreeDataFim& rreq =
        dynamic_cast<FreeDataFim&>(*free_mocks.entry[r]->request());
    EXPECT_EQ(42U, rreq->inode);
  }
}

TEST_F(MSWorkerTest, Rmdir) {
  FIM_PTR<RmdirFim> fim(new RmdirFim(4));
  fim->set_req_id(3);
  (*fim)->parent = 8;
  FIM_PTR<IFim> reply;
  std::memcpy(fim->tail_buf(), "dir", 4);
  EXPECT_CALL(*store_, Rmdir(HasReqContext(&(*fim)->context), 8,
                             StrEq("dir")));
  EXPECT_CALL(*replier_,
              DoReply(boost::static_pointer_cast<IFim>(fim), _,
                      boost::static_pointer_cast<IFimSocket>(fim_socket_), _,
                      _))
      .WillOnce(SaveArg<1>(&reply));
  EXPECT_CALL(*cache_tracker_, InvGetClients(8, 10, _));
  EXPECT_CALL(inv_func_, Call(8, true));

  worker_->Process(fim, fim_socket_);
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ReconfirmEnd) {
  FIM_PTR<ReconfirmEndFim> fim = ReconfirmEndFim::MakePtr();
  EXPECT_CALL(*failover_mgr_, AddReconfirmDone(10));

  worker_->Process(fim, fim_socket_);
}

TEST_F(MSWorkerTest, ResyncXattr) {
  FIM_PTR<ResyncXattrFim> fim(new ResyncXattrFim(4));
  std::memcpy(fim->tail_buf(), "0-3", 4);
  std::memcpy((*fim)->name, "swait", 6);
  EXPECT_CALL(*startup_mgr_, Reset(StrEq("0-3")));
  EXPECT_CALL(*fim_socket_, WriteMsg(_));

  worker_->Process(fim, fim_socket_);
}

TEST_F(MSWorkerTest, ResyncInode) {
  FIM_PTR<ResyncInodeFim> fim(new ResyncInodeFim(8));
  (*fim)->inode = InodeNum(5);
  (*fim)->extra_size = 8;
  FileAttr attr;
  std::memset(&attr, 0, sizeof(attr));
  (*fim)->fa = attr;
  std::memcpy(fim->tail_buf(), "../link", 7);
  fim->tail_buf()[7] = '\0';
  EXPECT_CALL(*store_, ResyncInode(InodeNum(5), _, StrEq("../link"), 8, 8));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply));

  worker_->Process(fim, fim_socket_);
  Sleep(0.01)();
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ResyncDentry) {
  FIM_PTR<ResyncDentryFim> fim(new ResyncDentryFim(10));
  (*fim)->parent = InodeNum(1);
  (*fim)->target = InodeNum(5);
  (*fim)->type = 'S';
  std::memcpy(fim->tail_buf(), "test", 5);
  EXPECT_CALL(*store_, ResyncDentry(_, _, InodeNum(1), InodeNum(5), 'S',
                                    StrEq("test")));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply));

  worker_->Process(fim, fim_socket_);
  Sleep(0.01)();
  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ResyncRemoval) {
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*store_, ResyncRemoval(5));
  EXPECT_CALL(*fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply));

  FIM_PTR<ResyncRemovalFim> fim = ResyncRemovalFim::MakePtr();
  (*fim)->inode = 5U;
  worker_->Process(fim, fim_socket_);
  Sleep(0.01)();

  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ResyncInodeUsagePrep) {
  EXPECT_CALL(*dirty_inode_mgr_, Reset(false));
  std::vector<InodeNum> last_used;
  EXPECT_CALL(*inode_src_, SetLastUsed(_)).WillOnce(SaveArg<0>(&last_used));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply));

  InodeNum to_set[] = {42U, 12UL << 52};
  FIM_PTR<ResyncInodeUsagePrepFim> fim
      = ResyncInodeUsagePrepFim::MakePtr(sizeof(to_set));
  memcpy(fim->tail_buf(), to_set, sizeof(to_set));
  worker_->Process(fim, fim_socket_);
  Sleep(0.01)();

  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
  EXPECT_EQ(42U, last_used[0]);
  EXPECT_EQ(12UL << 52, last_used[1]);
}

TEST_F(MSWorkerTest, ResyncDirtyInode) {
  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(42, false, false, _));
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply));

  FIM_PTR<ResyncDirtyInodeFim> fim = ResyncDirtyInodeFim::MakePtr();
  (*fim)->inode = 42;
  worker_->Process(fim, fim_socket_);
  Sleep(0.01)();

  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ResyncPendingUnlink) {
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply));

  InodeNum inodes[] = {10, 3, 501};
  boost::unordered_set<InodeNum> inodes_set;
  EXPECT_CALL(*inode_usage_, ClearPendingUnlink());
  EXPECT_CALL(*inode_usage_, AddPendingUnlinks(_))
      .WillOnce(SaveArg<0>(&inodes_set));
  FIM_PTR<ResyncPendingUnlinkFim> fim
      = ResyncPendingUnlinkFim::MakePtr(sizeof(inodes));
  (*fim)->first = (*fim)->last = 1;
  std::memcpy(fim->tail_buf(), inodes, sizeof(inodes));
  worker_->Process(fim, fim_socket_);
  Sleep(0.01)();

  EXPECT_EQ(3U, inodes_set.size());
  EXPECT_TRUE(inodes_set.find(10) != inodes_set.end());
  EXPECT_TRUE(inodes_set.find(3) != inodes_set.end());
  EXPECT_TRUE(inodes_set.find(501) != inodes_set.end());

  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, ResyncClientOpened) {
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket_, WriteMsg(_)).WillOnce(SaveArg<0>(&reply));

  EXPECT_CALL(*dirty_inode_mgr_, SetVolatile(1, true, true, _));
  InodeNum inodes[] = {1, 7};
  InodeAccessMap access_map;
  EXPECT_CALL(*inode_usage_, SwapClientOpened(2014, _))
      .WillOnce(SaveArgPointee<1>(&access_map));

  FIM_PTR<ResyncClientOpenedFim> fim
      = ResyncClientOpenedFim::MakePtr(sizeof(inodes));
  (*fim)->client_num = 2014;
  (*fim)->num_writable = 1;
  std::memcpy(fim->tail_buf(), inodes, sizeof(inodes));
  worker_->Process(fim, fim_socket_);
  Sleep(0.01)();
  EXPECT_EQ(2U, access_map.size());
  EXPECT_NE(access_map.end(), access_map.find(1));
  EXPECT_NE(access_map.end(), access_map.find(7));
  EXPECT_TRUE(access_map[1]);
  EXPECT_FALSE(access_map[7]);

  ResultCodeReplyFim& rreply = dynamic_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(0U, rreply->err_no);
}

TEST_F(MSWorkerTest, InodeCleanAttr) {
  FileAttr fa;
  fa.mtime.sec = 123456789;
  fa.mtime.ns = 12345;
  fa.size = 1234;
  EXPECT_CALL(*store_, GetInodeAttr(42, _))
      .WillOnce(DoAll(SetArgPointee<1>(fa),
                      Return(0)));
  EXPECT_CALL(*dirty_inode_mgr_, StartCleaning(42))
      .WillOnce(Return(true));
  EXPECT_CALL(*attr_updater_, AsyncUpdateAttr(42, fa.mtime, 1234, _))
      .WillOnce(Return(true));

  FIM_PTR<InodeCleanAttrFim> fim = InodeCleanAttrFim::MakePtr();
  (*fim)->inode = 42;
  (*fim)->gen = 63;
  worker_->Process(fim, fim_socket_);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
