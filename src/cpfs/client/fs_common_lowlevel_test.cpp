/* Copyright 2013 ClusterTech Ltd */
#include "client/fs_common_lowlevel.hpp"

#include <errno.h>
#include <fcntl.h>
#include <time.h>

#include <fuse/fuse_lowlevel.h>

#include <cstring>
#include <vector>

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "ds_iface.hpp"
#include "dsg_state.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "mock_actions.hpp"
#include "mutex_util.hpp"
#include "req_completion_mock.hpp"
#include "req_entry_mock.hpp"
#include "req_limiter_mock.hpp"
#include "req_tracker_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "client/base_client.hpp"
#include "client/cache_mock.hpp"
#include "client/file_handle.hpp"
#include "client/inode_usage_mock.hpp"

namespace cpfs {
namespace client {
namespace {

using testing::_;
using testing::DoAll;
using testing::InSequence;
using testing::InvokeArgument;
using testing::Mock;
using testing::SaveArg;
using testing::Return;
using testing::ReturnPointee;
using testing::ReturnRef;

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
 * @param req_entry The request entry to use
 *
 * @param callback The location to save the ack callback added to
 * the returning entry.  If 0, do not expect ack callback to be set
 *
 * @return The request entry.  This entry should not be reset before
 * the request tracker is cleared with Mock::VerifyAndClear
 */
void PrepareAddRequest(
    boost::shared_ptr<MockIReqTracker> tracker,
    FIM_PTR<IFim>* req_fim,
    boost::shared_ptr<MockIReqEntry> req_entry,
    const FIM_PTR<IFim>& reply_fim = FIM_PTR<IFim>(),
    ReqAckCallback* callback = 0) {
  static const FIM_PTR<IFim> empty_ifim;
  if (req_fim) {
    EXPECT_CALL(*tracker, AddRequest(_, _))
        .WillOnce(DoAll(SaveArg<0>(req_fim),
                        Return(req_entry)));
  } else {
    EXPECT_CALL(*tracker, AddRequest(_, _))
        .WillOnce(Return(req_entry));
  }
  if (callback)
    EXPECT_CALL(*req_entry, OnAck(_, false))
        .WillOnce(SaveArg<0>(callback));
  if (reply_fim)
    EXPECT_CALL(*req_entry, WaitReply())
        .WillOnce(ReturnRef(reply_fim ? reply_fim : empty_ifim));
}

class FSCommonLLTest : public ::testing::Test {
 protected:
  // Convenience objects, go first for correct cleanup order
  boost::shared_ptr<MockIReqEntry> req_entry1_;
  boost::shared_ptr<MockIReqEntry> req_entry2_;

  boost::shared_ptr<MockIReqTracker> ms_req_tracker_;
  boost::shared_ptr<MockIReqTracker> ds_req_tracker1_;
  boost::scoped_ptr<MockIReqLimiter> req_limiter1_;
  boost::shared_ptr<MockIReqTracker> ds_req_tracker2_;
  boost::scoped_ptr<MockIReqLimiter> req_limiter2_;
  boost::shared_ptr<MockIReqCompletionChecker> comp_checker_;

  // The client
  MockICacheMgr* cache_mgr_;
  MockIReqCompletionCheckerSet* req_completion_checker_set_;
  MockIInodeUsageSet* inode_usage_set_;
  MockITrackerMapper* tracker_mapper_;
  BaseFSClient client_;

  // The lowlevel object to test
  boost::scoped_ptr<FSCommonLL> ll_;

  FSCommonLLTest()
      : req_entry1_(new MockIReqEntry),
        req_entry2_(new MockIReqEntry),
        ms_req_tracker_(new MockIReqTracker),
        ds_req_tracker1_(new MockIReqTracker),
        req_limiter1_(new MockIReqLimiter),
        ds_req_tracker2_(new MockIReqTracker),
        req_limiter2_(new MockIReqLimiter),
        comp_checker_(new MockIReqCompletionChecker),
        ll_(new FSCommonLL()) {
    // Setup ll object
    client_.set_cache_mgr(cache_mgr_ = new MockICacheMgr);
    client_.set_inode_usage_set(inode_usage_set_ = new MockIInodeUsageSet);
    client_.set_req_completion_checker_set(
        req_completion_checker_set_ = new MockIReqCompletionCheckerSet);
    client_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    ll_->SetClient(&client_);

    EXPECT_CALL(*tracker_mapper_, GetMSTracker())
        .WillRepeatedly(Return(ms_req_tracker_));
    EXPECT_CALL(*ds_req_tracker1_, GetReqLimiter())
        .WillRepeatedly(Return(req_limiter1_.get()));
    EXPECT_CALL(*ds_req_tracker2_, GetReqLimiter())
        .WillRepeatedly(Return(req_limiter2_.get()));
  }

  virtual void TearDown() {
    Mock::VerifyAndClear(tracker_mapper_);
  }
};

FIM_PTR<DataReplyFim> MakeGroupDataReply() {
  FIM_PTR<DataReplyFim> ret = DataReplyFim::MakePtr(2 * sizeof(GroupId));
  GroupId* grp_ptr = reinterpret_cast<GroupId*>(ret->tail_buf());
  grp_ptr[0] = 2;
  grp_ptr[1] = 4;
  return ret;
}

FIM_PTR<ResultCodeReplyFim> MakeErrorReply(int err_no) {
  FIM_PTR<ResultCodeReplyFim> ret(new ResultCodeReplyFim);
  (*ret)->err_no = err_no;
  return ret;
}

TEST_F(FSCommonLLTest, Open) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim = MakeGroupDataReply();
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry1_, reply_fim);
  EXPECT_CALL(*inode_usage_set_, UpdateOpened(3, true, _));

  FSOpenReply res;
  EXPECT_TRUE(ll_->Open(FSIdentity(12345, 67890), 3, O_RDWR, &res));
  OpenFim& rfim = dynamic_cast<OpenFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(O_RDWR, rfim->flags);

  DeleteFH(res.fh);
}

TEST_F(FSCommonLLTest, OpenTruncate) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim = MakeGroupDataReply();
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry1_, reply_fim);
  EXPECT_CALL(*inode_usage_set_, UpdateOpened(3, true, _));
  EXPECT_CALL(*cache_mgr_, InvalidateInode(3, true));

  FSOpenReply res;
  EXPECT_TRUE(ll_->Open(FSIdentity(12345, 67890), 3, O_RDWR | O_TRUNC, &res));
  OpenFim& rfim = dynamic_cast<OpenFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(O_RDWR | O_TRUNC, rfim->flags);

  DeleteFH(res.fh);
}

TEST_F(FSCommonLLTest, OpenError) {
  InSequence dummy;
  EXPECT_CALL(*inode_usage_set_, UpdateOpened(3, false, _));
  FIM_PTR<IFim> err_fim = MakeErrorReply(EACCES);
  PrepareAddRequest(ms_req_tracker_, 0, req_entry1_, err_fim);
  EXPECT_CALL(*inode_usage_set_, UpdateClosed(3, false, _, _))
      .WillOnce(Return(1));
  FIM_PTR<IFim> req_fim;
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry2_);

  FSOpenReply res;
  EXPECT_FALSE(ll_->Open(FSIdentity(12345, 67890), 3, O_RDONLY, &res));
  EXPECT_EQ(kReleaseFim, req_fim->type());
  ReleaseFim& r_req = dynamic_cast<ReleaseFim&>(*req_fim);
  EXPECT_EQ(1, r_req->keep_read);

  Mock::VerifyAndClear(req_entry1_.get());
}

TEST_F(FSCommonLLTest, CreateSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr(sizeof(GroupId));
  FIM_PTR<IFim> reply_ifim = reply_fim;
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  (*reply_fim)->inode = 5;
  GroupId group = 42;
  std::memcpy(reply_fim->tail_buf(), &group, sizeof(group));
  ReqAckCallback callback;
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry1_, reply_fim,
                    &callback);
  EXPECT_CALL(*cache_mgr_, AddEntry(1, "hello", false));
  EXPECT_CALL(*inode_usage_set_, UpdateOpened(5, false, _));

  FSCreateReply res;
  EXPECT_TRUE(ll_->Create(FSIdentity(12345, 67890), 1, "hello", 0775, O_RDONLY,
                          &res));
  CreateFim& rfim = dynamic_cast<CreateFim&>(*req_fim);
  EXPECT_EQ(1U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(5U, FHFileCoordManager(res.fh)->inode());
  EXPECT_EQ(5U, res.inode);
  EXPECT_EQ(1234U, res.attr.st_mode);
  EXPECT_EQ(34567U, res.attr.st_gid);

  // Callback sets the new inode number and group list back to request
  // asynchronously
  EXPECT_CALL(*req_entry1_, request())
      .WillOnce(ReturnPointee(&req_fim));
  EXPECT_CALL(*(req_entry1_), reply())
      .WillOnce(ReturnRef(reply_ifim));

  callback(req_entry1_);
  EXPECT_EQ(5U, rfim->req.new_inode);
  EXPECT_EQ(6 + sizeof(GroupId), rfim.tail_buf_size());
  EXPECT_EQ(1U, rfim->num_ds_groups);

  DeleteFH(res.fh);
}

TEST_F(FSCommonLLTest, CreateError) {
  ReqAckCallback callback;
  FIM_PTR<IFim> err_fim = MakeErrorReply(EACCES);
  PrepareAddRequest(ms_req_tracker_, 0, req_entry1_, err_fim, &callback);
  EXPECT_CALL(*req_entry1_, reply())
      .WillOnce(ReturnRef(err_fim));

  FSCreateReply res;
  EXPECT_FALSE(ll_->Create(FSIdentity(12345, 67890), 1, "hello", 0775,
                           O_RDONLY, &res));
  callback(req_entry1_);
}

TEST_F(FSCommonLLTest, LookupSuccess) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->inode = 5;
  (*reply_fim)->dirty = 0;
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry1_, reply_fim);

  boost::shared_ptr<MockICacheInvRecord> inv_rec =
      boost::make_shared<MockICacheInvRecord>();
  EXPECT_CALL(*cache_mgr_, StartLookup())
      .WillOnce(Return(inv_rec));
  MUTEX_DECL(mutex);
  EXPECT_CALL(*inv_rec, GetMutex())
      .WillOnce(Return(&mutex));
  EXPECT_CALL(*inv_rec, InodeInvalidated(1, true))
      .WillOnce(Return(false));
  EXPECT_CALL(*inv_rec, InodeInvalidated(5, false))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_mgr_, AddEntry(1, "hello", true));

  FSLookupReply res;
  EXPECT_TRUE(ll_->Lookup(FSIdentity(12345, 67890), 1, "hello", &res));
  LookupFim& rfim = dynamic_cast<LookupFim&>(*req_fim);
  EXPECT_EQ(1U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(5U, res.inode);
  EXPECT_EQ(1234U, res.attr.st_mode);
  EXPECT_EQ(34567U, res.attr.st_gid);
  EXPECT_EQ(3600.0, res.attr_timeout);

  Mock::VerifyAndClear(cache_mgr_);
}

TEST_F(FSCommonLLTest, LookupError) {
  boost::shared_ptr<MockICacheInvRecord> inv_rec =
      boost::make_shared<MockICacheInvRecord>();
  EXPECT_CALL(*cache_mgr_, StartLookup())
      .WillOnce(Return(inv_rec));
  FIM_PTR<IFim> err_fim = MakeErrorReply(EACCES);
  PrepareAddRequest(ms_req_tracker_, 0, req_entry1_, err_fim);

  FSLookupReply res;
  EXPECT_FALSE(ll_->Lookup(FSIdentity(12345, 67890), 1, "hello", &res));

  Mock::VerifyAndClear(cache_mgr_);
}

FIM_PTR<DataReplyFim> MakeFilledDataReply(unsigned size) {
  FIM_PTR<DataReplyFim> ret(new DataReplyFim(size));
  for (unsigned i = 0; i < size; ++i)
    ret->tail_buf()[i] = char(i / 8);
  return ret;
}

struct ReadInfo {
  FSReadReply res;
  uint64_t fh;
  boost::scoped_array<char> buf;
  ReadInfo(InodeNum inode, GroupId* groups, int num_groups, int size) {
    fh = MakeFH(inode, groups, num_groups);
    buf.reset(new char[size]);
    res.buf = buf.get();
  }
  ~ReadInfo() {
    DeleteFH(fh);
  }
};

TEST_F(FSCommonLLTest, ReadOneSegment) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim = MakeFilledDataReply(1024);
  InodeNum inode = kNumDSPerGroup;
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(7, 1))
      .WillOnce(Return(ds_req_tracker1_));
  PrepareAddRequest(ds_req_tracker1_, &req_fim, req_entry1_, reply_fim);

  GroupId groups[] = { 7 };
  ReadInfo info(inode, groups, 1, 1024);
  EXPECT_TRUE(ll_->Read(info.fh, inode, 1024, 32768, &info.res));
  ReadFim& rfim = dynamic_cast<ReadFim&>(*req_fim);
  EXPECT_EQ(32768U, rfim->dsg_off);
  EXPECT_EQ(1024U, rfim->size);
  EXPECT_EQ(0, memcmp(reply_fim->tail_buf(), info.buf.get(), 1024));
  EXPECT_EQ(4U, rfim->checksum_role);
}

TEST_F(FSCommonLLTest, ReadTwoSegments) {
  FIM_PTR<IFim> req_fim1;
  FIM_PTR<IFim> req_fim2;
  InodeNum inode = kNumDSPerGroup;
  FIM_PTR<DataReplyFim> reply_fim1 = MakeFilledDataReply(512);
  FIM_PTR<DataReplyFim> reply_fim2 = MakeFilledDataReply(512);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 0))
      .WillOnce(Return(ds_req_tracker1_));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 1))
      .WillOnce(Return(ds_req_tracker2_));
  PrepareAddRequest(ds_req_tracker1_, &req_fim1, req_entry1_, reply_fim1);
  PrepareAddRequest(ds_req_tracker2_, &req_fim2, req_entry2_, reply_fim2);

  GroupId groups[] = { 0 };
  ReadInfo info(inode, groups, 1, 1024);
  EXPECT_TRUE(ll_->Read(info.fh, inode, 1024, 32768 - 512, &info.res));
  ReadFim& rfim1 = dynamic_cast<ReadFim&>(*req_fim1);
  EXPECT_EQ(32768U - 512U, rfim1->dsg_off);
  EXPECT_EQ(512U, rfim1->size);
  ReadFim& rfim2 = dynamic_cast<ReadFim&>(*req_fim2);
  EXPECT_EQ(32768U, rfim2->dsg_off);
  EXPECT_EQ(512U, rfim2->size);
  EXPECT_EQ(0, memcmp(reply_fim1->tail_buf(), info.buf.get(), 512));
  EXPECT_EQ(0, memcmp(reply_fim2->tail_buf(), info.buf.get() + 512, 512));
}

TEST_F(FSCommonLLTest, ReadDegraded) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim = MakeFilledDataReply(1024);
  InodeNum inode = kNumDSPerGroup;
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(7, 4))
      .WillOnce(Return(ds_req_tracker1_));
  PrepareAddRequest(ds_req_tracker1_, &req_fim, req_entry1_, reply_fim);

  client_.set_dsg_state(7, kDSGDegraded, 1);
  GroupId groups[] = { 7 };
  ReadInfo info(inode, groups, 1, 1024);
  EXPECT_TRUE(ll_->Read(info.fh, inode, 1024, 32768, &info.res));
  ReadFim& rfim = dynamic_cast<ReadFim&>(*req_fim);
  EXPECT_EQ(32768U, rfim->dsg_off);
  EXPECT_EQ(1024U, rfim->size);
  EXPECT_EQ(0, memcmp(reply_fim->tail_buf(), info.buf.get(), 1024));
  EXPECT_EQ(4U, rfim->checksum_role);
}

TEST_F(FSCommonLLTest, ReadError) {
  FIM_PTR<IFim> err_fim = MakeErrorReply(EACCES);
  PrepareAddRequest(ds_req_tracker1_, 0, req_entry1_, err_fim);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(7, _))
      .WillOnce(Return(ds_req_tracker1_));

  GroupId groups[] = { 7 };
  ReadInfo info(3, groups, 1, 1024);
  EXPECT_FALSE(ll_->Read(info.fh, 3, 1024, 32768, &info.res));
}

TEST_F(FSCommonLLTest, ReadvOneSegment) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim = MakeFilledDataReply(1024);
  InodeNum inode = kNumDSPerGroup;
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(7, 1))
      .WillOnce(Return(ds_req_tracker1_));
  PrepareAddRequest(ds_req_tracker1_, &req_fim, req_entry1_, reply_fim);

  GroupId groups[] = { 7 };
  FSReadvReply reply;
  uint64_t fh = MakeFH(inode, groups, 1);
  EXPECT_TRUE(ll_->Readv(fh, inode, 1024, 32768, &reply));
  DeleteFH(fh);
  ReadFim& rfim = dynamic_cast<ReadFim&>(*req_fim);
  EXPECT_EQ(32768U, rfim->dsg_off);
  EXPECT_EQ(1024U, rfim->size);
  EXPECT_EQ(1024U, reply.iov[0].iov_len);
  EXPECT_EQ(0, memcmp(reply_fim->tail_buf(), reply.iov[0].iov_base, 1024));
  EXPECT_EQ(4U, rfim->checksum_role);
}

TEST_F(FSCommonLLTest, ReadvTwoSegments) {
  FIM_PTR<IFim> req_fim1;
  FIM_PTR<IFim> req_fim2;
  InodeNum inode = kNumDSPerGroup;
  FIM_PTR<DataReplyFim> reply_fim1 = MakeFilledDataReply(512);
  FIM_PTR<DataReplyFim> reply_fim2 = MakeFilledDataReply(512);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 0))
      .WillOnce(Return(ds_req_tracker1_));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 1))
      .WillOnce(Return(ds_req_tracker2_));
  PrepareAddRequest(ds_req_tracker1_, &req_fim1, req_entry1_, reply_fim1);
  PrepareAddRequest(ds_req_tracker2_, &req_fim2, req_entry2_, reply_fim2);

  GroupId groups[] = { 0 };
  uint64_t fh = MakeFH(inode, groups, 1);
  FSReadvReply reply;
  EXPECT_TRUE(ll_->Readv(fh, inode, 1024, 32768 - 512, &reply));
  DeleteFH(fh);
  ReadFim& rfim1 = dynamic_cast<ReadFim&>(*req_fim1);
  EXPECT_EQ(32768U - 512U, rfim1->dsg_off);
  EXPECT_EQ(512U, rfim1->size);
  ReadFim& rfim2 = dynamic_cast<ReadFim&>(*req_fim2);
  EXPECT_EQ(32768U, rfim2->dsg_off);
  EXPECT_EQ(512U, rfim2->size);
  EXPECT_EQ(512U, reply.iov[0].iov_len);
  EXPECT_EQ(0, memcmp(reply_fim1->tail_buf(), reply.iov[0].iov_base, 512));
  EXPECT_EQ(512U, reply.iov[1].iov_len);
  EXPECT_EQ(0, memcmp(reply_fim2->tail_buf(), reply.iov[1].iov_base, 512));
}

TEST_F(FSCommonLLTest, ReadvDegraded) {
  FIM_PTR<IFim> req_fim;
  FIM_PTR<DataReplyFim> reply_fim = MakeFilledDataReply(1024);
  InodeNum inode = kNumDSPerGroup;
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(7, 4))
      .WillOnce(Return(ds_req_tracker1_));
  PrepareAddRequest(ds_req_tracker1_, &req_fim, req_entry1_, reply_fim);

  client_.set_dsg_state(7, kDSGDegraded, 1);
  GroupId groups[] = { 7 };
  uint64_t fh = MakeFH(inode, groups, 1);
  FSReadvReply reply;
  EXPECT_TRUE(ll_->Readv(fh, inode, 1024, 32768, &reply));
  DeleteFH(fh);
  ReadFim& rfim = dynamic_cast<ReadFim&>(*req_fim);
  EXPECT_EQ(32768U, rfim->dsg_off);
  EXPECT_EQ(1024U, rfim->size);
  EXPECT_EQ(1024U, reply.iov[0].iov_len);
  EXPECT_EQ(0, memcmp(reply_fim->tail_buf(), reply.iov[0].iov_base, 1024));
  EXPECT_EQ(4U, rfim->checksum_role);
}

TEST_F(FSCommonLLTest, ReadvError) {
  FIM_PTR<IFim> err_fim = MakeErrorReply(EACCES);
  PrepareAddRequest(ds_req_tracker1_, 0, req_entry1_, err_fim);
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(7, _))
      .WillOnce(Return(ds_req_tracker1_));

  GroupId groups[] = { 7 };
  uint64_t fh = MakeFH(3, groups, 1);
  FSReadvReply res;
  EXPECT_FALSE(ll_->Readv(fh, 3, 1024, 32768, &res));
  DeleteFH(fh);
}

void Increment(int* val) {
  ++*val;
}

TEST_F(FSCommonLLTest, WriteOneSegment) {
  EXPECT_CALL(*req_completion_checker_set_, Get(5))
      .WillOnce(Return(comp_checker_));
  EXPECT_CALL(*inode_usage_set_, SetDirty(5));
  InodeNum inode = kNumDSPerGroup;
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(7, 1))
      .WillOnce(Return(ds_req_tracker1_));
  boost::shared_ptr<IReqEntry> entry;
  EXPECT_CALL(*req_limiter1_, Send(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry),
                      Return(true)));
  EXPECT_CALL(*comp_checker_, RegisterOp(_));

  GroupId groups[] = { 7 };
  uint64_t fh = MakeFH(inode, groups, 1);
  char buf[1024];
  for (unsigned i = 0; i < 1024; ++i)
    buf[i] = char(i / 8);
  FSWriteReply res;
  EXPECT_TRUE(ll_->Write(fh, inode, buf, 1024, 32768, &res));
  WriteFim& rfim = dynamic_cast<WriteFim&>(*entry->request());
  EXPECT_EQ(0, std::memcmp(rfim.tail_buf(), buf, 1024));
  EXPECT_EQ(1024U, rfim.tail_buf_size());
  EXPECT_EQ(32768U, rfim->dsg_off);
  EXPECT_EQ(32768U + 1024U, rfim->last_off);

  // Check callback calling
  EXPECT_CALL(*req_completion_checker_set_, CompleteOp(5, _));

  FIM_PTR<ResultCodeReplyFim> reply = ResultCodeReplyFim::MakePtr();
  (*reply)->err_no = ENOSPC;
  reply->set_final();
  entry->SetReply(reply, 1);
  Sleep(0.01)();
  EXPECT_EQ(ENOSPC, FHGetErrno(fh, false));

  DeleteFH(fh);
}

TEST_F(FSCommonLLTest, WriteTwoSegments) {
  boost::shared_ptr<MockIReqCompletionChecker> checker(
      new MockIReqCompletionChecker);
  EXPECT_CALL(*req_completion_checker_set_, Get(5))
      .WillOnce(Return(checker));
  EXPECT_CALL(*inode_usage_set_, SetDirty(5));
  InodeNum inode = kNumDSPerGroup;
  FIM_PTR<IFim> req_fim1;
  FIM_PTR<IFim> req_fim2;
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 0))
      .WillOnce(Return(ds_req_tracker1_));
  EXPECT_CALL(*tracker_mapper_, GetDSTracker(0, 1))
      .WillOnce(Return(ds_req_tracker2_));
  boost::shared_ptr<IReqEntry> entry1;
  EXPECT_CALL(*req_limiter1_, Send(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry1),
                      Return(true)));
  boost::shared_ptr<IReqEntry> entry2;
  EXPECT_CALL(*req_limiter2_, Send(_, _))
      .WillOnce(DoAll(SaveArg<0>(&entry2),
                      Return(true)));
  EXPECT_CALL(*checker, RegisterOp(_))
      .Times(2);

  GroupId groups[] = { 0 };
  uint64_t fh = MakeFH(inode, groups, 1);
  char buf[1024];
  for (unsigned i = 0; i < 1024; ++i)
    buf[i] = char(i / 8);
  FSWriteReply res;
  EXPECT_TRUE(ll_->Write(fh, inode, buf, 1024, 32768 - 512, &res));
  DeleteFH(fh);
  WriteFim& rfim1 = dynamic_cast<WriteFim&>(*entry1->request());
  EXPECT_EQ(0, std::memcmp(rfim1.tail_buf(), buf, 512));
  EXPECT_EQ(512U, rfim1.tail_buf_size());
  EXPECT_EQ(32768U - 512U, rfim1->dsg_off);
  EXPECT_EQ(32768U + 512U, rfim1->last_off);
  WriteFim& rfim2 = dynamic_cast<WriteFim&>(*entry2->request());
  EXPECT_EQ(0, std::memcmp(rfim2.tail_buf(), buf + 512, 512));
  EXPECT_EQ(512U, rfim2.tail_buf_size());
  EXPECT_EQ(32768U, rfim2->dsg_off);
  EXPECT_EQ(32768U + 512U, rfim2->last_off);
  EXPECT_EQ(rfim1->optime, rfim2->optime);

  Mock::VerifyAndClear(req_completion_checker_set_);
}

TEST_F(FSCommonLLTest, WriteDeferredErr) {
  GroupId groups[] = { 7 };
  uint64_t fh = MakeFH(42, groups, 1);
  FHSetErrno(fh, ENOSPC);
  char buf[1024];
  FSWriteReply res;
  EXPECT_FALSE(ll_->Write(fh, 42, buf, 1024, 32768, &res));

  DeleteFH(fh);
}

TEST_F(FSCommonLLTest, ReleaseReadOnly) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  EXPECT_CALL(*inode_usage_set_, UpdateClosed(3, false, _, _))
      .WillOnce(Return(kClientAccessUnchanged));

  uint64_t fh = MakeFH(3, 0, 0);
  ll_->Release(3, &fh, O_RDONLY);
}

TEST_F(FSCommonLLTest, ReleaseReadWrite) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  EXPECT_CALL(*ms_req_tracker_, AddRequest(_, _))
      .WillOnce(DoAll(SaveArg<0>(&req_fim),
                      Return(req_entry1_)));
  EXPECT_CALL(*inode_usage_set_, UpdateClosed(3, true, _, _))
      .WillOnce(Return(0));

  uint64_t fh = MakeFH(3, 0, 0);
  ll_->Release(3, &fh, O_RDWR);
  ReleaseFim& rreq = dynamic_cast<ReleaseFim&>(*req_fim);
  EXPECT_EQ(3U, rreq->inode);
  EXPECT_EQ(0, rreq->keep_read);
}

TEST_F(FSCommonLLTest, GetattrSuccess) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(1, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  ReqAckCallback callback;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  (*reply_fim)->dirty = false;
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry1_, reply_fim);

  struct stat stbuf;
  FSGetattrReply res;
  res.stbuf = &stbuf;
  EXPECT_TRUE(ll_->Getattr(FSIdentity(12345, 67890), 1, &res));
  GetattrFim& rfim = dynamic_cast<GetattrFim&>(*req_fim);
  EXPECT_EQ(1U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(1U, stbuf.st_ino);
  EXPECT_EQ(1234U, stbuf.st_mode);
  EXPECT_EQ(34567U, stbuf.st_gid);
}

TEST_F(FSCommonLLTest, GetattrError) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(1, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> err_fim = MakeErrorReply(ENOENT);
  PrepareAddRequest(ms_req_tracker_, 0, req_entry1_, err_fim);

  struct stat stbuf;
  FSGetattrReply res;
  res.stbuf = &stbuf;
  EXPECT_FALSE(ll_->Getattr(FSIdentity(12345, 67890), 1, &res));
}

TEST_F(FSCommonLLTest, SetattrSuccess) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = S_IFDIR | 01666;
  (*reply_fim)->fa.gid = 34567;
  (*reply_fim)->dirty = false;
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry1_, reply_fim);
  EXPECT_CALL(*inode_usage_set_, StartLockedSetattr(3, _))
      .WillOnce(Return(false));
  EXPECT_CALL(*cache_mgr_, InvalidateInode(3, true));

  struct stat attr;
  attr.st_mode = 01775;
  attr.st_uid = 20;
  attr.st_gid = 30;
  attr.st_size = 4096;
  FSSetattrReply res;
  EXPECT_TRUE(ll_->Setattr(FSIdentity(12345, 67890), 3, &attr,
                           FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_UID |
                           FUSE_SET_ATTR_GID | FUSE_SET_ATTR_SIZE |
                           FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW,
                           &res));
  SetattrFim& rfim = dynamic_cast<SetattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(01775U, rfim->fa.mode);
  EXPECT_EQ(20U, rfim->fa.uid);
  EXPECT_EQ(30U, rfim->fa.gid);
  EXPECT_EQ(4096U, rfim->fa.size);
  EXPECT_EQ(rfim->fa.mtime, rfim->fa.atime);
  EXPECT_EQ(unsigned(FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_UID |
                     FUSE_SET_ATTR_GID | FUSE_SET_ATTR_SIZE |
                     FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME |
                     FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW),
            rfim->fa_mask);
  EXPECT_EQ(3U, res.stbuf.st_ino);
  EXPECT_EQ(S_IFDIR | 01666u, res.stbuf.st_mode);
  EXPECT_EQ(34567U, res.stbuf.st_gid);
}

TEST_F(FSCommonLLTest, SetattrSuccess2) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> req_fim;
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->fa.mode = 1234;
  (*reply_fim)->fa.gid = 34567;
  (*reply_fim)->dirty = true;
  PrepareAddRequest(ms_req_tracker_, &req_fim, req_entry1_, reply_fim);
  EXPECT_CALL(*inode_usage_set_, StartLockedSetattr(3, _))
      .WillOnce(Return(false));

  struct stat attr;
  attr.st_atim.tv_sec = 1234;
  attr.st_atim.tv_nsec = 5678;
  attr.st_mtim.tv_sec = 2345;
  attr.st_mtim.tv_nsec = 6789;
  FSSetattrReply res;
  EXPECT_TRUE(ll_->Setattr(FSIdentity(12345, 67890), 3, &attr,
                           FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME, &res));
  SetattrFim& rfim = dynamic_cast<SetattrFim&>(*req_fim);
  EXPECT_EQ(3U, rfim->inode);
  EXPECT_EQ(12345U, rfim->context.uid);
  EXPECT_EQ(1234U, rfim->fa.atime.sec);
  EXPECT_EQ(5678U, rfim->fa.atime.ns);
  EXPECT_EQ(2345U, rfim->fa.mtime.sec);
  EXPECT_EQ(6789U, rfim->fa.mtime.ns);
  EXPECT_EQ(unsigned(FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME), rfim->fa_mask);
  EXPECT_EQ(3U, res.stbuf.st_ino);
}

TEST_F(FSCommonLLTest, SetattrError) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());
  FIM_PTR<IFim> err_fim = MakeErrorReply(EPERM);
  PrepareAddRequest(ms_req_tracker_, 0, req_entry1_, err_fim);

  FSSetattrReply res;
  struct stat attr;
  EXPECT_FALSE(ll_->Setattr(FSIdentity(12345, 67890), 3, &attr, 0, &res));
}

TEST_F(FSCommonLLTest, Flush) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());

  ll_->Flush(3);
}

TEST_F(FSCommonLLTest, Fsync) {
  EXPECT_CALL(*req_completion_checker_set_, OnCompleteAll(3, _))
      .WillOnce(InvokeArgument<1>());

  ll_->Fsync(3);
}

TEST_F(FSCommonLLTest, CreateReplyCallback) {
  // Setup a req_entry to always say the reply is a valid one
  boost::shared_ptr<MockIReqEntry> req_entry(new MockIReqEntry);
  FIM_PTR<AttrReplyFim> reply_fim = AttrReplyFim::MakePtr();
  (*reply_fim)->inode = 42;
  reply_fim->tail_buf_resize(sizeof(GroupId));
  *reinterpret_cast<GroupId*>(reply_fim->tail_buf()) = 3;
  FIM_PTR<IFim> reply_ifim = reply_fim;
  EXPECT_CALL(*req_entry, reply())
      .WillRepeatedly(ReturnRef(reply_ifim));

  // Try mkdir
  FIM_PTR<MkdirFim> mkdir_req_fim = MkdirFim::MakePtr();
  EXPECT_CALL(*req_entry, request())
      .WillOnce(Return(mkdir_req_fim));

  ll_->CreateReplyCallback(req_entry);
  EXPECT_EQ(42U, (*mkdir_req_fim)->req.new_inode);
  EXPECT_EQ(0U, mkdir_req_fim->tail_buf_size());

  // Try symlink
  FIM_PTR<SymlinkFim> symlink_req_fim = SymlinkFim::MakePtr();
  EXPECT_CALL(*req_entry, request())
      .WillOnce(Return(symlink_req_fim));

  ll_->CreateReplyCallback(req_entry);
  EXPECT_EQ(42U, (*symlink_req_fim)->new_inode);
  EXPECT_EQ(0U, symlink_req_fim->tail_buf_size());

  // Try mknod
  FIM_PTR<MknodFim> mknod_req_fim = MknodFim::MakePtr();
  EXPECT_CALL(*req_entry, request())
      .WillOnce(Return(mknod_req_fim));

  ll_->CreateReplyCallback(req_entry);
  EXPECT_EQ(42U, (*mknod_req_fim)->new_inode);
  EXPECT_EQ(1U, (*mknod_req_fim)->num_ds_groups);
  EXPECT_EQ(sizeof(GroupId), mknod_req_fim->tail_buf_size());
  EXPECT_EQ(3U, *reinterpret_cast<GroupId*>(mknod_req_fim->tail_buf()));
}

}  // namespace
}  // namespace client
}  // namespace cpfs
