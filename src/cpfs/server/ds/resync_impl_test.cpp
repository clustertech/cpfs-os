/* Copyright 2013 ClusterTech Ltd */
#include "server/ds/resync_impl.hpp"

#include <stdint.h>

#include <sys/stat.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "common.hpp"
#include "dir_iterator_mock.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "posix_fs_mock.hpp"
#include "req_entry.hpp"
#include "req_tracker_mock.hpp"
#include "shaped_sender.hpp"
#include "shaped_sender_mock.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ds/base_ds_mock.hpp"
#include "server/ds/resync_mock.hpp"
#include "server/ds/store_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;
using ::testing::StartsWith;
using ::testing::Throw;
using ::testing::WithArg;

namespace cpfs {
namespace server {
namespace ds {
namespace {

class DSResyncTest : public ::testing::Test {
 protected:
  MockShapedSenderMaker s_sender_maker_;
  MockResyncSenderMaker sender_maker_;
  MockBaseDataServer server_;
  boost::scoped_ptr<IResyncSender> resync_sender_;
  boost::scoped_ptr<IResyncMgr> resync_mgr_;
  MockIPosixFS* posix_fs_;
  MockIInodeRemovalTracker* inode_removal_tracker_;
  MockIDurableRange* durable_range_;
  MockIStore* store_;
  MockFunction<void(bool success)> completion_handler_;
  boost::scoped_ptr<IResyncFimProcessor> resync_fim_processor_;
  MockITrackerMapper* tracker_mapper_;
  MockITimeKeeper* dsg_ready_time_keeper_;
  boost::shared_ptr<MockIReqTracker> ds_tracker_;

  DSResyncTest()
      : resync_sender_(kResyncSenderMaker(&server_, 1)),
        resync_mgr_(MakeResyncMgr(&server_)),
        store_(new MockIStore),
        resync_fim_processor_(MakeResyncFimProcessor(&server_)),
        ds_tracker_(new MockIReqTracker) {
    resync_sender_->SetShapedSenderMaker(s_sender_maker_.GetMaker());
    resync_mgr_->SetResyncSenderMaker(sender_maker_.GetMaker());
    resync_mgr_->SetShapedSenderMaker(s_sender_maker_.GetMaker());
    server_.set_posix_fs(posix_fs_ = new MockIPosixFS);
    server_.set_inode_removal_tracker(
        inode_removal_tracker_ = new MockIInodeRemovalTracker);
    server_.set_durable_range(durable_range_ = new MockIDurableRange);
    server_.set_store(store_);
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    server_.set_dsg_ready_time_keeper(
        dsg_ready_time_keeper_ = new MockITimeKeeper);
    EXPECT_CALL(*store_, ds_group())
        .WillRepeatedly(Return(3));
    EXPECT_CALL(*tracker_mapper_, GetDSTracker(3, 1))
        .WillRepeatedly(Return(ds_tracker_));
  }

  ~DSResyncTest() {
    Mock::VerifyAndClear(tracker_mapper_);
  }
};

ACTION_P(SetStatbufCtime, n) {
  arg0->st_ctime = n;
}

TEST_F(DSResyncTest, ResyncSenderEmpty) {
  // SendDirFims
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, List())
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("r"),
                      SetArgPointee<1>(true),
                      Return(true)))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<DSResyncListReplyFim> reply = DSResyncListReplyFim::MakePtr();
  (*reply)->num_inode = 0;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));
  // SendDataRemoval
  MockIShapedSender* shaped_sender_2 = new MockIShapedSender;
  MockIShapedSender* shaped_sender_3 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 32768))
      .WillOnce(Return(shaped_sender_2))
      .WillOnce(Return(shaped_sender_3));
  EXPECT_CALL(*shaped_sender_2, WaitAllReplied());
  // SendAllResync
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(std::vector<InodeNum>()));
  FIM_PTR<IFim> req;
  EXPECT_CALL(*shaped_sender_3, SendFim(_))
      .WillOnce(SaveArg<0>(&req));
  EXPECT_CALL(*shaped_sender_3, WaitAllReplied());

  resync_sender_->Run();
  EXPECT_EQ(kDSResyncDataEndFim, req->type());

  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderRemove) {
  // SendDirFims
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, List())
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<DSResyncListReplyFim> reply = DSResyncListReplyFim::MakePtr();
  (*reply)->num_inode = 0;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));
  // SendDataRemoval
  MockIShapedSender* shaped_sender_2 = new MockIShapedSender;
  MockIShapedSender* shaped_sender_3 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 32768))
      .WillOnce(Return(shaped_sender_2))
      .WillOnce(Return(shaped_sender_3));
  std::vector<InodeNum> removed;
  removed.push_back(100);
  removed.push_back(200);
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(removed));
  FIM_PTR<IFim> req1;
  EXPECT_CALL(*shaped_sender_2, SendFim(_))
      .WillOnce(SaveArg<0>(&req1));
  EXPECT_CALL(*shaped_sender_2, WaitAllReplied());
  // SendAllResync
  FIM_PTR<IFim> req2;
  EXPECT_CALL(*shaped_sender_3, SendFim(_))
      .WillOnce(SaveArg<0>(&req2));
  EXPECT_CALL(*shaped_sender_3, WaitAllReplied());

  resync_sender_->Run();
  EXPECT_EQ(kDSResyncRemovalFim, req1->type());
  ASSERT_EQ(2U * sizeof(InodeNum), req1->tail_buf_size());
  const InodeNum* to_remove = reinterpret_cast<const InodeNum*>(
      req1->tail_buf());
  EXPECT_EQ(100U, to_remove[0]);
  EXPECT_EQ(200U, to_remove[1]);
  EXPECT_EQ(kDSResyncDataEndFim, req2->type());

  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderEmptyFile) {
  // SendDirFims
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, List())
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("0000005.d"),
                      SetArgPointee<1>(false),
                      Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>("0000005.c"),
                      SetArgPointee<1>(false),
                      Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>("000000x.d"),
                      SetArgPointee<1>(false),
                      Return(true)))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  FIM_PTR<IFim> req1;
  EXPECT_CALL(*shaped_sender_1, SendFim(_))
      .WillOnce(SaveArg<0>(&req1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<DSResyncListReplyFim> reply =
      DSResyncListReplyFim::MakePtr(sizeof(InodeNum));
  (*reply)->num_inode = 1;
  InodeNum* buf = reinterpret_cast<InodeNum*>(reply->tail_buf());
  buf[0] = 5;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));
  // SendDataRemoval
  MockIShapedSender* shaped_sender_2 = new MockIShapedSender;
  MockIShapedSender* shaped_sender_3 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 32768))
      .WillOnce(Return(shaped_sender_2))
      .WillOnce(Return(shaped_sender_3));
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(std::vector<InodeNum>()));
  EXPECT_CALL(*shaped_sender_2, WaitAllReplied());
  // SendAllResync
  MockIChecksumGroupIterator* cg_iter = new MockIChecksumGroupIterator;
  EXPECT_CALL(*store_, GetChecksumGroupIterator(5))
      .WillOnce(Return(cg_iter));
  FSTime mtime = {1234567890ULL, 987654321};
  EXPECT_CALL(*cg_iter, GetInfo(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mtime),
                      SetArgPointee<1>(10)));
  EXPECT_CALL(*cg_iter, GetNext(_, _))
      .WillOnce(Return(0));
  FIM_PTR<IFim> info_fim;
  EXPECT_CALL(*shaped_sender_3, SendFim(_))
      .WillOnce(SaveArg<0>(&info_fim))
      .WillOnce(DoDefault());
  EXPECT_CALL(*shaped_sender_3, WaitAllReplied());

  resync_sender_->Run();
  ASSERT_EQ(kDSResyncInfoFim, info_fim->type());
  DSResyncInfoFim& rinfo_fim = static_cast<DSResyncInfoFim&>(*info_fim);
  EXPECT_EQ(mtime, rinfo_fim->mtime);
  EXPECT_EQ(10U, rinfo_fim->size);

  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderOneSmallFile) {
  // SendDirFims
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, List())
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("0000005.d"),
                      SetArgPointee<1>(false),
                      Return(true)))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  FIM_PTR<IFim> req1;
  EXPECT_CALL(*shaped_sender_1, SendFim(_))
      .WillOnce(SaveArg<0>(&req1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<DSResyncListReplyFim> reply = DSResyncListReplyFim::MakePtr(
      sizeof(InodeNum));
  (*reply)->num_inode = 1;
  InodeNum* buf = reinterpret_cast<InodeNum*>(reply->tail_buf());
  buf[0] = 5;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));
  // SendDataRemoval
  MockIShapedSender* shaped_sender_2 = new MockIShapedSender;
  MockIShapedSender* shaped_sender_3 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 32768))
      .WillOnce(Return(shaped_sender_2))
      .WillOnce(Return(shaped_sender_3));
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(std::vector<InodeNum>()));
  EXPECT_CALL(*shaped_sender_2, WaitAllReplied());
  // SendAllResync
  MockIChecksumGroupIterator* cg_iter = new MockIChecksumGroupIterator;
  EXPECT_CALL(*store_, GetChecksumGroupIterator(5))
      .WillOnce(Return(cg_iter));
  char file_data[5];
  std::memset(file_data, 42, 5);
  FSTime mtime = {1234567890ULL, 987654321};
  EXPECT_CALL(*cg_iter, GetInfo(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mtime),
                      SetArgPointee<1>(10)));
  EXPECT_CALL(*cg_iter, GetNext(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(kSegmentSize),
                      SetArrayArgument<1>(file_data, file_data + 5),
                      Return(5)))
      .WillOnce(Return(0));
  FIM_PTR<IFim> req2;
  EXPECT_CALL(*shaped_sender_3, SendFim(_))
      .WillOnce(DoDefault())
      .WillOnce(SaveArg<0>(&req1))
      .WillOnce(SaveArg<0>(&req2));
  EXPECT_CALL(*shaped_sender_3, WaitAllReplied());

  resync_sender_->Run();
  DSResyncFim& req = static_cast<DSResyncFim&>(*req1);
  EXPECT_EQ(kSegmentSize, req->cg_off);
  EXPECT_EQ(5U, req.tail_buf_size());

  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderTwoFiles) {
  // SendDirFims
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, List())
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("0000005.d"),
                      SetArgPointee<1>(false),
                      Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>("000000A.d"),
                      SetArgPointee<1>(false),
                      Return(true)))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  FIM_PTR<IFim> req1;
  EXPECT_CALL(*shaped_sender_1, SendFim(_))
      .WillOnce(SaveArg<0>(&req1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<DSResyncListReplyFim> reply =
      DSResyncListReplyFim::MakePtr(2 * sizeof(InodeNum));
  (*reply)->num_inode = 2;
  InodeNum* buf = reinterpret_cast<InodeNum*>(reply->tail_buf());
  buf[0] = 5;
  buf[1] = 10;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));
  // SendDataRemoval
  MockIShapedSender* shaped_sender_2 = new MockIShapedSender;
  MockIShapedSender* shaped_sender_3 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 32768))
      .WillOnce(Return(shaped_sender_2))
      .WillOnce(Return(shaped_sender_3));
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(std::vector<InodeNum>()));
  EXPECT_CALL(*shaped_sender_2, WaitAllReplied());
  // SendAllResync
  MockIChecksumGroupIterator* cg_iter1 = new MockIChecksumGroupIterator;
  EXPECT_CALL(*store_, GetChecksumGroupIterator(5))
      .WillOnce(Return(cg_iter1));
  FSTime mtime = {1234567890ULL, 987654321};
  EXPECT_CALL(*cg_iter1, GetInfo(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mtime),
                      SetArgPointee<1>(10)));
  char file_data[kSegmentSize];
  std::memset(file_data, 42, kSegmentSize);
  EXPECT_CALL(*cg_iter1, GetNext(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(0),
                      SetArrayArgument<1>(file_data, file_data + kSegmentSize),
                      Return(kSegmentSize)))
      .WillOnce(Return(0));
  MockIChecksumGroupIterator* cg_iter2 = new MockIChecksumGroupIterator;
  EXPECT_CALL(*cg_iter2, GetInfo(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mtime),
                      SetArgPointee<1>(10)));
  EXPECT_CALL(*store_, GetChecksumGroupIterator(0xa))
      .WillOnce(Return(cg_iter2));
  EXPECT_CALL(*cg_iter2, GetNext(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(0),
                      SetArrayArgument<1>(file_data, file_data + 5),
                      Return(5)))
      .WillOnce(Return(0));
  EXPECT_CALL(*shaped_sender_3, SendFim(_))
      .Times(5);
  EXPECT_CALL(*shaped_sender_3, WaitAllReplied());

  resync_sender_->Run();

  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderOptResync) {
  // SendDirFims
  EXPECT_CALL(*dsg_ready_time_keeper_, GetLastUpdate())
      .WillOnce(Return(123456789));
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_CALL(*durable_range_, Get())
      .WillOnce(Return(ranges));
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, InodeList(ranges))
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, SetFilterCTime(123456669));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("000/000000A.d"),
                      SetArgPointee<1>(false),
                      WithArg<2>(SetStatbufCtime(123456789)),
                      Return(true)))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  FIM_PTR<IFim> req1;
  EXPECT_CALL(*shaped_sender_1, SendFim(_))
      .WillOnce(SaveArg<0>(&req1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<DSResyncListReplyFim> reply =
      DSResyncListReplyFim::MakePtr(sizeof(InodeNum));
  (*reply)->num_inode = 1;
  InodeNum* buf = reinterpret_cast<InodeNum*>(reply->tail_buf());
  buf[0] = 5;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));
  // SendDataRemoval
  MockIShapedSender* shaped_sender_2 = new MockIShapedSender;
  MockIShapedSender* shaped_sender_3 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 32768))
      .WillOnce(Return(shaped_sender_2))
      .WillOnce(Return(shaped_sender_3));
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(std::vector<InodeNum>()));
  EXPECT_CALL(*shaped_sender_2, WaitAllReplied());
  // SendAllResync
  MockIChecksumGroupIterator* cg_iter = new MockIChecksumGroupIterator;
  EXPECT_CALL(*store_, GetChecksumGroupIterator(5))
      .WillOnce(Return(cg_iter));
  FSTime mtime = {1234567890ULL, 987654321};
  EXPECT_CALL(*cg_iter, GetInfo(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mtime),
                      SetArgPointee<1>(10)));
  EXPECT_CALL(*cg_iter, GetNext(_, _))
      .WillOnce(Return(0));
  EXPECT_CALL(*shaped_sender_3, SendFim(_))
      .Times(2);
  EXPECT_CALL(*shaped_sender_3, WaitAllReplied());

  server_.set_opt_resync(true);
  resync_sender_->Run();
  DSResyncDirFim& dreq1 = dynamic_cast<DSResyncDirFim&>(*req1);
  EXPECT_EQ(sizeof(InodeNum), dreq1.tail_buf_size());
  InodeNum* rec1 = reinterpret_cast<InodeNum*>(dreq1.tail_buf());
  EXPECT_EQ(0xAU, rec1[0]);
  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderOptResyncPlainList) {
  // SendDirFims
  EXPECT_CALL(*dsg_ready_time_keeper_, GetLastUpdate())
      .WillOnce(Return(0));
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, List())
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("000/x"),
                      SetArgPointee<1>(true),
                      WithArg<2>(SetStatbufCtime(123456789)),
                      Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>("000/000000A.d"),
                      SetArgPointee<1>(false),
                      WithArg<2>(SetStatbufCtime(123456789)),
                      Return(true)))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  FIM_PTR<IFim> req1;
  EXPECT_CALL(*shaped_sender_1, SendFim(_))
      .WillOnce(SaveArg<0>(&req1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<DSResyncListReplyFim> reply =
      DSResyncListReplyFim::MakePtr(sizeof(InodeNum));
  (*reply)->num_inode = 1;
  InodeNum* buf = reinterpret_cast<InodeNum*>(reply->tail_buf());
  buf[0] = 5;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));
  // SendDataRemoval
  MockIShapedSender* shaped_sender_2 = new MockIShapedSender;
  MockIShapedSender* shaped_sender_3 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 32768))
      .WillOnce(Return(shaped_sender_2))
      .WillOnce(Return(shaped_sender_3));
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(std::vector<InodeNum>()));
  EXPECT_CALL(*shaped_sender_2, WaitAllReplied());
  // SendAllResync
  MockIChecksumGroupIterator* cg_iter = new MockIChecksumGroupIterator;
  EXPECT_CALL(*store_, GetChecksumGroupIterator(5))
      .WillOnce(Return(cg_iter));
  FSTime mtime = {1234567890ULL, 987654321};
  EXPECT_CALL(*cg_iter, GetInfo(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mtime),
                      SetArgPointee<1>(10)));
  EXPECT_CALL(*cg_iter, GetNext(_, _))
      .WillOnce(Return(0));
  EXPECT_CALL(*shaped_sender_3, SendFim(_))
      .Times(2);
  EXPECT_CALL(*shaped_sender_3, WaitAllReplied());

  server_.set_opt_resync(true);
  resync_sender_->Run();
  DSResyncDirFim& dreq1 = dynamic_cast<DSResyncDirFim&>(*req1);
  EXPECT_EQ(sizeof(InodeNum), dreq1.tail_buf_size());
  InodeNum* rec1 = reinterpret_cast<InodeNum*>(dreq1.tail_buf());
  EXPECT_EQ(0xAU, rec1[0]);
  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderOptResyncError1) {
  // SendDirFims
  EXPECT_CALL(*dsg_ready_time_keeper_, GetLastUpdate())
      .WillOnce(Return(123456789));
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_CALL(*durable_range_, Get())
      .WillOnce(Return(ranges));
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, InodeList(ranges))
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, SetFilterCTime(123456669));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(Return(false));

  server_.set_opt_resync(true);
  EXPECT_THROW(resync_sender_->Run(), std::runtime_error);

  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncSenderOptResyncError2) {
  // SendDirFims
  EXPECT_CALL(*dsg_ready_time_keeper_, GetLastUpdate())
      .WillOnce(Return(123456789));
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_CALL(*durable_range_, Get())
      .WillOnce(Return(ranges));
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, InodeList(ranges))
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, SetFilterCTime(123456669));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(Return(false));
  MockIShapedSender* shaped_sender_1 = new MockIShapedSender;
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 262144))
      .WillOnce(Return(shaped_sender_1));
  EXPECT_CALL(*shaped_sender_1, WaitAllReplied());
  // ReadResyncList
  FIM_PTR<ResultCodeReplyFim> reply =
      ResultCodeReplyFim::MakePtr();
  (*reply)->err_no = 0;
  EXPECT_CALL(*ds_tracker_, AddRequestEntry(_, _))
      .WillOnce(DoAll(Invoke(boost::bind(&IReqEntry::SetReply, _1, reply, 1)),
                      Return(true)));

  server_.set_opt_resync(true);
  EXPECT_THROW(resync_sender_->Run(), std::runtime_error);

  Mock::VerifyAndClear(store_);
}

TEST_F(DSResyncTest, ResyncMgr) {
  // Normal run
  MockIResyncSender* resync_sender = new MockIResyncSender;
  EXPECT_CALL(sender_maker_, Make(&server_, 1))
      .WillOnce(Return(resync_sender));
  ShapedSenderMaker s_sender_maker;
  EXPECT_CALL(*resync_sender, SetShapedSenderMaker(_))
      .WillOnce(SaveArg<0>(&s_sender_maker));
  EXPECT_CALL(*resync_sender, Run());
  resync_mgr_->Start(1U);
  resync_mgr_->Start(2U);  // Ignored
  GroupRole target;
  EXPECT_TRUE(resync_mgr_->IsStarted(&target));
  EXPECT_EQ(1U, target);
  Sleep(0.05)();
  EXPECT_FALSE(resync_mgr_->IsStarted(&target));

  // The expected shaped sender maker is used
  EXPECT_CALL(s_sender_maker_, Make(
      boost::static_pointer_cast<IReqTracker>(ds_tracker_), 10))
      .WillOnce(Return(reinterpret_cast<IShapedSender*>(0)));

  EXPECT_FALSE(s_sender_maker(ds_tracker_, 10));

  // Normal exceptional run
  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());

  resync_sender = new MockIResyncSender;
  EXPECT_CALL(sender_maker_, Make(&server_, 1))
      .WillOnce(Return(resync_sender));
  EXPECT_CALL(*resync_sender, SetShapedSenderMaker(_));
  EXPECT_CALL(*resync_sender, Run())
      .WillOnce(Throw(std::runtime_error("An error")));
  EXPECT_CALL(callback, Call(
      PLEVEL(error, Degraded),
      StartsWith("Exception thrown when sending resync to peer DS 1: "), _));

  resync_mgr_->Start(1U);
  Sleep(0.05)();
  EXPECT_FALSE(resync_mgr_->IsStarted(&target));

  // Ill-formed exception
  resync_sender = new MockIResyncSender;
  EXPECT_CALL(sender_maker_, Make(&server_, 1))
      .WillOnce(Return(resync_sender));
  EXPECT_CALL(*resync_sender, SetShapedSenderMaker(_));
  EXPECT_CALL(*resync_sender, Run())
      .WillOnce(Throw(10));
  EXPECT_CALL(callback, Call(
      PLEVEL(error, Degraded),
      StartsWith("Unknown exception thrown when sending resync to peer"), _));

  resync_mgr_->Start(1U);
  Sleep(0.05)();
  EXPECT_FALSE(resync_mgr_->IsStarted(&target));
}

TEST_F(DSResyncTest, ResyncFimProcessorDisabled) {
  // Handling of DSResyncFim
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  FIM_PTR<DSResyncFim> rfim = DSResyncFim::MakePtr(5);
  rfim->set_req_id(1);
  (*rfim)->inode = 42;
  (*rfim)->cg_off = kSegmentSize;
  std::memcpy(rfim->tail_buf(), "hello", 5);
  resync_fim_processor_->Process(rfim, fim_socket);
  ResultCodeReplyFim& rreply = static_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(EINVAL, int(rreply->err_no));

  // Handling of DSResyncDataEndFim
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
  efim->set_req_id(2);
  resync_fim_processor_->Process(efim, fim_socket);
  ResultCodeReplyFim& ereply = static_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(EINVAL, int(ereply->err_no));

  // Handling of DSResyncDirFim
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  FIM_PTR<DSResyncDirFim> dfim =
      DSResyncDirFim::MakePtr(sizeof(InodeNum));
  dfim->set_req_id(3);
  reinterpret_cast<InodeNum&>(*dfim->tail_buf()) = 2;
  resync_fim_processor_->Process(dfim, fim_socket);
  ResultCodeReplyFim& dreply = static_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(EINVAL, int(dreply->err_no));

  // Handling of DSResyncListFim
  EXPECT_CALL(*fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  FIM_PTR<DSResyncListFim> lfim = DSResyncListFim::MakePtr();
  lfim->set_req_id(4);
  (*lfim)->start_idx = 0;
  (*lfim)->max_reply = 1024;
  resync_fim_processor_->Process(lfim, fim_socket);
  ResultCodeReplyFim& lreply = static_cast<ResultCodeReplyFim&>(*reply);
  EXPECT_EQ(EINVAL, int(lreply->err_no));
}

TEST_F(DSResyncTest, ResyncFimProcessorNoResyncFim) {
  EXPECT_CALL(*store_, RemoveAll());

  resync_fim_processor_->AsyncResync(boost::bind(
      GetMockCall(completion_handler_), &completion_handler_, _1));

  boost::shared_ptr<MockIFimSocket> fim_sockets[kNumDSPerGroup - 1];
  FIM_PTR<IFim> fims[kNumDSPerGroup - 1];
  uint64_t req_id = 0;

  for (unsigned i = 0; i < kNumDSPerGroup - 2; ++i) {
    fim_sockets[i].reset(new MockIFimSocket);
    EXPECT_CALL(*fim_sockets[i], WriteMsg(_))
        .WillOnce(SaveArg<0>(fims + i));

    FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
    efim->set_req_id(++req_id);
    resync_fim_processor_->Process(efim, fim_sockets[i]);
  }

  EXPECT_CALL(*posix_fs_, Sync());
  EXPECT_CALL(completion_handler_, Call(true));

  fim_sockets[kNumDSPerGroup - 2].reset(new MockIFimSocket);
  EXPECT_CALL(*fim_sockets[kNumDSPerGroup - 2], WriteMsg(_))
      .WillOnce(SaveArg<0>(fims + kNumDSPerGroup - 2));
  resync_fim_processor_->Process(DSResyncDataEndFim::MakePtr(),
                                 fim_sockets[kNumDSPerGroup - 2]);
  for (unsigned i = 0; i < kNumDSPerGroup - 1; ++i) {
    ResultCodeReplyFim& fim = static_cast<ResultCodeReplyFim&>(*fims[i]);
    EXPECT_EQ(0U, fim->err_no);
  }
}

TEST_F(DSResyncTest, ResyncFimProcessorRemoval) {
  boost::shared_ptr<MockIFimSocket> fim_socket(new MockIFimSocket);
  EXPECT_CALL(*store_, FreeData(100, true));
  EXPECT_CALL(*store_, FreeData(300, true));
  EXPECT_CALL(*fim_socket, WriteMsg(_));

  FIM_PTR<DSResyncRemovalFim> ifim = DSResyncRemovalFim::MakePtr(
      2 * sizeof(InodeNum));
  InodeNum* to_remove = reinterpret_cast<InodeNum*>(ifim->tail_buf());
  to_remove[0] = 100;
  to_remove[1] = 300;
  resync_fim_processor_->Process(ifim, fim_socket);
}

TEST_F(DSResyncTest, ResyncFimProcessorEarlyResyncEnd) {
  EXPECT_CALL(*store_, RemoveAll());

  resync_fim_processor_->AsyncResync(boost::bind(
      GetMockCall(completion_handler_), &completion_handler_, _1));

  boost::shared_ptr<MockIFimSocket> fim_sockets[kNumDSPerGroup - 1];
  uint64_t req_id = 0;
  FSTime mtime = {1234567890ULL, 987654321};
  for (unsigned i = 0; i < kNumDSPerGroup - 2; ++i) {
    fim_sockets[i].reset(new MockIFimSocket);

    if (i == 1) {  // The only Fims actually received
      EXPECT_CALL(*fim_sockets[i], WriteMsg(_)).Times(2);

      FIM_PTR<DSResyncInfoFim> ifim = DSResyncInfoFim::MakePtr();
      (*ifim)->inode = 5;
      (*ifim)->mtime = mtime;
      (*ifim)->size = 42;
      resync_fim_processor_->Process(ifim, fim_sockets[i]);
      FIM_PTR<DSResyncFim> rfim = DSResyncFim::MakePtr(5);
      rfim->set_req_id(++req_id);
      (*rfim)->inode = 42;
      (*rfim)->cg_off = kSegmentSize;
      std::memcpy(rfim->tail_buf(), "hello", 5);
      resync_fim_processor_->Process(rfim, fim_sockets[i]);
    } else {
      EXPECT_CALL(*fim_sockets[i], WriteMsg(_));
    }
    FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
    efim->set_req_id(++req_id);
    resync_fim_processor_->Process(efim, fim_sockets[i]);
  }

  MockIChecksumGroupWriter* writer = new MockIChecksumGroupWriter;
  EXPECT_CALL(*store_, GetChecksumGroupWriter(42))
      .WillOnce(Return(writer));
  EXPECT_CALL(*writer, Write(StartsWith("hello"), 5, kSegmentSize));
  EXPECT_CALL(*fim_sockets[1], WriteMsg(_));
  EXPECT_CALL(*posix_fs_, Sync());
  EXPECT_CALL(*store_, UpdateAttr(5, mtime, 42));
  EXPECT_CALL(completion_handler_, Call(true));

  fim_sockets[kNumDSPerGroup - 2].reset(new MockIFimSocket);
  EXPECT_CALL(*fim_sockets[kNumDSPerGroup - 2], WriteMsg(_));
  resync_fim_processor_->Process(DSResyncDataEndFim::MakePtr(),
                                 fim_sockets[kNumDSPerGroup - 2]);
}

TEST_F(DSResyncTest, ResyncFimProcessorCancelledContent) {
  EXPECT_CALL(*store_, RemoveAll());

  resync_fim_processor_->AsyncResync(boost::bind(
      GetMockCall(completion_handler_), &completion_handler_, _1));

  boost::shared_ptr<MockIFimSocket> fim_sockets[kNumDSPerGroup - 1];
  uint64_t req_id = 0;
  for (unsigned i = 0; i < kNumDSPerGroup - 2; ++i) {
    fim_sockets[i].reset(new MockIFimSocket);
    EXPECT_CALL(*fim_sockets[i], WriteMsg(_));

    if (i < 2) {  // Two Fims actually received, cancelling each other
      FIM_PTR<DSResyncFim> rfim = DSResyncFim::MakePtr(5);
      rfim->set_req_id(++req_id);
      (*rfim)->inode = 42;
      (*rfim)->cg_off = kSegmentSize;
      std::memcpy(rfim->tail_buf(), "hello", 5);
      resync_fim_processor_->Process(rfim, fim_sockets[i]);
    }
    FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
    efim->set_req_id(++req_id);
    resync_fim_processor_->Process(efim, fim_sockets[i]);
  }

  // No content is written out, data writer not obtained
  EXPECT_CALL(*fim_sockets[0], WriteMsg(_));
  EXPECT_CALL(*fim_sockets[1], WriteMsg(_));
  EXPECT_CALL(*posix_fs_, Sync());
  EXPECT_CALL(completion_handler_, Call(true));

  fim_sockets[kNumDSPerGroup - 2].reset(new MockIFimSocket);
  EXPECT_CALL(*fim_sockets[kNumDSPerGroup - 2], WriteMsg(_));
  resync_fim_processor_->Process(DSResyncDataEndFim::MakePtr(),
                                 fim_sockets[kNumDSPerGroup - 2]);
}

TEST_F(DSResyncTest, ResyncFimProcessorLateResyncEnd) {
  EXPECT_CALL(*store_, RemoveAll());

  resync_fim_processor_->AsyncResync(boost::bind(
      GetMockCall(completion_handler_), &completion_handler_, _1));

  boost::shared_ptr<MockIFimSocket> fim_sockets[kNumDSPerGroup - 1];
  uint64_t req_id = 0;
  // Create all Fim sockets, send resync on socket 1 and 2
  for (unsigned i = 0; i < kNumDSPerGroup - 1; ++i) {
    fim_sockets[i].reset(new MockIFimSocket);
    if (i == 1 || i == 2) {
      FIM_PTR<DSResyncFim> rfim = DSResyncFim::MakePtr(5);
      rfim->set_req_id(++req_id);
      (*rfim)->inode = 42 + i;
      (*rfim)->cg_off = kSegmentSize;
      std::memcpy(rfim->tail_buf(), "hello", 5);
      resync_fim_processor_->Process(rfim, fim_sockets[i]);
    }
  }
  // Late resync end: send resync end on sockets except 0, 1 and 2
  for (unsigned i = 3; i < kNumDSPerGroup - 1; ++i) {
    EXPECT_CALL(*fim_sockets[i], WriteMsg(_));

    FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
    efim->set_req_id(++req_id);
    resync_fim_processor_->Process(efim, fim_sockets[i]);
  }

  // send resync end on socket 0, trigger write on socket 1
  EXPECT_CALL(*fim_sockets[0], WriteMsg(_));
  MockIChecksumGroupWriter* writer = new MockIChecksumGroupWriter;
  EXPECT_CALL(*store_, GetChecksumGroupWriter(43))
      .WillOnce(Return(writer));
  EXPECT_CALL(*writer, Write(StartsWith("hello"), 5, kSegmentSize));
  EXPECT_CALL(*fim_sockets[1], WriteMsg(_));

  FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
  efim->set_req_id(++req_id);
  resync_fim_processor_->Process(efim, fim_sockets[0]);

  // send resync end on socket 1, trigger write on socket 2
  EXPECT_CALL(*fim_sockets[1], WriteMsg(_));
  MockIChecksumGroupWriter* writer2 = new MockIChecksumGroupWriter;
  EXPECT_CALL(*store_, GetChecksumGroupWriter(44))
      .WillOnce(Return(writer2));
  EXPECT_CALL(*writer2, Write(StartsWith("hello"), 5, kSegmentSize));
  EXPECT_CALL(*fim_sockets[2], WriteMsg(_));

  resync_fim_processor_->Process(DSResyncDataEndFim::MakePtr(),
                                 fim_sockets[1]);

  // send resync end on socket 2, trigger full completion
  EXPECT_CALL(*fim_sockets[2], WriteMsg(_));
  EXPECT_CALL(*posix_fs_, Sync());
  EXPECT_CALL(completion_handler_, Call(true));

  resync_fim_processor_->Process(DSResyncDataEndFim::MakePtr(),
                                 fim_sockets[2]);
}

TEST_F(DSResyncTest, ResyncFimProcessorTooManySockets) {
  EXPECT_CALL(*store_, RemoveAll());

  resync_fim_processor_->AsyncResync(boost::bind(
      GetMockCall(completion_handler_), &completion_handler_, _1));

  boost::shared_ptr<MockIFimSocket> fim_sockets[kNumDSPerGroup];
  uint64_t req_id = 0;
  // Create all Fim sockets except the last, send resync on socket 1
  // and end to others
  for (unsigned i = 0; i < kNumDSPerGroup - 2; ++i) {
    fim_sockets[i].reset(new MockIFimSocket);
    if (i == 1) {  // The only Fim actually received
      FIM_PTR<DSResyncFim> rfim = DSResyncFim::MakePtr(5);
      rfim->set_req_id(++req_id);
      (*rfim)->inode = 42;
      (*rfim)->cg_off = kSegmentSize;
      std::memcpy(rfim->tail_buf(), "hello", 5);
      resync_fim_processor_->Process(rfim, fim_sockets[i]);
    } else {
      EXPECT_CALL(*fim_sockets[i], WriteMsg(_));

      FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
      efim->set_req_id(++req_id);
      resync_fim_processor_->Process(efim, fim_sockets[i]);
    }
  }

  // Create the last Fim socket and send end, trigger Write
  fim_sockets[kNumDSPerGroup - 2].reset(new MockIFimSocket);
  EXPECT_CALL(*fim_sockets[kNumDSPerGroup - 2], WriteMsg(_));
  MockIChecksumGroupWriter* writer = new MockIChecksumGroupWriter;
  EXPECT_CALL(*store_, GetChecksumGroupWriter(42))
      .WillOnce(Return(writer));
  EXPECT_CALL(*writer, Write(StartsWith("hello"), 5, kSegmentSize));
  EXPECT_CALL(*fim_sockets[1], WriteMsg(_));

  FIM_PTR<IFim> efim = DSResyncDataEndFim::MakePtr();
  efim->set_req_id(++req_id);
  resync_fim_processor_->Process(efim, fim_sockets[kNumDSPerGroup - 2]);

  // Create one more Fim socket, trigger error
  fim_sockets[kNumDSPerGroup - 1].reset(new MockIFimSocket);
  EXPECT_CALL(*fim_sockets[kNumDSPerGroup - 1], WriteMsg(_));
  EXPECT_CALL(completion_handler_, Call(false));

  efim = DSResyncDataEndFim::MakePtr();
  efim->set_req_id(++req_id);
  resync_fim_processor_->Process(efim, fim_sockets[kNumDSPerGroup - 1]);
}

TEST_F(DSResyncTest, ResyncFimProcessorOptResync) {
  server_.set_opt_resync(true);
  resync_fim_processor_->AsyncResync(boost::bind(
      GetMockCall(completion_handler_), &completion_handler_, _1));

  // Send Dir
  boost::shared_ptr<MockIFimSocket> fim_sockets[kNumDSPerGroup - 1];
  uint64_t req_id = 0;
  for (unsigned i = 0; i < kNumDSPerGroup - 1; ++i)
    fim_sockets[i].reset(new MockIFimSocket);

  for (unsigned i = 0; i < 2; ++i) {
    EXPECT_CALL(*fim_sockets[i], WriteMsg(_));

    FIM_PTR<DSResyncDirFim> dfim = DSResyncDirFim::MakePtr(sizeof(InodeNum));
    dfim->set_req_id(++req_id);
    reinterpret_cast<InodeNum&>(*dfim->tail_buf()) = 0x2C;
    resync_fim_processor_->Process(dfim, fim_sockets[i]);
  }

  // Send List except the last
  for (unsigned i = 0; i < kNumDSPerGroup - 2; ++i) {
    FIM_PTR<DSResyncListFim> lfim = DSResyncListFim::MakePtr();
    (*lfim)->start_idx = 0;
    (*lfim)->max_reply = 1024;
    lfim->set_req_id(++req_id);
    resync_fim_processor_->Process(lfim, fim_sockets[i]);
  }

  // Send last List, trigger local listing
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*dsg_ready_time_keeper_, GetLastUpdate())
      .WillOnce(Return(123456789));
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  ranges.push_back(32);
  EXPECT_CALL(*durable_range_, Get())
      .WillOnce(Return(ranges));
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, InodeList(ranges))
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, SetFilterCTime(123456669));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("000/000002B.d"),
                      SetArgPointee<1>(false),
                      WithArg<2>(SetStatbufCtime(123456789)),
                      Return(true)))
      .WillOnce(Return(false));
  EXPECT_CALL(*store_, FreeData(0x2B, true));
  EXPECT_CALL(*store_, FreeData(0x2C, true));
  EXPECT_CALL(*store_, FreeData(0x2D, true));
  std::vector<InodeNum> removed;
  removed.push_back(0x2B);
  removed.push_back(0x2D);
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(removed));
  FIM_PTR<IFim> reply;
  for (unsigned i = 0; i < kNumDSPerGroup - 1; ++i)
    EXPECT_CALL(*fim_sockets[i], WriteMsg(_))
        .WillOnce(SaveArg<0>(&reply));

  FIM_PTR<DSResyncListFim> lfim = DSResyncListFim::MakePtr();
  (*lfim)->start_idx = 0;
  (*lfim)->max_reply = 1024;
  lfim->set_req_id(++req_id);
  resync_fim_processor_->Process(lfim, fim_sockets[kNumDSPerGroup - 2]);
  DSResyncListReplyFim& lreply = dynamic_cast<DSResyncListReplyFim&>(*reply);
  EXPECT_EQ(3U, lreply->num_inode);
  EXPECT_EQ(3U * sizeof(InodeNum), lreply.tail_buf_size());
  InodeNum* inodes = reinterpret_cast<InodeNum*>(lreply.tail_buf());
  EXPECT_EQ(0x2BU, inodes[0]);
  EXPECT_EQ(0x2CU, inodes[1]);
  EXPECT_EQ(0x2DU, inodes[2]);

  // Last List past the end, return nothing
  EXPECT_CALL(*fim_sockets[kNumDSPerGroup - 2], WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  Mock::VerifyAndClear(store_);
  (*lfim)->start_idx = 1024;
  (*lfim)->max_reply = 1024;
  lfim->set_req_id(++req_id);
  resync_fim_processor_->Process(lfim, fim_sockets[kNumDSPerGroup - 2]);
  DSResyncListReplyFim& lreply2 = dynamic_cast<DSResyncListReplyFim&>(*reply);
  EXPECT_EQ(3U, lreply2->num_inode);
  EXPECT_EQ(0U, lreply2.tail_buf_size());
}

TEST_F(DSResyncTest, ResyncFimProcessorOptResyncPlainList) {
  server_.set_opt_resync(true);
  resync_fim_processor_->AsyncResync(boost::bind(
      GetMockCall(completion_handler_), &completion_handler_, _1));

  // Send Dir
  boost::shared_ptr<MockIFimSocket> fim_sockets[kNumDSPerGroup - 1];
  uint64_t req_id = 0;
  for (unsigned i = 0; i < kNumDSPerGroup - 1; ++i)
    fim_sockets[i].reset(new MockIFimSocket);

  for (unsigned i = 0; i < 2; ++i) {
    EXPECT_CALL(*fim_sockets[i], WriteMsg(_));

    FIM_PTR<DSResyncDirFim> dfim = DSResyncDirFim::MakePtr(sizeof(InodeNum));
    dfim->set_req_id(++req_id);
    reinterpret_cast<InodeNum&>(*dfim->tail_buf()) = 0x2C;
    resync_fim_processor_->Process(dfim, fim_sockets[i]);
  }

  // Send List except the last
  for (unsigned i = 0; i < kNumDSPerGroup - 2; ++i) {
    FIM_PTR<DSResyncListFim> lfim = DSResyncListFim::MakePtr();
    (*lfim)->start_idx = 0;
    (*lfim)->max_reply = 1024;
    lfim->set_req_id(++req_id);
    resync_fim_processor_->Process(lfim, fim_sockets[i]);
  }

  // Send last List, trigger local listing
  EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(true));
  EXPECT_CALL(*dsg_ready_time_keeper_, GetLastUpdate())
      .WillOnce(Return(0));
  MockIDirIterator* dir_iterator = new MockIDirIterator;
  EXPECT_CALL(*store_, List())
      .WillOnce(Return(dir_iterator));
  EXPECT_CALL(*dir_iterator, GetNext(_, _, _))
      .WillOnce(DoAll(SetArgPointee<0>("000/x"),
                      SetArgPointee<1>(true),
                      Return(true)))
      .WillOnce(DoAll(SetArgPointee<0>("000/000002B.d"),
                      SetArgPointee<1>(false),
                      WithArg<2>(SetStatbufCtime(123456789)),
                      Return(true)))
      .WillOnce(Return(false));
  EXPECT_CALL(*store_, FreeData(0x2B, true));
  EXPECT_CALL(*store_, FreeData(0x2C, true));
  EXPECT_CALL(*store_, FreeData(0x2D, true));
  std::vector<InodeNum> removed;
  removed.push_back(0x2B);
  removed.push_back(0x2D);
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(removed));
  FIM_PTR<IFim> reply;
  for (unsigned i = 0; i < kNumDSPerGroup - 1; ++i)
    EXPECT_CALL(*fim_sockets[i], WriteMsg(_))
        .WillOnce(SaveArg<0>(&reply));

  FIM_PTR<DSResyncListFim> lfim = DSResyncListFim::MakePtr();
  (*lfim)->start_idx = 0;
  (*lfim)->max_reply = 1024;
  lfim->set_req_id(++req_id);
  resync_fim_processor_->Process(lfim, fim_sockets[kNumDSPerGroup - 2]);
  DSResyncListReplyFim& lreply = dynamic_cast<DSResyncListReplyFim&>(*reply);
  EXPECT_EQ(3U, lreply->num_inode);
  EXPECT_EQ(3U * sizeof(InodeNum), lreply.tail_buf_size());
  InodeNum* inodes = reinterpret_cast<InodeNum*>(lreply.tail_buf());
  EXPECT_EQ(0x2BU, inodes[0]);
  EXPECT_EQ(0x2CU, inodes[1]);
  EXPECT_EQ(0x2DU, inodes[2]);

  // Last List past the end, return nothing
  EXPECT_CALL(*fim_sockets[kNumDSPerGroup - 2], WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));

  Mock::VerifyAndClear(store_);
  (*lfim)->start_idx = 1024;
  (*lfim)->max_reply = 1024;
  lfim->set_req_id(++req_id);
  resync_fim_processor_->Process(lfim, fim_sockets[kNumDSPerGroup - 2]);
  DSResyncListReplyFim& lreply2 = dynamic_cast<DSResyncListReplyFim&>(*reply);
  EXPECT_EQ(3U, lreply2->num_inode);
  EXPECT_EQ(0U, lreply2.tail_buf_size());
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
