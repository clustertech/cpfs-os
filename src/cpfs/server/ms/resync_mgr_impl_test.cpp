/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/resync_mgr_impl.hpp"

#include <fcntl.h>
#include <stdint.h>

#include <sys/stat.h>

#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "config_mgr.hpp"
#include "dir_iterator_mock.hpp"
#include "fim.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "inode_src_mock.hpp"
#include "mock_actions.hpp"
#include "req_entry_mock.hpp"
#include "req_tracker_mock.hpp"
#include "time_keeper_mock.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/durable_range_mock.hpp"
#include "server/inode_removal_tracker_mock.hpp"
#include "server/ms/base_ms_mock.hpp"
#include "server/ms/dirty_inode_mock.hpp"
#include "server/ms/inode_usage_mock.hpp"
#include "server/ms/meta_dir_reader_mock.hpp"
#include "server/ms/resync_mgr.hpp"
#include "server/ms/state_mgr_mock.hpp"
#include "server/ms/store_mock.hpp"
#include "server/ms/topology_mock.hpp"
#include "server/server_info_mock.hpp"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::InvokeWithoutArgs;
using ::testing::InSequence;
using ::testing::Lt;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::Throw;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/resync_mgr_test_XXXXXX";
const unsigned kMaxSend = 2;  // Send 2 Fims before waiting for reply

class ResyncMgrTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  MockITrackerMapper* tracker_mapper_;
  MockIMetaDirReader* meta_dir_reader_;
  MockIInodeRemovalTracker* inode_removal_tracker_;
  MockIInodeSrc* inode_src_;
  MockIInodeUsage* inode_usage_;
  MockITimeKeeper* peer_time_keeper_;
  MockITopologyMgr* topology_mgr_;
  MockIHACounter* ha_counter_;
  MockIStateMgr* state_mgr_;
  MockIStore* store_;
  MockIDirtyInodeMgr* dirty_inode_mgr_;
  MockIServerInfo* server_info_;
  MockIDurableRange* durable_range_;
  MockBaseMetaServer server_;
  boost::scoped_ptr<IResyncMgr> resync_mgr_;
  boost::shared_ptr<MockIReqTracker> ms_tracker_;
  boost::shared_ptr<MockIFimSocket> ms_fim_socket_;
  boost::shared_ptr<MockIReqEntry> req_entry_;
  FIM_PTR<IFim> reply_;
  MockIDirIterator* inode_iter_;

  ResyncMgrTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        resync_mgr_(MakeResyncMgr(&server_, kMaxSend)),
        ms_tracker_(new MockIReqTracker),
        ms_fim_socket_(new MockIFimSocket),
        req_entry_(new MockIReqEntry),
        reply_(ResultCodeReplyFim::MakePtr()),
        inode_iter_(0) {
    server_.set_tracker_mapper(tracker_mapper_ = new MockITrackerMapper);
    server_.set_meta_dir_reader(meta_dir_reader_ = new MockIMetaDirReader);
    server_.set_inode_removal_tracker(
        inode_removal_tracker_ = new MockIInodeRemovalTracker);
    server_.set_inode_src(inode_src_ = new MockIInodeSrc);
    server_.set_inode_usage(inode_usage_ = new MockIInodeUsage);
    server_.set_peer_time_keeper(peer_time_keeper_ = new MockITimeKeeper);
    server_.set_topology_mgr(topology_mgr_ = new MockITopologyMgr);
    server_.set_ha_counter(ha_counter_ = new MockIHACounter);
    server_.set_state_mgr(state_mgr_ = new MockIStateMgr);
    server_.set_store(store_ = new MockIStore);
    server_.set_dirty_inode_mgr(dirty_inode_mgr_ = new MockIDirtyInodeMgr);
    server_.set_server_info(server_info_ = new MockIServerInfo);
    server_.set_durable_range(durable_range_ = new MockIDurableRange);
    server_.configs().set_heartbeat_interval(5.0);
    server_.configs().set_data_dir(data_path_);
    // Mock for AddTransientRequest()
    EXPECT_CALL(*tracker_mapper_, GetMSTracker())
        .WillRepeatedly(Return(ms_tracker_));
    // TODO(Joseph): WIP for #13801
    mkdir((std::string(data_path_) + "/000").c_str(), 0777);
  }

  void ExpectNormalSending() {
    EXPECT_CALL(*topology_mgr_, SendAllFCInfo());
    EXPECT_CALL(*ms_tracker_, GetFimSocket())
        .WillOnce(Return(ms_fim_socket_));
    EXPECT_CALL(*topology_mgr_,
                SendAllDSInfo(boost::static_pointer_cast<IFimSocket>
                              (ms_fim_socket_)));
  }

  void ExpectEmptyRemovedInodes() {
    EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
        .WillOnce(Return(std::vector<InodeNum>()));
  }

  void ExpectSendLastUsed() {
    std::vector<InodeNum> ret(4096U, 0);
    ret[0] = 42U;
    EXPECT_CALL(*inode_src_, GetLastUsed())
        .WillOnce(Return(ret));
  }

  void ExpectEmptyInodeUsage() {
    EXPECT_CALL(*dirty_inode_mgr_, GetList())
        .WillOnce(Return(DirtyInodeMap()));
    EXPECT_CALL(*inode_usage_, pending_unlink())
        .WillOnce(Return(boost::unordered_set<InodeNum>()));
    EXPECT_CALL(*inode_usage_, client_opened())
        .WillOnce(Return(ClientInodeMap()));
  }

  void ExpectLastUpdateForFullResync() {
    EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
        .WillOnce(Return(std::numeric_limits<uint64_t>::max()));
  }

  void ExpectEmptyInodeList() {
    EXPECT_CALL(*store_, List(StrEq("")))
        .WillOnce(Return(inode_iter_));
    EXPECT_CALL(*inode_iter_, GetNext(_, _, _))
        .WillOnce(Return(false));
  }

  void ExpectSendCompletion() {
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(Return(req_entry_));
    EXPECT_CALL(*req_entry_, WaitReply())
        .WillOnce(ReturnRef(reply_));
    EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(false));
    EXPECT_CALL(*durable_range_, SetConservative(false));
    EXPECT_CALL(*peer_time_keeper_, Start());
    EXPECT_CALL(*state_mgr_, SwitchState(kStateActive));
    EXPECT_CALL(*topology_mgr_, StartStopWorker());
  }

  void PrepareGetReply() {
    EXPECT_CALL(*req_entry_, reply())
        .WillRepeatedly(ReturnRef(reply_));
  }

  ~ResyncMgrTest() {
    Mock::VerifyAndClear(durable_range_);
    Mock::VerifyAndClear(dirty_inode_mgr_);
    Mock::VerifyAndClear(inode_removal_tracker_);
    Mock::VerifyAndClear(inode_src_);
    Mock::VerifyAndClear(inode_usage_);
    Mock::VerifyAndClear(ms_fim_socket_.get());
    Mock::VerifyAndClear(ms_tracker_.get());
    Mock::VerifyAndClear(peer_time_keeper_);
    Mock::VerifyAndClear(req_entry_.get());
    Mock::VerifyAndClear(state_mgr_);
    Mock::VerifyAndClear(store_);
    Mock::VerifyAndClear(topology_mgr_);
    Mock::VerifyAndClear(tracker_mapper_);
  }
};

TEST_F(ResyncMgrTest, RequestResyncFull) {
  EXPECT_CALL(*ha_counter_, PersistCount(0));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(0));
  boost::shared_ptr<MockIFimSocket> ms_fim_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket));
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*ms_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));

  // Actual call
  resync_mgr_->RequestResync();
  Sleep(0.15)();  // Wait for thread to complete
  MSResyncReqFim& rfim = dynamic_cast<MSResyncReqFim&>(*fim);
  EXPECT_TRUE(rfim->first);
  EXPECT_EQ('F', rfim->last);
  EXPECT_EQ(0U, rfim.tail_buf_size());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ResyncMgrTest, RequestResyncNoRemoveConcurrent) {
  EXPECT_CALL(*ha_counter_, PersistCount(0));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(500));
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(std::vector<InodeNum>()));
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_CALL(*durable_range_, Get())
      .WillOnce(Return(ranges));
  EXPECT_CALL(*store_, RemoveInodeSince(ranges, Lt(500U)))
      .WillOnce(DoAll(InvokeWithoutArgs(Sleep(0.2)),
                      Return(0)));
  boost::shared_ptr<MockIFimSocket> ms_fim_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket));
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*ms_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));

  // Actual call
  resync_mgr_->RequestResync();
  resync_mgr_->RequestResync();
  Sleep(0.4)();  // Wait for thread to complete
  MSResyncReqFim& rfim = dynamic_cast<MSResyncReqFim&>(*fim);
  EXPECT_TRUE(rfim->first);
  EXPECT_EQ('O', rfim->last);
  EXPECT_EQ(0U, rfim.tail_buf_size());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ResyncMgrTest, RequestResyncRemoveTwo) {
  EXPECT_CALL(*ha_counter_, PersistCount(0));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(500));
  std::vector<InodeNum> removed;
  removed.push_back(123);
  removed.push_back(456);
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(removed));
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_CALL(*durable_range_, Get())
      .WillOnce(Return(ranges));
  EXPECT_CALL(*store_, RemoveInodeSince(ranges, Lt(500U)))
      .WillOnce(Return(0));
  boost::shared_ptr<MockIFimSocket> ms_fim_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket));
  FIM_PTR<IFim> fim;
  EXPECT_CALL(*ms_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim));

  // Actual call
  resync_mgr_->RequestResync();
  Sleep(0.1)();  // Wait for thread to complete
  MSResyncReqFim& rfim = dynamic_cast<MSResyncReqFim&>(*fim);
  EXPECT_TRUE(rfim->first);
  EXPECT_EQ('O', rfim->last);
  EXPECT_EQ(2 * sizeof(InodeNum), rfim.tail_buf_size());
  const InodeNum* inodes = reinterpret_cast<const InodeNum*>(rfim.tail_buf());
  EXPECT_EQ(123U, inodes[0]);
  EXPECT_EQ(456U, inodes[1]);

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ResyncMgrTest, RequestResyncRemoveMany) {
  EXPECT_CALL(*ha_counter_, PersistCount(0));
  EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
      .WillOnce(Return(500));
  std::vector<InodeNum> removed;
  for (int i = 0; i < 40000; ++i)
    removed.push_back(i + 2);
  EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
      .WillOnce(Return(removed));
  std::vector<InodeNum> ranges;
  ranges.push_back(0);
  EXPECT_CALL(*durable_range_, Get())
      .WillOnce(Return(ranges));
  EXPECT_CALL(*store_, RemoveInodeSince(ranges, Lt(500U)))
      .WillOnce(Return(0));
  boost::shared_ptr<MockIFimSocket> ms_fim_socket(new MockIFimSocket);
  EXPECT_CALL(*tracker_mapper_, GetMSFimSocket())
      .WillRepeatedly(Return(ms_fim_socket));
  FIM_PTR<IFim> fim, fim2, fim3;
  EXPECT_CALL(*ms_fim_socket, WriteMsg(_))
      .WillOnce(SaveArg<0>(&fim))
      .WillOnce(SaveArg<0>(&fim2))
      .WillOnce(SaveArg<0>(&fim3));

  // Actual call
  resync_mgr_->RequestResync();
  Sleep(1.5)();  // Wait for thread to complete
  MSResyncReqFim& rfim = dynamic_cast<MSResyncReqFim&>(*fim);
  EXPECT_TRUE(rfim->first);
  EXPECT_FALSE(rfim->last);
  EXPECT_EQ(16384 * sizeof(InodeNum), rfim.tail_buf_size());
  MSResyncReqFim& rfim2 = dynamic_cast<MSResyncReqFim&>(*fim2);
  EXPECT_FALSE(rfim2->first);
  EXPECT_FALSE(rfim2->last);
  EXPECT_EQ(16384 * sizeof(InodeNum), rfim2.tail_buf_size());
  MSResyncReqFim& rfim3 = dynamic_cast<MSResyncReqFim&>(*fim3);
  EXPECT_FALSE(rfim3->first);
  EXPECT_TRUE(rfim3->last);
  EXPECT_EQ((40000 - 2 * 16384) * sizeof(InodeNum), rfim3.tail_buf_size());

  Mock::VerifyAndClear(tracker_mapper_);
}

TEST_F(ResyncMgrTest, ResyncExtendedAttrs) {
  inode_iter_ = new MockIDirIterator;  // Delete in ResyncInodes()
  {
    PrepareGetReply();
    EXPECT_CALL(*server_info_, Get("swait", ""))
       .WillRepeatedly(Return("0-3"));
    ExpectNormalSending();
    ExpectEmptyRemovedInodes();
    ExpectSendLastUsed();
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillRepeatedly(Return(req_entry_));
    ExpectEmptyInodeUsage();
    ExpectLastUpdateForFullResync();
    ExpectEmptyInodeList();
    EXPECT_CALL(*req_entry_, WaitReply())
        .WillOnce(ReturnRef(reply_));
    EXPECT_CALL(*inode_removal_tracker_, SetPersistRemoved(false));
    EXPECT_CALL(*durable_range_, SetConservative(false));
    EXPECT_CALL(*peer_time_keeper_, Start());
    EXPECT_CALL(*state_mgr_, SwitchState(kStateActive));
    EXPECT_CALL(*topology_mgr_, StartStopWorker());
  }
  resync_mgr_->SendAllResync(true, std::vector<InodeNum>());
  resync_mgr_->SendAllResync(true, std::vector<InodeNum>());
  Sleep(0.4)();  // Wait for thread to complete
  // Cleanup
  Mock::VerifyAndClear(inode_iter_);
}

TEST_F(ResyncMgrTest, ResyncConcurrentSendNothing) {
  inode_iter_ = new MockIDirIterator;  // Delete in ResyncInodes()
  {
    PrepareGetReply();
    EXPECT_CALL(*server_info_, Get("swait", ""))
       .WillRepeatedly(Return(""));
    InSequence seq;
    ExpectNormalSending();
    ExpectEmptyRemovedInodes();
    ExpectSendLastUsed();
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))  // Prepare
        .WillOnce(DoAll(InvokeWithoutArgs(Sleep(0.2)),
                        Return(req_entry_)));
    ExpectEmptyInodeUsage();
    ExpectLastUpdateForFullResync();
    ExpectEmptyInodeList();
    ExpectSendCompletion();
  }
  resync_mgr_->SendAllResync(true, std::vector<InodeNum>());
  resync_mgr_->SendAllResync(true, std::vector<InodeNum>());
  Sleep(0.4)();  // Wait for thread to complete
  // Cleanup
  Mock::VerifyAndClear(inode_iter_);
}

TEST_F(ResyncMgrTest, ResyncException) {
  {
    InSequence seq;
    ExpectNormalSending();
    EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
        .WillOnce(Throw(std::runtime_error("Test I/O error")));
    // Unfreeze after error
    EXPECT_CALL(*ms_tracker_, GetFimSocket())
        .WillOnce(Return(ms_fim_socket_));
    EXPECT_CALL(*ms_fim_socket_, Shutdown());
    EXPECT_CALL(*state_mgr_, SwitchState(kStateActive));
    EXPECT_CALL(*topology_mgr_, StartStopWorker());
  }
  resync_mgr_->SendAllResync(true, std::vector<InodeNum>());
  Sleep(0.1)();  // Wait for thread to complete
}

TEST_F(ResyncMgrTest, ResyncSendRemoval) {
  inode_iter_ = new MockIDirIterator;  // Delete in ResyncInodes()
  FIM_PTR<IFim> req1, req2;
  {
    PrepareGetReply();
    EXPECT_CALL(*server_info_, Get("swait", ""))
       .WillRepeatedly(Return(""));
    InSequence seq;
    ExpectNormalSending();
    std::vector<InodeNum> removed_inodes;
    removed_inodes.push_back(2);
    removed_inodes.push_back(3);
    EXPECT_CALL(*inode_removal_tracker_, GetRemovedInodes())
        .WillOnce(Return(removed_inodes));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(DoAll(SaveArg<0>(&req1),
                        Return(req_entry_)))
        .WillOnce(DoAll(SaveArg<0>(&req2),
                        Return(req_entry_)));
    ExpectSendLastUsed();
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))  // Prepare
        .WillOnce(Return(req_entry_));
    ExpectEmptyInodeUsage();
    ExpectEmptyInodeList();
    ExpectSendCompletion();
  }
  resync_mgr_->SendAllResync(false, std::vector<InodeNum>());
  Sleep(0.1)();  // Wait for thread to complete
  // Checks
  ASSERT_EQ(kResyncRemovalFim, req1->type());
  ASSERT_EQ(kResyncRemovalFim, req2->type());
  ResyncRemovalFim& r_req1 = dynamic_cast<ResyncRemovalFim&>(*req1);
  ResyncRemovalFim& r_req2 = dynamic_cast<ResyncRemovalFim&>(*req2);
  EXPECT_EQ(2U, r_req1->inode);
  EXPECT_EQ(3U, r_req2->inode);
  // Cleanup
  Mock::VerifyAndClear(inode_iter_);
}

TEST_F(ResyncMgrTest, ResyncSendInodeUsage) {
  inode_iter_ = new MockIDirIterator;  // Delete in ResyncInodes()
  FIM_PTR<IFim> req_prep, req_dirty, req_p_unlink, req_c_opened;
  {
    // Same as empty sending
    PrepareGetReply();
    EXPECT_CALL(*server_info_, Get("swait", ""))
       .WillRepeatedly(Return(""));
    InSequence seq;
    ExpectNormalSending();
    ExpectEmptyRemovedInodes();
    ExpectSendLastUsed();
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(DoAll(SaveArg<0>(&req_prep),
                        Return(req_entry_)));
    // Dirty list
    DirtyInodeMap dirty_map;
    dirty_map[99] = DirtyInodeInfo();
    dirty_map[99].clean = false;
    dirty_map[100] = DirtyInodeInfo();
    dirty_map[100].clean = true;
    EXPECT_CALL(*dirty_inode_mgr_, GetList())
        .WillOnce(Return(dirty_map));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(DoAll(SaveArg<0>(&req_dirty),
                        Return(req_entry_)));
    // Pending Removal
    boost::unordered_set<InodeNum> pending_rm_inodes;
    pending_rm_inodes.insert(100);
    pending_rm_inodes.insert(101);
    EXPECT_CALL(*inode_usage_, pending_unlink())
        .WillOnce(Return(pending_rm_inodes));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(DoAll(SaveArg<0>(&req_p_unlink),
                        Return(req_entry_)));
    // FC opened inodes
    ClientInodeMap fc_inodes;
    fc_inodes[100][5] = kInodeReadAccess;
    fc_inodes[100][700] = kInodeWriteAccess;
    EXPECT_CALL(*inode_usage_, client_opened())
        .WillOnce(Return(fc_inodes));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(DoAll(SaveArg<0>(&req_c_opened),
                        Return(req_entry_)));
    // Same as empty sending again
    ExpectLastUpdateForFullResync();
    ExpectEmptyInodeList();
    ExpectSendCompletion();
  }
  resync_mgr_->SendAllResync(true, std::vector<InodeNum>());
  Sleep(0.1)();  // Wait for thread to complete
  // Checks
  ASSERT_EQ(kResyncInodeUsagePrepFim, req_prep->type());
  ResyncInodeUsagePrepFim& r_req_prep =
      dynamic_cast<ResyncInodeUsagePrepFim&>(*req_prep);
  const InodeNum* inodes =
      reinterpret_cast<const InodeNum*>(r_req_prep.tail_buf());
  EXPECT_EQ(42U, inodes[0]);
  EXPECT_EQ(kResyncDirtyInodeFim, req_dirty->type());
  ResyncDirtyInodeFim& r_req_dirty =
      dynamic_cast<ResyncDirtyInodeFim&>(*req_dirty);
  EXPECT_EQ(99U, r_req_dirty->inode);
  ASSERT_EQ(kResyncPendingUnlinkFim, req_p_unlink->type());
  ResyncPendingUnlinkFim& r_req_p_unlink =
      dynamic_cast<ResyncPendingUnlinkFim&>(*req_p_unlink);
  EXPECT_TRUE(r_req_p_unlink->first);
  EXPECT_TRUE(r_req_p_unlink->last);
  EXPECT_EQ(2 * sizeof(InodeNum), req_p_unlink->tail_buf_size());
  ASSERT_EQ(kResyncClientOpenedFim, req_c_opened->type());
  ResyncClientOpenedFim& r_req_c_opened =
      dynamic_cast<ResyncClientOpenedFim&>(*req_c_opened);
  EXPECT_EQ(1U, r_req_c_opened->num_writable);
  // Cleanup
  Mock::VerifyAndClear(inode_iter_);
}

TEST_F(ResyncMgrTest, ResyncSendInodes) {
  inode_iter_ = new MockIDirIterator;  // Delete in ResyncInodes()
  MockIDirIterator* dentry_iter = new MockIDirIterator;
  FIM_PTR<IFim> empty_fim;
  {
    EXPECT_CALL(*server_info_, Get("swait", ""))
       .WillRepeatedly(Return(""));
    InSequence seq;
    ExpectNormalSending();
    ExpectEmptyRemovedInodes();
    ExpectSendLastUsed();
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))  // Prepare
        .WillOnce(Return(req_entry_));
    EXPECT_CALL(*req_entry_, reply())
        .WillOnce(ReturnRef(empty_fim));
    ExpectEmptyInodeUsage();
    // Send last update time
    EXPECT_CALL(*peer_time_keeper_, GetLastUpdate())
        .WillOnce(Return(12345U));
    // Send inode
    std::vector<InodeNum> ranges;
    ranges.push_back(0);
    EXPECT_CALL(*durable_range_, Get())
        .WillOnce(Return(ranges));
    EXPECT_CALL(*store_, InodeList(ranges))
        .WillOnce(Return(inode_iter_));
    EXPECT_CALL(*inode_iter_, SetFilterCTime(_));
    EXPECT_CALL(*inode_iter_, GetNext(_, _, _))
        .WillOnce(DoAll(SetArgPointee<0>("foo"),
                        SetArgPointee<1>(false),
                        Return(true)))
        .WillOnce(DoAll(SetArgPointee<0>("000/0000000000000001"),
                        SetArgPointee<1>(true),
                        Return(true)));
    // root inode
    FIM_PTR<ResyncInodeFim> rfim1 = ResyncInodeFim::MakePtr();
    EXPECT_CALL(*meta_dir_reader_, ToInodeFim(StrEq("000/0000000000000001")))
        .WillOnce(Return(rfim1));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(Return(req_entry_));
    EXPECT_CALL(*req_entry_, reply())
        .WillOnce(ReturnRef(empty_fim));
    EXPECT_CALL(*req_entry_, WaitReply())
        .WillOnce(ReturnRef(reply_));
    EXPECT_CALL(*req_entry_, reply())
        .Times(2)
        .WillRepeatedly(ReturnRef(reply_));
    // root dentry
    EXPECT_CALL(*store_, List(StrEq("000/0000000000000001")))
        .WillOnce(Return(dentry_iter));
    EXPECT_CALL(*dentry_iter, GetNext(_, _, _))
        .WillOnce(DoAll(SetArgPointee<0>("test"),
                        SetArgPointee<1>(false),
                        Return(true)));
    FIM_PTR<ResyncDentryFim> rfim3 = ResyncDentryFim::MakePtr();
    EXPECT_CALL(*meta_dir_reader_,
                ToDentryFim(StrEq("000/0000000000000001"), "test", 'F'))
        .WillOnce(Return(rfim3));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(Return(req_entry_));
    EXPECT_CALL(*req_entry_, reply())
        .WillOnce(ReturnRef(reply_));
    EXPECT_CALL(*dentry_iter, GetNext(_, _, _))
        .WillOnce(Return(false));
    // Non-root inode
    EXPECT_CALL(*inode_iter_, GetNext(_, _, _))
        .WillOnce(DoAll(SetArgPointee<0>("000/0000000000000001x"),
                        SetArgPointee<1>(false),
                        Return(true)))
        .WillOnce(DoAll(SetArgPointee<0>("000/0000000000000002"),
                        SetArgPointee<1>(false),
                        Return(true)));
    FIM_PTR<ResyncInodeFim> rfim2 = ResyncInodeFim::MakePtr();
    EXPECT_CALL(*meta_dir_reader_, ToInodeFim(StrEq("000/0000000000000002")))
        .WillOnce(Return(rfim2));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(Return(req_entry_));
    EXPECT_CALL(*req_entry_, reply())
        .WillOnce(ReturnRef(reply_));
    EXPECT_CALL(*inode_iter_, GetNext(_, _, _))
        .WillOnce(DoAll(SetArgPointee<0>("000/0000000000000002.1"),
                        SetArgPointee<1>(false),
                        Return(true)))
        .WillOnce(DoAll(SetArgPointee<0>("000/c"),
                        SetArgPointee<1>(false),
                        Return(true)))
        .WillOnce(Return(false));
    // Removed file
    FIM_PTR<ResyncInodeFim> rfim4 = ResyncInodeFim::MakePtr();
    EXPECT_CALL(*meta_dir_reader_, ToInodeFim(StrEq("000/0000000000000008")))
        .WillOnce(Return(rfim4));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(Return(req_entry_));
    EXPECT_CALL(*req_entry_, reply())
        .WillOnce(ReturnRef(reply_));
    // Removed directory
    FIM_PTR<ResyncInodeFim> rfim5 = ResyncInodeFim::MakePtr();
    EXPECT_CALL(*meta_dir_reader_, ToInodeFim(StrEq("000/0000000000000009")))
        .WillOnce(Return(rfim5));
    EXPECT_CALL(*ms_tracker_, AddTransientRequest(_, _))
        .WillOnce(Return(req_entry_));
    EXPECT_CALL(*req_entry_, reply())
        .WillOnce(ReturnRef(reply_));
    MockIDirIterator* dentry_iter2 = new MockIDirIterator;
    EXPECT_CALL(*store_, List(StrEq("000/0000000000000009")))
        .WillOnce(Return(dentry_iter2));
    EXPECT_CALL(*dentry_iter2, GetNext(_, _, _))
        .WillOnce(Return(false));
    ExpectSendCompletion();
  }
  open((std::string(data_path_) + "/000/0000000000000008").c_str(),
       O_CREAT | O_WRONLY, 0666);
  mkdir((std::string(data_path_) + "/000/0000000000000009").c_str(), 0777);
  // Inodes removed by standby MS
  std::vector<InodeNum> peer_removed;
  peer_removed.push_back(2);
  peer_removed.push_back(8);
  peer_removed.push_back(9);
  resync_mgr_->SendAllResync(true, peer_removed);
  Sleep(0.4)();  // Wait for thread to complete

  // Cleanup
  Mock::VerifyAndClear(inode_iter_);
  Mock::VerifyAndClear(meta_dir_reader_);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
