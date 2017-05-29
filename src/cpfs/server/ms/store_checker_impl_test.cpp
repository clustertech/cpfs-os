/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/store_checker_impl.hpp"

#include <stdint.h>

#include <sys/stat.h>

#include <string>
#include <vector>

#include <boost/format.hpp>
#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "console_mock.hpp"
#include "finfo.hpp"
#include "mock_actions.hpp"
#include "server/ms/dsg_alloc_mock.hpp"
#include "server/ms/inode_mutex.hpp"
#include "server/ms/inode_src_mock.hpp"
#include "server/ms/inode_usage_mock.hpp"
#include "server/ms/store.hpp"
#include "server/ms/store_checker_mock.hpp"
#include "server/ms/store_impl.hpp"
#include "server/ms/ugid_handler_mock.hpp"
#include "server/server_info.hpp"
#include "server/server_info_impl.hpp"

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::ContainsRegex;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnNew;
using ::testing::StartsWith;
using ::testing::Throw;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/store_test_XXXXXX";

class MSStoreCheckerTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  boost::scoped_ptr<IStore> store_;
  boost::scoped_ptr<MockIDSGAllocator> dsg_allocator_;
  boost::scoped_ptr<MockIInodeSrc> inode_src_;
  boost::scoped_ptr<MockIUgidHandler> ugid_handler_;
  boost::scoped_ptr<MockIInodeUsage> inode_usage_;
  ReqContext req_context_;
  OpContext op_context_;
  boost::scoped_ptr<IStoreChecker> checker_;

  static const uint32_t kTestUid;
  static const uint32_t kTestGid;
  static const uint64_t kTestOptime;

  MSStoreCheckerTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        store_(MakeStore(data_path_)),
        dsg_allocator_(new MockIDSGAllocator),
        inode_src_(new MockIInodeSrc),
        ugid_handler_(new MockIUgidHandler),
        inode_usage_(new MockIInodeUsage) {
    store_->SetDSGAllocator(dsg_allocator_.get());
    store_->SetInodeSrc(inode_src_.get());
    store_->SetUgidHandler(ugid_handler_.get());
    store_->SetInodeUsage(inode_usage_.get());
    req_context_.uid = kTestUid;
    req_context_.gid = kTestGid;
    req_context_.optime.sec = kTestOptime;
    req_context_.optime.ns = 0;
    op_context_.req_context = &req_context_;
  }

  std::string EntryFilename(InodeNum parent, std::string filename) {
    return (boost::format("%s/000/%016x/%s") % data_path_ % parent % filename)
        .str();
  }

  std::string InodeFilename(InodeNum inode) {
    return (boost::format("%s/000/%016x") % data_path_ % inode).str();
  }
};

const uint32_t MSStoreCheckerTest::kTestUid = 100;
const uint32_t MSStoreCheckerTest::kTestGid = 100;
const uint64_t MSStoreCheckerTest::kTestOptime = 1234567890;

TEST_F(MSStoreCheckerTest, Empty) {
  store_->Initialize();
  MockIConsole* console;
  checker_.reset(MakeStoreChecker(data_path_, console = new MockIConsole()));
  EXPECT_TRUE(checker_->Run(false));
  EXPECT_EQ(std::string(data_path_), checker_->GetRoot());
  EXPECT_EQ(InodeFilename(1), checker_->GetInodePath(1));
  EXPECT_EQ(EntryFilename(1, "hello"),
            checker_->GetDentryPath(1, "hello"));
  EXPECT_TRUE(checker_->GetStore());
  EXPECT_EQ(console, checker_->GetConsole());
}

TEST_F(MSStoreCheckerTest, Basic) {
  // Create store with some data
  store_->Initialize();
  boost::scoped_ptr<IServerInfo> server_info(MakeServerInfo(data_path_));
  server_info->Set("hello", "world");
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(_))
      .Times(AnyNumber());

  CreateDirReq d_create_req = {3, 0755};
  FileAttr fa;
  EXPECT_EQ(0, store_->Mkdir(&op_context_, 1, "testd1", &d_create_req,
                             &fa));
  CreateReq f_create_req = {4, 0, 0755};
  std::vector<GroupId> groups;
  groups.push_back(1);
  groups.push_back(2);
  boost::scoped_ptr<RemovedInodeInfo> removed_info;
  EXPECT_EQ(0, store_->Create(&op_context_, 3, "testf1", &f_create_req, &groups,
                              &fa, &removed_info));
  EXPECT_EQ(0, store_->Link(&op_context_, 4, 3, "testl1", &fa));

  Mock::VerifyAndClear(inode_src_.get());
  Mock::VerifyAndClear(ugid_handler_.get());

  // Add some redundant entries
  mkdir((InodeFilename(1) + ".x").c_str(), 0777);
  mkdir((InodeFilename(1) + "zz").c_str(), 0777);
  mkdir(EntryFilename(1, "hello").c_str(), 0777);

  MockIConsole* console;
  checker_.reset(MakeStoreChecker(data_path_, console = new MockIConsole()));
  MockIStoreCheckerPlugin* plugin = new MockIStoreCheckerPlugin();
  checker_->RegisterPlugin(plugin);

  EXPECT_CALL(*plugin, Init(_, true))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InfoFound("hello", "world"))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InfoScanCompleted())
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(1, _, 0))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(1, _, -1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(3, _, 0))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(3, _, -1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(4, _, 0))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(4, _, -1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(4, _, 1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, UnknownNodeFound("000/0000000000000001.x"))
      .WillOnce(Throw(SkipItemException(".../testd2", false)));
  EXPECT_CALL(*plugin, UnknownNodeFound("000/0000000000000001zz"))
      .WillOnce(Throw(SkipItemException(".../testd2", false)));
  EXPECT_CALL(*plugin, InodeScanCompleted())
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, DentryFound(1, "testd1", 3, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, DentryFound(3, "testf1", 4, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, DentryFound(3, "testl1", 4, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, UnknownDentryFound(1, "hello"))
      .WillOnce(Throw(SkipItemException(".../testd2", false)));
  EXPECT_CALL(*plugin, DentryScanCompleted())
      .WillOnce(Return(true));

  EXPECT_TRUE(checker_->Run(true));
}

TEST_F(MSStoreCheckerTest, Unrecognized) {
  // Create store with some data
  store_->Initialize();

  // Add some redundant entries
  mkdir((InodeFilename(1) + ".x").c_str(), 0777);
  mkdir(EntryFilename(1, "hello").c_str(), 0777);

  MockIConsole* console;
  checker_.reset(MakeStoreChecker(data_path_, console = new MockIConsole()));
  MockIStoreCheckerPlugin* plugin = new MockIStoreCheckerPlugin();
  checker_->RegisterPlugin(plugin);

  EXPECT_CALL(*plugin, Init(_, true))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InfoScanCompleted())
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(1, _, 0))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(1, _, -1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, UnknownNodeFound("000/0000000000000001.x"))
      .WillOnce(Return(true));
  EXPECT_CALL(*console, PrintLine(StartsWith("Unrecognized inode entry ")));
  EXPECT_CALL(*plugin, InodeScanCompleted())
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, UnknownDentryFound(1, "hello"))
      .WillOnce(Return(true));
  EXPECT_CALL(*console, PrintLine(StartsWith("Unrecognized dentry entry ")));
  EXPECT_CALL(*plugin, DentryScanCompleted())
      .WillOnce(Return(true));

  EXPECT_FALSE(checker_->Run(true));
}
TEST_F(MSStoreCheckerTest, StopBySkip) {
  store_->Initialize();
  boost::scoped_ptr<IServerInfo> server_info(MakeServerInfo(data_path_));
  server_info->Set("hello", "world");
  EXPECT_CALL(*ugid_handler_, SetFsIdsProxied(100, 100))
      .WillRepeatedly(ReturnNew<MockIUgidSetterGuard>());
  EXPECT_CALL(*inode_src_, NotifyUsed(_))
      .Times(AnyNumber());

  CreateDirReq create_req = {3, 0744};
  FileAttr fa;
  EXPECT_EQ(0, store_->Mkdir(&op_context_, 1, "testd1", &create_req, &fa));
  create_req.new_inode = 4;
  EXPECT_EQ(0, store_->Mkdir(&op_context_, 3, "testd2", &create_req, &fa));

  Mock::VerifyAndClear(inode_src_.get());
  Mock::VerifyAndClear(ugid_handler_.get());

  MockIConsole* console;
  checker_.reset(MakeStoreChecker(data_path_, console = new MockIConsole()));
  MockIStoreCheckerPlugin* plugin = new MockIStoreCheckerPlugin();
  checker_->RegisterPlugin(plugin);

  EXPECT_CALL(*plugin, Init(_, false))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InfoFound("hello", "world"))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InfoScanCompleted())
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(1, _, 0))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(1, _, -1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(3, _, 0))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(3, _, -1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(4, _, 0))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeFound(4, _, -1))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, InodeScanCompleted())
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, DentryFound(1, "testd1", 3, _))
      .WillOnce(Return(true));
  EXPECT_CALL(*plugin, DentryFound(3, "testd2", 4, _))
      .WillOnce(Throw(SkipItemException(".../testd2")));
  EXPECT_CALL(*plugin, DentryScanCompleted())
      .WillOnce(Return(true));

  EXPECT_FALSE(checker_->Run(false));
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
