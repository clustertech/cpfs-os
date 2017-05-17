/* Copyright 2014 ClusterTech Ltd */
#include "checksum_validator.hpp"

#include <sys/stat.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/format.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "ds_iface.hpp"
#include "finfo.hpp"
#include "log_testlib.hpp"
#include "logger.hpp"
#include "mock_actions.hpp"
#include "posix_fs.hpp"
#include "posix_fs_impl.hpp"
#include "server/ds/store.hpp"
#include "server/ds/store_impl.hpp"

using ::testing::AnyNumber;
using ::testing::ContainsRegex;
using ::testing::_;

namespace cpfs {
namespace server {
namespace ds {
namespace {

const char* kDataPath = "/tmp/checksum_validator_test_XXXXXX";

typedef std::vector<boost::shared_ptr<IStore> > Stores;

class ChecksumValidatorTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<IPosixFS> posix_fs_;
  MockDataPathMgr data_path_mgr_;
  char* data_path_;

  ChecksumValidatorTest()
      : posix_fs_(MakePosixFS()),
        data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()) {}

  Stores Initialize(bool no_role = false) {
    Stores ret;
    for (int i = 1; i <= 5; ++i) {
      std::string path = std::string(data_path_) +
          (boost::format("/ds%d") % i).str();
      ret.push_back(
          boost::shared_ptr<IStore>(MakeStore(path)));
      mkdir((path + "/r").c_str(), 0777);
      ret.back()->SetPosixFS(posix_fs_.get());
      if (!no_role)
        ret.back()->SetRole(0, i - 1);
    }
    return ret;
  }
};

TEST_F(ChecksumValidatorTest, NoRootPath) {
  uint64_t num_checked;
  EXPECT_THROW(ValidateChecksum("/no_such_path", &num_checked),
               std::runtime_error);
}

TEST_F(ChecksumValidatorTest, MissingDS) {
  // No DS folders
  uint64_t num_checked;
  EXPECT_TRUE(ValidateChecksum(data_path_, &num_checked));
  EXPECT_EQ(0U, num_checked);
}

TEST_F(ChecksumValidatorTest, MissingRole) {
  // DS folders without role
  Initialize(true);
  uint64_t num_checked;
  EXPECT_TRUE(ValidateChecksum(data_path_, &num_checked));
  EXPECT_EQ(0U, num_checked);
}

TEST_F(ChecksumValidatorTest, EmptyDS) {
  // Empty DS folders
  Initialize();
  uint64_t num_checked;
  EXPECT_TRUE(ValidateChecksum(data_path_, &num_checked));
  EXPECT_EQ(0U, num_checked);
}

TEST_F(ChecksumValidatorTest, ChecksumSuccess) {
  // Populate DS with valid data
  Stores stores = Initialize();
  GroupId groups[] = {0};
  FileCoordManager coord_mgr(2, groups, 1);
  std::size_t off = 100 * kSegmentSize;
  std::vector<Segment> segments;
  coord_mgr.GetSegments(off, 6, &segments);
  char data[] = "hello";
  char checksum_changes[6];
  FSTime time = {0, 0};
  stores[segments[0].dsr_data]->
      Write(2, time, off + 6, DsgToDataOffset(segments[0].dsg_off),
            data, 6, checksum_changes);
  stores[segments[0].dsr_checksum]->
      ApplyDelta(2, time, off + 6, DsgToChecksumOffset(segments[0].dsg_off),
                 checksum_changes, 6);
  stores.clear();

  uint64_t num_checked;
  EXPECT_TRUE(ValidateChecksum(data_path_, &num_checked));
  EXPECT_EQ(1U, num_checked);
}

TEST_F(ChecksumValidatorTest, ChecksumError) {
  // Populate DS with invalid data
  Stores stores = Initialize();
  GroupId groups[] = {0};
  FileCoordManager coord_mgr(2, groups, 1);
  std::vector<Segment> segments;
  std::size_t off = 100 * kSegmentSize;
  coord_mgr.GetSegments(off, 6, &segments);
  char data[] = "hello";
  char checksum_changes[6];
  FSTime time = {0, 0};
  stores[segments[0].dsr_data]->
      Write(2, time, off + 6, DsgToDataOffset(segments[0].dsg_off),
            data, 6, checksum_changes);
  --checksum_changes[5];
  stores[segments[0].dsr_checksum]->
      ApplyDelta(2, time, off + 6, DsgToChecksumOffset(segments[0].dsg_off),
                 checksum_changes, 6);
  stores.clear();

  MockLogCallback callback;
  LogRoute route(callback.GetLogCallback());
  EXPECT_CALL(callback, Call(PLEVEL(notice, Server), _, _))
      .Times(AnyNumber());
  EXPECT_CALL(callback, Call(PLEVEL(error, Store),
                             ContainsRegex(
                                 "Inode 0000000000000002 is corrupted"
                                 " at offset 3276805"), _));
  uint64_t num_checked;
  EXPECT_FALSE(ValidateChecksum(data_path_, &num_checked));
  EXPECT_EQ(1U, num_checked);
}

}  // namespace
}  // namespace ds
}  // namespace server
}  // namespace cpfs
