/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/meta_dir_reader_impl.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/xattr.h>

#include <cstddef>
#include <cstring>
#include <list>
#include <stdexcept>
#include <string>
#include <utility>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "finfo.hpp"
#include "mock_actions.hpp"
#include "store_util.hpp"
#include "server/ms/meta_dir_reader.hpp"

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;

namespace cpfs {
namespace server {
namespace ms {
namespace {

const char* kDataPath = "/tmp/meta_dir_reader_test_XXXXXX";

class MetaDirReaderTest : public ::testing::Test {
 protected:
  MockDataPathMgr data_path_mgr_;
  char* data_path_;
  boost::scoped_ptr<IMetaDirReader> meta_dir_reader_;

  MetaDirReaderTest()
      : data_path_mgr_(kDataPath),
        data_path_(data_path_mgr_.GetPath()),
        meta_dir_reader_(MakeMetaDirReader(data_path_)) {
    // TODO(Joseph): WIP for #13801
    mkdir(GetRoot().c_str(), 0777);
  }

  std::string GetFilename(const char* filename) {
    return GetRoot() + "/" + filename;
  }

  void CreateControlFile(const char* inode,
                         const char* ct, const char* sg = 0) {
    std::string path = GetRoot() + "/" + inode;
    open((path + "x").c_str(), O_CREAT | O_WRONLY, 0666);
    if (ct)
      lsetxattr((path + "x").c_str(), "user.ct", ct, strlen(ct), 0);
    if (sg)
      lsetxattr((path + "x").c_str(), "user.sg", sg, strlen(sg), 0);
  }

  // TODO(Joseph): WIP for #13801
  std::string GetRoot() {
    return std::string(data_path_) + "/000";
  }
};

TEST_F(MetaDirReaderTest, ToInodeFim) {
  // Directory
  std::string root = GetFilename("0000000000000001");
  mkdir(root.c_str(), 0777);
  LSetUserXattr(root.c_str(), "abcd", std::string("test", 5));
  LSetUserXattr(root.c_str(), "ndsg", std::string("2", 2));
  CreateControlFile("0000000000000001", "1-23");
  lsetxattr((root + "x").c_str(), "user.par", "0000000000000001", 16, 0);
  FIM_PTR<ResyncInodeFim> fim1
      = meta_dir_reader_->ToInodeFim("000/0000000000000001");
  EXPECT_EQ(InodeNum(1), (*fim1)->inode);
  const InodeNum& parent =
      reinterpret_cast<const InodeNum&>(*(fim1->tail_buf()));
  EXPECT_EQ(InodeNum(1), parent);
  std::size_t attr_size = fim1->tail_buf_size() - (*fim1)->extra_size;
  std::string xattr_buf;
  xattr_buf.resize(attr_size);
  std::memcpy(&xattr_buf[0], fim1->tail_buf() + (*fim1)->extra_size, attr_size);
  XAttrList attr_list = BufferToXattrList(xattr_buf);
  attr_list.sort();
  EXPECT_THAT(attr_list,
              ElementsAre(std::make_pair("abcd", std::string("test", 5)),
                          std::make_pair("ndsg", std::string("2", 2))));
  // Regular file
  std::string file = GetFilename("0000000000000002");
  open(file.c_str(), O_CREAT | O_WRONLY, 0666);
  truncate(file.c_str(), 8192);
  CreateControlFile("0000000000000002", "1-23", "0,1");
  FIM_PTR<ResyncInodeFim> fim2
      = meta_dir_reader_->ToInodeFim("000/0000000000000002");
  EXPECT_EQ(InodeNum(2), (*fim2)->inode);
  EXPECT_EQ(8192U, (*fim2)->fa.size);
  EXPECT_EQ(GroupId(0), reinterpret_cast<GroupId*>(fim2->tail_buf())[0]);
  EXPECT_EQ(GroupId(1), reinterpret_cast<GroupId*>(fim2->tail_buf())[1]);

  // Soft link
  std::string soft_link = GetFilename("0000000000000003");
  symlink("../test", soft_link.c_str());
  CreateControlFile("0000000000000003", "1-23");
  FIM_PTR<ResyncInodeFim> fim3
      = meta_dir_reader_->ToInodeFim("000/0000000000000003");
  EXPECT_EQ(std::string("../test"), std::string(fim3->tail_buf()));
}

TEST_F(MetaDirReaderTest, ToInodeFimError) {
  // Not exists
  EXPECT_THROW(meta_dir_reader_->ToInodeFim("000/0000000000000000"),
               std::runtime_error);
}

TEST_F(MetaDirReaderTest, ToDentryFim) {
  std::string root = GetRoot() + "/0000000000000001";
  std::string dir = root + "/test1";
  std::string regularf = root + "/test2";
  mkdir(root.c_str(), 0777);
  mkdir(dir.c_str(), 0777);
  lsetxattr(dir.c_str(), "user.ino", "0000000000000003", 16, 0);
  symlink("K0000000000000004", regularf.c_str());

  FIM_PTR<ResyncDentryFim> fim1
      = meta_dir_reader_->ToDentryFim("000/0000000000000001", "test1", 'D');
  EXPECT_EQ(DT_DIR, (*fim1)->type);
  EXPECT_EQ(InodeNum(3), (*fim1)->target);

  FIM_PTR<ResyncDentryFim> fim2
      = meta_dir_reader_->ToDentryFim("000/0000000000000001", "test2", 'F');
  EXPECT_EQ(DT_BLK, (*fim2)->type);
  EXPECT_EQ(InodeNum(4), (*fim2)->target);
}

TEST_F(MetaDirReaderTest, ToDentryFimError) {
  // Not exists
  EXPECT_THROW(
      meta_dir_reader_->ToDentryFim("000/0000000000000001", "test1", 'D'),
      std::runtime_error);
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
