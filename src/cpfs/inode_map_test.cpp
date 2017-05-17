/* Copyright 2013 ClusterTech Ltd */
#include "inode_map.hpp"

#include <gtest/gtest.h>

namespace cpfs {
namespace {

class InodeIntMap : public InodeMap<int> {
 protected:
  bool IsUnused(const boost::shared_ptr<int>& elt) const {
    return *elt == 0;
  }
};

TEST(InodeMapTest, BaseAPI) {
  InodeIntMap iim;
  // Initially the element does not exist
  EXPECT_FALSE(iim.HasInode(1));
  EXPECT_FALSE(iim.Get(1));
  EXPECT_EQ(0, *iim[1]);
  EXPECT_TRUE(iim.Get(1));
  *iim[1] = 42;
  EXPECT_TRUE(iim.HasInode(1));
  EXPECT_EQ(42, *iim.Get(1));
  iim.Clean(1);
  EXPECT_TRUE(iim.HasInode(1));
  EXPECT_EQ(42, *iim.Get(1));
  *iim[1] = 0;
  boost::shared_ptr<int> use = iim.Get(1);
  iim.Clean(1);
  EXPECT_TRUE(iim.HasInode(1));
  EXPECT_EQ(0, *iim.Get(1));
  use.reset();
  iim.Clean(1);
  EXPECT_FALSE(iim.HasInode(1));
}

}  // namespace
}  // namespace cpfs
