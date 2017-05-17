/* Copyright 2013 ClusterTech Ltd */
#include "timed_list.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mock_actions.hpp"

using ::testing::ElementsAre;

namespace cpfs {
namespace {

TEST(TimedList, BasicAPI) {
  TimedList<int> entries;
  entries.Add(1);
  entries.Add(3);
  EXPECT_FALSE(entries.IsEmpty());
  Sleep(0.2);
  TimedList<int>::Elems relems;
  entries.Expire(0, &relems);
  EXPECT_THAT(relems, ElementsAre(1, 3));
  EXPECT_TRUE(entries.IsEmpty());
  entries.Add(5);
  entries.Add(7);
  entries.Expire(5);
  EXPECT_FALSE(entries.IsEmpty());
  relems = entries.FetchAndClear();
  EXPECT_TRUE(entries.IsEmpty());
  EXPECT_THAT(relems, ElementsAre(5, 7));
}

}  // namespace
}  // namespace cpfs
