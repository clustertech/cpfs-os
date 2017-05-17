/* Copyright 2013 ClusterTech Ltd */
#include "server/reply_set_impl.hpp"

#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "common.hpp"
#include "fim.hpp"
#include "fims.hpp"
#include "server/reply_set.hpp"

namespace cpfs {
namespace server {
namespace {

TEST(ReplySetTest, BasicAPI) {
  boost::scoped_ptr<IReplySet> rs(MakeReplySet());
  EXPECT_FALSE(rs->FindReply(42));
  EXPECT_TRUE(rs->IsEmpty());
  FIM_PTR<ResultCodeReplyFim> fim(new ResultCodeReplyFim);
  fim->set_req_id(42);
  (*fim)->err_no = 0;
  rs->AddReply(fim);
  EXPECT_EQ(fim, rs->FindReply(42));
  EXPECT_FALSE(rs->IsEmpty());
  FIM_PTR<ResultCodeReplyFim> fim2(new ResultCodeReplyFim);
  fim2->set_req_id(42);
  (*fim2)->err_no = 5;
  rs->AddReply(fim2);
  EXPECT_EQ(fim2, rs->FindReply(42));
  rs->ExpireReplies(60);
  EXPECT_EQ(fim2, rs->FindReply(42));
  EXPECT_FALSE(rs->IsEmpty());
  rs->ExpireReplies(0);
  EXPECT_FALSE(rs->FindReply(42));
  EXPECT_TRUE(rs->IsEmpty());
}

TEST(ReplySetTest, RemoveClient) {
  boost::scoped_ptr<IReplySet> rs(MakeReplySet());
  FIM_PTR<ResultCodeReplyFim> fim(new ResultCodeReplyFim);
  fim->set_req_id(42);
  (*fim)->err_no = 0;
  rs->AddReply(fim);
  ReqId big_id = (1ULL << (64 - kClientBits)) + 42;
  FIM_PTR<ResultCodeReplyFim> fim2(new ResultCodeReplyFim);
  fim2->set_req_id(big_id);
  (*fim2)->err_no = 5;
  rs->AddReply(fim2);
  EXPECT_EQ(fim, rs->FindReply(42));
  EXPECT_EQ(fim2, rs->FindReply(big_id));
  rs->RemoveClient(1);
  EXPECT_EQ(fim, rs->FindReply(42));
  EXPECT_FALSE(rs->FindReply(big_id));
  rs->RemoveClient(0);
  EXPECT_FALSE(rs->FindReply(42));
}

}  // namespace
}  // namespace server
}  // namespace cpfs
