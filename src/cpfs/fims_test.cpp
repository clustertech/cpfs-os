/* Copyright 2013 ClusterTech Ltd */
#include "fims.hpp"

#include <cstring>
#include <stdexcept>
#include <string>

#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "log_testlib.hpp"
#include "logger.hpp"

using ::testing::_;
using ::testing::StrEq;

namespace cpfs {

// Explicit instantiate class template to better detect missing coverage
template class BaseFim<void, 1>;

namespace {

TEST(FimTest, DirectConstruction) {
  ReadFim rf(20);
  rf.set_req_id(5);
  rf->inode = 10;
  const ReadFim& rfr(rf);
  EXPECT_EQ(10U, rfr->inode);
  EXPECT_EQ(10U, (*rf).inode);
  EXPECT_EQ(ReqId(5), rf.req_id());
  EXPECT_EQ(kReadFim, rf.type());
  EXPECT_EQ(false, rf.is_final());
  EXPECT_EQ(uint64_t(1), rf.protocol());
  EXPECT_EQ(uint32_t(20), rf.tail_buf_size());
  EXPECT_EQ(uint32_t(32), rf.tail_buf() - rf.body());
  EXPECT_EQ(uint32_t(52), rf.body_size());
  EXPECT_FALSE(rf.IsReply());
  rf.set_final(true);
  EXPECT_EQ(kReadFim, rf.type());
  EXPECT_EQ(true, rf.is_final());
  rf.set_final(false);
  EXPECT_EQ(kReadFim, rf.type());
  EXPECT_EQ(false, rf.is_final());

  DataReplyFim df_reply;
  df_reply.set_req_id(5);
  EXPECT_EQ(kDataReplyFim, df_reply.type());
  EXPECT_EQ(uint64_t(1), df_reply.protocol());
  EXPECT_EQ(uint32_t(0), df_reply.tail_buf_size());
  EXPECT_TRUE(df_reply.IsReply());
  FIM_PTR<HeartbeatFim> sf = HeartbeatFim::MakePtr();
  sf->set_req_id(2);
  EXPECT_EQ(ReqId(2), sf->req_id());
  EXPECT_EQ(FimType(kHeartbeatFim), sf->type());
  EXPECT_EQ(uint64_t(1), sf->protocol());
  EXPECT_EQ(uint32_t(0), sf->tail_buf_size());
  EXPECT_FALSE(sf->IsReply());
  EXPECT_EQ("#Q00000-000002(Heartbeat)", boost::lexical_cast<std::string>(*sf));

  MockLogCallback log_callback;
  LogRoute route(log_callback.GetLogCallback());
  EXPECT_CALL(log_callback, Call(_, StrEq("#Q00000-000002(Heartbeat)"), _));

  LOG(notice, Server, PVal(sf));
}

TEST(FimTest, CopyAndAssign) {
  ReadFim rf(20);
  rf.set_req_id(5);
  std::memset(rf.tail_buf(), char(7), 10);
  const ReadFim rf2(rf);
  EXPECT_EQ(ReqId(5), rf2.req_id());
  EXPECT_EQ(char(7), rf2.tail_buf()[0]);
  EXPECT_EQ(uint32_t(20), rf2.tail_buf_size());
  ReadFim rf3, rf4;
  rf3 = rf4 = rf;
  EXPECT_EQ(ReqId(5), rf3.req_id());
  EXPECT_EQ(char(7), rf3.tail_buf()[0]);
  EXPECT_EQ(uint32_t(20), rf3.tail_buf_size());
  EXPECT_EQ(ReqId(5), rf4.req_id());
  EXPECT_EQ(char(7), rf4.tail_buf()[0]);
  EXPECT_EQ(uint32_t(20), rf4.tail_buf_size());
}

TEST(FimTest, Resize) {
  ReadFim rf(20);
  rf->size = 12345;
  std::memset(rf.tail_buf(), char(7), 20);
  rf.tail_buf_resize(30);
  EXPECT_EQ(30U, rf.tail_buf_size());
  std::memset(rf.tail_buf() + 20, char(6), 10);
  rf.tail_buf_resize(21);
  EXPECT_EQ(21U, rf.tail_buf_size());
  EXPECT_EQ(12345U, rf->size);
  EXPECT_EQ(std::string(20, '\007') + '\006', std::string(rf.tail_buf(), 21));
}

TEST(FimTest, FromHead) {
  ReadFim rf;
  rf.set_req_id(5);
  rf->inode = 42;
  rf->dsg_off = 456;
  rf->size = 12345;
  rf->target_group = 0;
  rf->target_role = 2;
  rf->checksum_role = 1;
  FIM_PTR<IFim> head_copied_rf(ReadFim::FromHead(rf.msg()));
  EXPECT_EQ(0, std::memcmp(rf.msg(), head_copied_rf->msg(), sizeof(FimHead)));
  boost::scoped_array<char> dup_msg(new char[rf.len()]);
  std::memcpy(dup_msg.get(), rf.msg(), rf.len());
  reinterpret_cast<FimHead*>(dup_msg.get())->len = -1;
  EXPECT_THROW(ReadFim::FromHead(dup_msg.get()), std::runtime_error);
}

}  // namespace
}  // namespace cpfs
