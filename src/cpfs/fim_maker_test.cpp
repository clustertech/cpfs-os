/* Copyright 2013 ClusterTech Ltd */
#include "fim_maker.hpp"

#include <stdint.h>

#include <cstring>
#include <stdexcept>
#include <string>

#include <boost/shared_ptr.hpp>

#include <gtest/gtest.h>

#include "fims.hpp"

namespace cpfs {
namespace {

TEST(FimMakerTest, FimMaker) {
  FimMaker maker;
  maker.RegisterClass<ResultCodeReplyFim>("ResultCodeReply");
  ResultCodeReplyFim orig_fim(10);
  orig_fim.set_req_id(13);
  std::memset(orig_fim.tail_buf(), char(8), 10);
  orig_fim->err_no = 1;
  FIM_PTR<IFim> copied_head_fim(maker.FromHead(orig_fim.msg()));
  EXPECT_EQ(ReqId(13), copied_head_fim->req_id());
  EXPECT_EQ(uint32_t(10), copied_head_fim->tail_buf_size());
  GetattrFim sfim;
  EXPECT_THROW(maker.FromHead(sfim.msg()), std::out_of_range);
  EXPECT_EQ("ResultCodeReply", maker.TypeName(kResultCodeReplyFim));
  EXPECT_EQ("65432", maker.TypeName(65432));
}

TEST(FimMakerTest, FimsMaker) {
  ResultCodeReplyFim fim;
  FIM_PTR<IFim> copied(GetFimsMaker().FromHead(fim.msg()));
  EXPECT_TRUE(copied);
  fim.set_final();
  FIM_PTR<IFim> copied2(GetFimsMaker().FromHead(fim.msg()));
  EXPECT_TRUE(copied2);
  FIM_PTR<IFim> unlink_fim = UnlinkFim::MakePtr(5);
  std::memcpy(unlink_fim->tail_buf(), "test", 5);
  FIM_PTR<IFim> cloned = GetFimsMaker().Clone(unlink_fim);
  EXPECT_EQ(std::string("test"), std::string(cloned->tail_buf()));
}

}  // namespace
}  // namespace cpfs
