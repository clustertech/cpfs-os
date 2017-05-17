/* Copyright 2013 ClusterTech Ltd */
#include "composite_fim_processor.hpp"

#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"

using ::testing::Return;

namespace cpfs {
namespace {

TEST(CompositeFimProcessorTest, Basic) {
  CompositeFimProcessor comp_proc;
  MockIFimProcessor* sub_proc1 = new MockIFimProcessor;
  MockIFimProcessor* sub_proc2 = new MockIFimProcessor;
  comp_proc.AddFimProcessor(sub_proc1);
  comp_proc.AddFimProcessor(sub_proc2);

  FIM_PTR<IFim> fim(new ReadFim);
  fim->set_req_id(5);
  boost::shared_ptr<IFimSocket> fim_socket(new MockIFimSocket);

  // Neither subprocessor return true
  EXPECT_CALL(*sub_proc1, Process(fim, fim_socket));
  EXPECT_CALL(*sub_proc2, Process(fim, fim_socket));
  EXPECT_FALSE(comp_proc.Process(fim, fim_socket));

  // Second subprocessor return true
  EXPECT_CALL(*sub_proc1, Process(fim, fim_socket));
  EXPECT_CALL(*sub_proc2, Process(fim, fim_socket))
      .WillOnce(Return(true));
  EXPECT_TRUE(comp_proc.Process(fim, fim_socket));

  // First subprocessor return true
  EXPECT_CALL(*sub_proc1, Process(fim, fim_socket))
      .WillOnce(Return(true));
  EXPECT_TRUE(comp_proc.Process(fim, fim_socket));

  // Check accept
  EXPECT_CALL(*sub_proc1, Accept(fim))
      .WillOnce(Return(false))
      .WillOnce(Return(false));
  EXPECT_CALL(*sub_proc2, Accept(fim))
      .WillOnce(Return(true))
      .WillOnce(Return(false));
  EXPECT_TRUE(comp_proc.Accept(fim));
  EXPECT_FALSE(comp_proc.Accept(fim));
}

TEST(CompositeFimProcessorTest, NonFreeing) {
  CompositeFimProcessor comp_proc;
  MockIFimProcessor sub_proc1;
  comp_proc.AddFimProcessor(&sub_proc1, true);

  FIM_PTR<IFim> fim(new ReadFim);
  fim->set_req_id(5);
  boost::shared_ptr<IFimSocket> fim_socket(new MockIFimSocket);

  // subprocessor return false, ensure that sub_proc1 get called
  EXPECT_CALL(sub_proc1, Process(fim, fim_socket));

  EXPECT_FALSE(comp_proc.Process(fim, fim_socket));
}

}  // namespace
}  // namespace cpfs
