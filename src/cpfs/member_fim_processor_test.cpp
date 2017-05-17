/* Copyright 2013 ClusterTech Ltd */
#include "member_fim_processor.hpp"

#include <boost/function.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "fim_socket_mock.hpp"
#include "fims.hpp"

using ::testing::Return;

namespace cpfs {
namespace {

class MyFimProcessor : public MemberFimProcessor<MyFimProcessor> {
 public:
  MyFimProcessor() {
    AddHandler(&MyFimProcessor::ReadHandler);
  }

  MOCK_METHOD2(ReadHandler, bool(const FIM_PTR<ReadFim>&,
                                 const boost::shared_ptr<IFimSocket>&));
};

TEST(MemberFimProcessorTest, Handler) {
  MyFimProcessor fim_processor;
  boost::shared_ptr<MockIFimSocket> fim_socket =
      boost::make_shared<MockIFimSocket>();

  // Send it a WriteFim
  EXPECT_FALSE(fim_processor.Process(WriteFim::MakePtr(), fim_socket));

  // Send it a ReadFim
  FIM_PTR<ReadFim> read_fim = ReadFim::MakePtr();
  EXPECT_CALL(fim_processor, ReadHandler(
      read_fim, boost::shared_ptr<IFimSocket>(fim_socket)))
      .WillOnce(Return(true));

  EXPECT_TRUE(fim_processor.Process(read_fim, fim_socket));
}

}  // namespace
}  // namespace cpfs
