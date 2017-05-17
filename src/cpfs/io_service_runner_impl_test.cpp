/* Copyright 2013 ClusterTech Ltd */
#include "io_service_runner_impl.hpp"

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "io_service_runner.hpp"
#include "mock_actions.hpp"

namespace cpfs {
namespace {

class Handler {
 public:
  MOCK_METHOD0(TestFunc1, void());
  MOCK_METHOD0(TestFunc2, void());
};

TEST(IOServiceRunnerTest, Run) {
  Handler handler1, handler2, handler3;
  IOService service1, service2, service3;
  service1.post(boost::bind(&Handler::TestFunc1, &handler1));
  service2.post(boost::bind(&Handler::TestFunc2, &handler2));
  EXPECT_CALL(handler1, TestFunc1());
  EXPECT_CALL(handler2, TestFunc2());

  boost::scoped_ptr<IIOServiceRunner> runner(MakeIOServiceRunner());
  runner->AddService(&service1);
  runner->AddService(&service2);
  runner->Run();

  // Add new service and run
  runner->AddService(&service3);
  service3.post(boost::bind(&Handler::TestFunc1, &handler3));

  EXPECT_CALL(handler3, TestFunc1());

  runner->Run();
  Sleep(0.5)();

  // Try adding more task after service is emptied
  EXPECT_CALL(handler3, TestFunc1());

  service3.post(boost::bind(&Handler::TestFunc1, &handler3));
  Sleep(0.5)();

  // Stop the services
  runner->Stop();
  runner->Join();
}

}  // namespace
}  // namespace cpfs
