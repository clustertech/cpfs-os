/* Copyright 2013 ClusterTech Ltd */
#include "asio_policy_impl.hpp"

#include <csignal>
#include <stdexcept>
#include <string>

#include <boost/array.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "mock_actions.hpp"

using ::testing::_;
using ::testing::MockFunction;

namespace cpfs {
namespace {

void WriteHandler(const boost::system::error_code& error) {
  ASSERT_FALSE(error);
}

std::string data = "hello, world!\n";

void AcceptHandler(IAsioPolicy* policy, TcpSocket* socket,
                   const boost::system::error_code& error) {
  ASSERT_FALSE(error);
  policy->AsyncWrite(socket, boost::asio::buffer(data),
                     boost::bind(WriteHandler,
                                 boost::asio::placeholders::error));
}

void Server() {
  TcpEndpoint bind_addr(boost::asio::ip::tcp::v4(), 12345U);
  boost::scoped_ptr<IAsioPolicy> policy(MakeAsioPolicy());
  boost::scoped_ptr<TcpSocket> socket(new TcpSocket(*policy->io_service()));
  boost::scoped_ptr<TcpAcceptor>
      acceptor(policy->MakeAcceptor(bind_addr));
  policy->AsyncAccept(acceptor.get(), socket.get(), boost::bind(
      AcceptHandler, policy.get(), socket.get(),
      boost::asio::placeholders::error));
  policy->io_service()->run();
}

void ReadHandler(boost::array<char, 14>* buf,
                 const boost::system::error_code& error) {
  ASSERT_FALSE(error);
  EXPECT_EQ(std::string("hello, world!\n"),
            std::string(buf->begin(), buf->end()));
}

void ConnectHandler(IAsioPolicy* policy, TcpSocket* socket,
                    const boost::system::error_code& error) {
  ASSERT_FALSE(error);
  static boost::array<char, 14> buf;
  policy->AsyncRead(socket, boost::asio::buffer(buf), boost::bind(
      ReadHandler, &buf, boost::asio::placeholders::error));
}

TEST(AsioPolicyTest, Sync) {
  boost::scoped_ptr<IAsioPolicy> policy(MakeAsioPolicy());
  std::string host;
  int ip;
  policy->ParseHostPort("localhost:12345", &host, &ip);
  EXPECT_EQ("127.0.0.1", host);
  EXPECT_EQ(12345, ip);
  EXPECT_THROW(policy->ParseHostPort("localhost", &host, &ip),
               std::runtime_error);
}

TEST(AsioPolicyTest, AsioPolicy) {
  boost::thread server_thread(Server);
  Sleep(0.1)();
  boost::scoped_ptr<IAsioPolicy> policy(MakeAsioPolicy());
  TcpEndpoint target_addr = policy->TcpResolve("localhost", "12345");
  boost::scoped_ptr<TcpSocket> socket(new TcpSocket(*policy->io_service()));
  policy->AsyncConnect(socket.get(), target_addr, boost::bind(
      ConnectHandler, policy.get(), socket.get(),
      boost::asio::placeholders::error));
  EXPECT_TRUE(policy->SocketIsOpen(socket.get()));
  policy->SetTcpNoDelay(socket.get());
  policy->io_service()->run();
  TcpEndpoint remote_ep = policy->GetRemoteEndpoint(socket.get());
  ASSERT_EQ("127.0.0.1", boost::lexical_cast<std::string>(remote_ep.address()));
  ASSERT_EQ(12345, remote_ep.port());
  TcpEndpoint local_ep = policy->GetLocalEndpoint(socket.get());
  ASSERT_EQ("127.0.0.1", boost::lexical_cast<std::string>(local_ep.address()));
  ASSERT_NE(12345, local_ep.port());
  boost::scoped_ptr<IAsioPolicy> policy2(MakeAsioPolicy());
  boost::scoped_ptr<TcpSocket> socket2(policy2->DuplicateSocket(socket.get()));
  EXPECT_TRUE(socket2);
  server_thread.join();
}

TEST(AsioPolicyTest, Post) {
  boost::scoped_ptr<IAsioPolicy> policy(MakeAsioPolicy());

  MockFunction<void()> scallback;
  EXPECT_CALL(scallback, Call());

  policy->Post(boost::bind(GetMockCall(scallback), &scallback));
  policy->io_service()->run();
}

TEST(AsioPolicyTest, DeadlineTimer) {
  boost::scoped_ptr<IAsioPolicy> policy(MakeAsioPolicy());
  MockFunction<void(const boost::system::error_code&)> callback;
  boost::scoped_ptr<DeadlineTimer> timer1(policy->MakeDeadlineTimer());
  boost::scoped_ptr<DeadlineTimer> timer2(policy->MakeDeadlineTimer());
  policy->SetDeadlineTimer(
      timer1.get(), 0.01, boost::bind(GetMockCall(callback), &callback, _1));
  policy->SetDeadlineTimer(
      timer2.get(), 0.01, boost::bind(GetMockCall(callback), &callback, _1));
  policy->SetDeadlineTimer(
      timer1.get(), 0, boost::bind(GetMockCall(callback), &callback, _1));
  EXPECT_CALL(callback, Call(boost::system::error_code()))
      .Times(1);
  EXPECT_CALL(callback, Call(boost::system::error_code(
      boost::system::errc::operation_canceled,
      boost::system::system_category())))
      .Times(1);
  policy->io_service()->run();
}

void TriggerTerm() {
  Sleep(0.1)();
  raise(SIGUSR1);
}

TEST(AsioPolicyTest, SignalHandler) {
  boost::scoped_ptr<IAsioPolicy> policy(MakeAsioPolicy());
  boost::scoped_ptr<SignalSet> signal_set(policy->MakeSignalSet());
  MockFunction<void(const boost::system::error_code&, int)> callback;

  int signal = SIGUSR1;
  EXPECT_CALL(callback, Call(_, signal));
  policy->SetSignalHandler(
      signal_set.get(), &signal, 1,
      boost::bind(GetMockCall(callback), &callback, _1, _2));
  boost::thread thread(TriggerTerm);
  policy->io_service()->run();
}

}  // namespace
}  //  namespace cpfs
