#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "asio_policy.hpp"  // IWYU pragma: export
#include "mock_helper.hpp"

namespace cpfs {

#define OBJ_METHODS                                                     \
  ((io_service, IOService*,))                                           \
  ((Post, void, (ServiceTask)))                                         \
  ((SocketIsOpen, bool, (TcpSocket*)))                                  \
  ((SetTcpNoDelay, void, (TcpSocket*)))                                 \
  ((GetRemoteEndpoint, TcpEndpoint, (TcpSocket*)))                      \
  ((GetLocalEndpoint, TcpEndpoint, (TcpSocket*)))                       \
  ((DuplicateSocket, TcpSocket*, (TcpSocket*)))                         \
  ((AsyncRead, void, (TcpSocket*)(const MIOBufs&)(SIOHandler)))         \
  ((AsyncWrite, void, (TcpSocket*)(const CIOBufs&)(SIOHandler)))        \
  ((TcpResolve, TcpEndpoint, (const char*)(const char*)))               \
  ((ParseHostPort, void,                                                \
    (const std::string&)(std::string*)(int*)))                          \
  ((MakeAcceptor, TcpAcceptor*, (const TcpEndpoint&)))                  \
  ((AsyncAccept, void, (TcpAcceptor*)(TcpSocket*)(IOHandler)))          \
  ((AsyncConnect, void, (TcpSocket*)(TcpEndpoint)(IOHandler)))          \
  ((MakeDeadlineTimer, DeadlineTimer*,))                                \
  ((SetDeadlineTimer, void, (DeadlineTimer*)(double)(IOHandler)))       \
  ((MakeSignalSet, SignalSet*,))                                        \
  ((SetSignalHandler, void,                                             \
    (SignalSet*)(const int[])(unsigned)(SignalHandler)))

class MockIAsioPolicy : public IAsioPolicy {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace cpfs
