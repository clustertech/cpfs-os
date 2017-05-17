#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define common types used by CPFS for asio.
 */

#include <boost/asio.hpp>  // IWYU pragma: export

namespace cpfs {

/** Type for the io_service */
typedef boost::asio::io_service IOService;

/** Type for the tcp::socket */
typedef boost::asio::ip::tcp::socket TcpSocket;

/** Type for the tcp acceptor */
typedef boost::asio::ip::tcp::acceptor TcpAcceptor;

/** Type for the tcp endpoint */
typedef boost::asio::ip::tcp::endpoint TcpEndpoint;

/** Type for the deadline timer */
typedef boost::asio::deadline_timer DeadlineTimer;

/** Type for signal set */
typedef boost::asio::signal_set SignalSet;

}  // namespace cpfs
