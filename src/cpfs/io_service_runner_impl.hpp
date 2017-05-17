#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the cpfs::IOServiceRunner for delay
 * the start of Asio io_service(s) on thread(s) after io_service(s)
 * are created and initialized.
 */

namespace cpfs {

class IIOServiceRunner;

/**
 * Create a Boost.Asio io_service runner
 */
IIOServiceRunner* MakeIOServiceRunner();

}  // namespace cpfs
