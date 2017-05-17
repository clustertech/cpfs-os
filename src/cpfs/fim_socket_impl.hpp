#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of cpfs::IFimSocket interface for send
 * and receive FIM messages over an Asio-type socket.
 */

#include "fim_socket.hpp"

namespace cpfs {

/**
 * Default FimSocketMaker to create IFimSocket instances.
 */
extern FimSocketMaker kFimSocketMaker;

}  // namespace cpfs
