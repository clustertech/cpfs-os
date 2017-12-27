#pragma once

/* Copyright 2017 ClusterTech Ltd */

/**
 * @file
 *
 * Header for the implementation of the VFS interface.
 */

namespace cpfs {
namespace client {

class IFSCommonLL;

/**
 * Create an implementation of IFSCommonLL.
 */
IFSCommonLL* MakeFSCommonLL();

}  // namespace client

}  // namespace cpfs
