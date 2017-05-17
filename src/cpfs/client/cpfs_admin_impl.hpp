#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of InodeUsageSet for tracking inode
 * operations.
 */

namespace cpfs {
namespace client {

class BaseAdminClient;
class ICpfsAdmin;

/**
 * Get an Inode usage tracker for FC
 */
ICpfsAdmin* MakeCpfsAdmin(BaseAdminClient* client);

}  // namespace client
}  // namespace cpfs
