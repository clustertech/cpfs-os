#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of AdminClient
 */

namespace cpfs {

struct AdminConfigItems;

namespace client {

class BaseAdminClient;

/**
 * Create an Admin Client
 *
 * @param configs The admin configs
 */
BaseAdminClient* MakeAdminClient(const AdminConfigItems& configs);

}  // namespace client
}  // namespace cpfs
