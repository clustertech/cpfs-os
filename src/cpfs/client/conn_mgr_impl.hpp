#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header file for implementation of cpfs::client::IConnMgr for
 * initializing connections in FC.
 */

namespace cpfs {
namespace client {

class BaseClient;
class IConnMgr;

/**
 * Create a ConnMgr.
 *
 * @param client The client using the manager
 *
 * @param type The type of connection to make: 'F' for FC and 'A' for Admin
 */
IConnMgr* MakeConnMgr(BaseClient* client, char type);

}  // namespace client
}  // namespace cpfs
