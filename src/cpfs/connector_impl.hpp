#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the cpfs::Connector to create
 * FimSocket by making connections with Asio.
 */

namespace cpfs {

class IAsioPolicy;
class IAuthenticator;
class IConnector;

/**
 * Create an instance of IConnector.
 *
 * @param asio_policy The policy to perform async IO operations
 *
 * @param authenticator The authenticator to authenticate connections
 *
 * @param monitor_accepted Whether accepted connections should use heartbeat
 */
IConnector* MakeConnector(
    IAsioPolicy* asio_policy,
    IAuthenticator* authenticator,
    bool monitor_accepted = true);

}  // namespace cpfs
