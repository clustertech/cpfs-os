#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of Authenticator.
 */
namespace cpfs {

class IAsioPolicy;
class IAuthenticator;
class ICrypto;

/**
 * Get an authenticator
 *
 * @param crypto The crypto
 *
 * @param asio_policy The asio_policy to run periodic timer on
 */
IAuthenticator* MakeAuthenticator(ICrypto* crypto, IAsioPolicy* asio_policy);

}  // namespace cpfs
