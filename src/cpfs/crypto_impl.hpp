#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of Crypto.
 */

namespace cpfs {

class ICrypto;

/**
 * Get the crypto
 */
ICrypto* MakeCrypto();

}  // namespace cpfs
