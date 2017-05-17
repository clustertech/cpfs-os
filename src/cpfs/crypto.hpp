#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define Crypto.
 */
#include <inttypes.h>
#include <stdint.h>

#include <string>

#include "common.hpp"

namespace cpfs {

const std::size_t kNonceSize =  /**< # of bytes for nonce */
    sizeof(Nonce) / sizeof(uint8_t);  // NOLINT(readability/function)
const std::size_t kChallengeSize = 20;  /**< # of bytes for AES-128 encrypt */
const std::size_t kEncryptKeySize = 16;  /**< # of bytes for encrypt key */

/**
 * Perform cryptographic functions
 */
class ICrypto {
 public:
  virtual ~ICrypto() {}

  /**
   * Create a randomly generated nonce
   *
   * @return The nonce generated
   */
  virtual Nonce CreateNonce() = 0;

  /**
   * Create a randomly generated key
   *
   * @return The key generated
   */
  virtual std::string CreateKey() = 0;

  /**
   * Sign the data provided
   *
   * @param key The encryption key
   *
   * @param data The data to sign
   *
   * @param dest The destination to store the signed data
   */
  virtual void Sign(const std::string& key,
                    const std::string& data, char* dest) = 0;
};

}  // namespace cpfs
