/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of Crypto.
 */

#include "crypto.hpp"

#include <stdint.h>

#include <botan/auto_rng.h>
#include <botan/botan.h>
#include <botan/hex.h>

#include <cstring>
#include <string>

#include "common.hpp"

namespace cpfs {

namespace {

/**
 * Implementation of the Crypto
 */
class Crypto : public ICrypto {
 public:
  Nonce CreateNonce() {
    Nonce encoded;
    rng_.randomize(reinterpret_cast<uint8_t*>(&encoded), kNonceSize);
    return encoded;
  }

  std::string CreateKey() {
    Botan::SecureVector<Botan::byte> key = rng_.random_vec(kEncryptKeySize);
    return Botan::hex_encode(key, kEncryptKeySize, true);
  }

  void Sign(const std::string& key,
            const std::string& data, char* dest) {
    Botan::SecureVector<Botan::byte> ret;
    Botan::Pipe pipe(
        Botan::get_cipher(
            "AES-128/CBC", Botan::SymmetricKey(key), Botan::ENCRYPTION),
        new Botan::Hash_Filter("SHA-1"));
    pipe.start_msg();
    pipe.write(data);
    pipe.end_msg();
    ret = pipe.read_all(0);
    std::memcpy(dest, ret.begin(), ret.size());
  }

 private:
  Botan::LibraryInitializer init_;  // Initialize the botan library
  Botan::AutoSeeded_RNG rng_;  // The random generator
};

}  // namespace

ICrypto* MakeCrypto() {
  return new Crypto;
}

}  // namespace cpfs
