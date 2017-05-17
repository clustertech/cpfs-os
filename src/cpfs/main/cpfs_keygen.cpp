/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for generating secret key for CPFS system protection
 */
#include <sys/stat.h>

#include <cstdio>
#include <string>

#include <boost/scoped_ptr.hpp>

#include "crypto.hpp"
#include "crypto_impl.hpp"

/**
 * Utility main
 */
int main(int argc, char** argv) {
  boost::scoped_ptr<cpfs::ICrypto> crypto(cpfs::MakeCrypto());
  if (argc != 2) {
    std::fprintf(stderr, "Usage: <The path to store the key>\n");
    return 2;
  }
  const char* key_path = argv[1];
  FILE* fp = fopen(key_path, "w");
  if (!fp) {
    std::fprintf(stderr, "Cannot open %s for write \n", key_path);
    return 2;
  }
  std::string key = crypto->CreateKey();
  fwrite(key.data(), 1, key.length(), fp);
  fclose(fp);
  chmod(key_path, 0600);
  std::fprintf(stderr, "The key is created %s \n", key_path);
  return 0;
}
