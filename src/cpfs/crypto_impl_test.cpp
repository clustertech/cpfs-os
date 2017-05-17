/* Copyright 2014 ClusterTech Ltd */
#include "crypto_impl.hpp"

#include <cstring>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "crypto.hpp"

using ::testing::_;
using ::testing::Mock;

namespace cpfs {
namespace {

class CryptoTest : public ::testing::Test {
 protected:
  boost::scoped_ptr<ICrypto> crypto_;
  CryptoTest() : crypto_(MakeCrypto()) {}
};

TEST_F(CryptoTest, CreateNonce) {
  std::vector<Nonce> nonces(2);
  nonces[0] = crypto_->CreateNonce();
  nonces[1] = crypto_->CreateNonce();
  EXPECT_GT(nonces[0], 0U);
  EXPECT_GT(nonces[1], 0U);
  EXPECT_NE(nonces[0], nonces[1]);
}

TEST_F(CryptoTest, Sign) {
  std::vector<Nonce> nonces(2);
  nonces[0] = 123;
  nonces[1] = 456;
  // Different key results in different signature
  std::string data1 = "data1";
  char buffer1[20];
  crypto_->Sign(std::string(32, 'a'), data1, buffer1);
  char buffer2[20];
  crypto_->Sign(std::string(32, 'b'), data1, buffer2);
  EXPECT_NE(0, memcmp(buffer1, buffer2, 20));
  // Different data order results in different signature
  std::string data2 = "data2";
  char buffer3[20];
  crypto_->Sign(std::string(32, 'b'), data2, buffer3);
  EXPECT_NE(0, memcmp(buffer2, buffer3, 20));
}

TEST_F(CryptoTest, CreateKey) {
  std::string keys[2];
  keys[0] = crypto_->CreateKey();
  keys[1] = crypto_->CreateKey();
  EXPECT_EQ(32U, keys[0].size());
  EXPECT_EQ(32U, keys[1].size());
  EXPECT_NE(keys[0], keys[1]);
}

}  // namespace
}  // namespace cpfs
