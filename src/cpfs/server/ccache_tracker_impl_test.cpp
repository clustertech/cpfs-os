/* Copyright 2013 ClusterTech Ltd */
#include "server/ccache_tracker_impl.hpp"

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.hpp"
#include "server/ccache_tracker.hpp"

using ::testing::Mock;

namespace cpfs {
namespace server {
namespace {

class MockExpiry {
 public:
  MOCK_METHOD2(func, void(InodeNum, ClientNum));
};

TEST(CCacheTracker, CreationAPI) {
  boost::scoped_ptr<ICCacheTracker> tracker(MakeICacheTracker());
  MockExpiry mock_expiry;
  CCacheExpiryFunc expiry_func
      = boost::bind(&MockExpiry::func, &mock_expiry, _1, _2);

  tracker->SetCache(1, 2);
  tracker->SetCache(1, 2);
  EXPECT_EQ(1U, tracker->Size());
  EXPECT_CALL(mock_expiry, func(1, 2));
  tracker->InvGetClients(1, ClientNum(-1), expiry_func);

  tracker->SetCache(1, 2);
  tracker->SetCache(1, 3);
  EXPECT_EQ(2U, tracker->Size());
  EXPECT_CALL(mock_expiry, func(1, 2));
  EXPECT_CALL(mock_expiry, func(1, 3));
  tracker->InvGetClients(1, ClientNum(-1), expiry_func);
  Mock::VerifyAndClear(&mock_expiry);

  tracker->SetCache(1, 2);
  tracker->SetCache(1, 3);
  EXPECT_EQ(2U, tracker->Size());
  EXPECT_CALL(mock_expiry, func(1, 2));
  tracker->InvGetClients(1, 3, expiry_func);
}

TEST(CCacheTracker, RemoveClient) {
  boost::scoped_ptr<ICCacheTracker> tracker(MakeICacheTracker());
  tracker->SetCache(1, 2);
  tracker->SetCache(1, 3);
  EXPECT_EQ(2U, tracker->Size());
  tracker->RemoveClient(2);
  EXPECT_EQ(1U, tracker->Size());
}

TEST(CCacheTracker, Expiry) {
  boost::scoped_ptr<ICCacheTracker> tracker(MakeICacheTracker());
  MockExpiry mock_expiry;
  CCacheExpiryFunc expiry_func
      = boost::bind(&MockExpiry::func, &mock_expiry, _1, _2);
  tracker->SetCache(1, 2);
  tracker->ExpireCache(60, expiry_func);

  EXPECT_CALL(mock_expiry, func(1, 2));
  tracker->ExpireCache(0, expiry_func);
  EXPECT_EQ(0U, tracker->Size());
}

}  // namespace
}  // namespace server
}  // namespace cpfs
