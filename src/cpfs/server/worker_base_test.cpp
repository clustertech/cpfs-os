/* Copyright 2013 ClusterTech Ltd */
#include "server/worker_base.hpp"

#include <boost/scoped_ptr.hpp>

#include <gmock/gmock.h>
// IWYU pragma: no_forward_declare testing::MockFunction
#include <gtest/gtest.h>

#include "config_mgr.hpp"
#include "fim_processor_mock.hpp"
#include "fim_socket_mock.hpp"
#include "fims.hpp"
#include "mock_actions.hpp"
#include "tracker_mapper_mock.hpp"
#include "server/ccache_tracker_mock.hpp"

using ::testing::_;
using ::testing::Mock;
using ::testing::MockFunction;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StartsWith;

namespace cpfs {
namespace server {
namespace {

TEST(WorkerBaseTest, WorkerCacheInvalidator) {
  boost::scoped_ptr<MockITrackerMapper> tracker_mapper(new MockITrackerMapper);
  boost::scoped_ptr<MockICCacheTracker> cache_tracker(new MockICCacheTracker);
  MockFunction<void(InodeNum, bool)> inv_func;
  WorkerCacheInvalidator cinv;
  cinv.SetTrackerMapper(tracker_mapper.get());
  cinv.SetCCacheTracker(cache_tracker.get());
  cinv.SetCacheInvalFunc(boost::bind(GetMockCall(inv_func), &inv_func, _1, _2));

  CCacheExpiryFunc expiry_func;
  EXPECT_CALL(*cache_tracker, InvGetClients(2, 10, _))
      .WillOnce(SaveArg<2>(&expiry_func));
  EXPECT_CALL(inv_func, Call(2, true));
  cinv.InvalidateClients(2, true, 10);

  // send invalidation
  boost::shared_ptr<MockIFimSocket> cacher(new MockIFimSocket);
  FIM_PTR<IFim> reply;
  EXPECT_CALL(*tracker_mapper, GetFCFimSocket(5))
      .WillOnce(Return(cacher));
  EXPECT_CALL(*cacher, WriteMsg(_))
      .WillOnce(SaveArg<0>(&reply));
  expiry_func(2, 5);
  Mock::VerifyAndClear(tracker_mapper.get());
  InvInodeFim& rreply = dynamic_cast<InvInodeFim&>(*reply);
  EXPECT_EQ(2U, rreply->inode);
  EXPECT_TRUE(rreply->clear_page);
}

class MyBaseWorker : public BaseWorker {
 public:
  bool Accept(const FIM_PTR<IFim>& fim) const {
    (void) fim;
    return true;
  }

  /**
   * What to do when Fim is received from FimSocket.
   *
   * @param fim The fim received
   *
   * @param socket The socket receiving the fim
   *
   * @return Whether the handling is completed
   */
  virtual bool Process(const FIM_PTR<IFim>& fim,
                       const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    (void) socket;
    return false;
  }
};

void f() {}

TEST(WorkerBaseTest, BaseWorker) {
  MyBaseWorker worker;

  EXPECT_EQ(0, worker.server());
  ConfigMgr config;
  boost::scoped_ptr<BaseCpfsServer> server(new BaseCpfsServer(config));
  server->set_tracker_mapper(new MockITrackerMapper);
  server->set_cache_tracker(new MockICCacheTracker);
  worker.set_server(server.get());

  MockIFimProcessor queuer;
  worker.SetQueuer(&queuer);

  EXPECT_CALL(queuer, Process(FIM_PTR<IFim>(),
                              boost::shared_ptr<IFimSocket>()));

  worker.Enqueue(FIM_PTR<IFim>(), boost::shared_ptr<IFimSocket>());

  worker.SetCacheInvalFunc(boost::bind(&f));
  EXPECT_EQ(1U, worker.GetMemoId());
  EXPECT_EQ(2U, worker.GetMemoId());

  boost::shared_ptr<WorkerMemo> memo(new WorkerMemo(1));
  EXPECT_EQ(1, memo->type());
  EXPECT_EQ(0, memo->CheckCast<WorkerMemo>(2));
  EXPECT_EQ(memo.get(), memo->CheckCast<WorkerMemo>(1));

  worker.PutMemo(1, memo);
  EXPECT_EQ(memo, worker.GetMemo(1));
  EXPECT_EQ(boost::shared_ptr<WorkerMemo>(), worker.GetMemo(1));
}

}  // namespace
}  // namespace server
}  // namespace cpfs
