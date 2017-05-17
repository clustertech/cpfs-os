/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IThreadGroup to process FC FIMs
 * received, by dispatching to multiple cpfs::IThreadFimProcessor.
 */

#include "server/thread_group_impl.hpp"

#include <boost/function.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/thread.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "thread_fim_processor.hpp"
#include "server/thread_group.hpp"
#include "server/worker.hpp"

namespace cpfs {

class IFimSocket;

namespace server {
namespace {

/**
 * Implement IThreadGroup.
 */
class ThreadGroup : public IThreadGroup {
 public:
  ThreadGroup() : op_mutex_(MUTEX_INIT) {}

  void SetThreadFimProcessorMaker(ThreadFimProcessorMaker maker) {
    thread_fim_proc_maker_ = maker;
  }

  void AddWorker(IWorker* worker) {
    IThreadFimProcessor* tproc = thread_fim_proc_maker_(worker);
    tprocs_.push_back(tproc);
    worker->SetQueuer(tproc);
  }

  unsigned num_workers() {
    return tprocs_.size();
  }

  void Start() {
    MUTEX_LOCK_GUARD(op_mutex_);
    for (unsigned i = 0; i < tprocs_.size(); ++i)
      tprocs_[i].Start();
  }

  void Stop(int max_wait_milli) {
    MUTEX_LOCK_GUARD(op_mutex_);
    for (unsigned i = 0; i < tprocs_.size(); ++i)
      tprocs_[i].Stop();
    if (max_wait_milli < 0) {
      Join();
    } else {
      boost::shared_ptr<bool> completed(new bool(false));
      boost::shared_ptr<MUTEX_TYPE> mutex(new MUTEX_TYPE(MUTEX_INIT));
      boost::shared_ptr<boost::condition_variable> complete_cond(
          new boost::condition_variable);
      boost::thread(&ThreadGroup::Join, this, completed, mutex, complete_cond);
      boost::unique_lock<MUTEX_TYPE> lock(*mutex);
      if (!*completed)
        if (!MUTEX_TIMED_WAIT(lock, *complete_cond,
                              boost::posix_time::milliseconds(max_wait_milli)))
          LOG(notice, Server,
              "Thread group stop request timed out, proceeding anyway");
    }
  }

  /**
   * Check whether the specified Fim is accepted by the corresponding worker
   * assigned to the inode
   *
   * @param fim The fim to be processed
   *
   * @return True if the fim can be passed to Process()
   */
  bool Accept(const FIM_PTR<IFim>& fim) const {
    InodeNum inode = reinterpret_cast<InodeNum&>(*(fim->body()));
    return tprocs_[inode % tprocs_.size()].Accept(fim);
  }

  /**
   * What to do when Fim is received from FimSocket.
   *
   * @param fim The fim received
   *
   * @param sender The socket receiving the fim
   *
   * @return Whether the handling is completed
   */
  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& sender) {
    InodeNum inode = reinterpret_cast<InodeNum&>(*(fim->body()));
    LOG(debug, Fim, "*q ", PVal(fim), ", inode = ", PHex(inode));
    return tprocs_[inode % tprocs_.size()].Process(fim, sender);
  }

  void EnqueueAll(const FIM_PTR<IFim>& fim) {
    for (unsigned i = 0; i < tprocs_.size(); ++i)
      tprocs_[i].Process(fim, boost::shared_ptr<IFimSocket>());
  }

  bool SocketPending(const boost::shared_ptr<IFimSocket>& sender) const {
    for (unsigned i = 0; i < tprocs_.size(); ++i)
      if (tprocs_[i].SocketPending(sender))
        return true;
    return false;
  }

 private:
  /** How to make a ThreadFimProcessor */
  ThreadFimProcessorMaker thread_fim_proc_maker_;
  MUTEX_TYPE op_mutex_; /**< Prevent concurrent Start and Stop */
  /** Thread processors for workers added */
  boost::ptr_vector<IThreadFimProcessor> tprocs_;

  void Join(boost::shared_ptr<bool> completed = boost::shared_ptr<bool>(),
            boost::shared_ptr<boost::mutex> mutex =
            boost::shared_ptr<boost::mutex>(),
            boost::shared_ptr<boost::condition_variable> complete_cond =
            boost::shared_ptr<boost::condition_variable>()) {
    for (unsigned i = 0; i < tprocs_.size(); ++i)
      tprocs_[i].Join();
    if (completed) {
      boost::unique_lock<boost::mutex> lock(*mutex);
      *completed = true;
      complete_cond->notify_one();
    }
  }
};

}  // namespace

IThreadGroup* MakeThreadGroup() {
  return new ThreadGroup;
}

}  // namespace server
}  // namespace cpfs
