#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines IThreadGroup to process FC FIMs received, by dispatching to
 * multiple cpfs::IThreadFimProcessor.
 */

#include "fim.hpp"
#include "fim_processor.hpp"
#include "thread_fim_processor.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimSocket;

namespace server {

class IWorker;

/**
 * Interface representing a group of threads processing Fim's.
 */
class IThreadGroup : public IFimProcessor {
 public:
  /**
   * Set the ThreadFimProcessorMaker to use.  Must be called before
   * the other methods are called.
   */
  virtual void SetThreadFimProcessorMaker(
      ThreadFimProcessorMaker maker) = 0;

  /**
   * Add a worker to the group.  This should be called only before
   * Start() is called.  The ownership of worker will be transferred
   * to the worker group.
   *
   * @param worker The worker to add
   */
  virtual void AddWorker(IWorker* worker) = 0;

  /**
   * @return The number of workers in the group.
   */
  virtual unsigned num_workers() = 0;

  /**
   * Start the workers.  This should be called exactly once, before
   * calls to EqueueFim().
   */
  virtual void Start() = 0;

  /**
   * Stop the workers and wait until it completes.
   *
   * @param max_wait_milli Maximum number of milliseconds to wait for
   * thread joining.  If negative, join threads synchronously
   */
  virtual void Stop(int max_wait_milli = -1) = 0;

  /**
   * Enqueue a Fim to all workers.  The Process() method of the
   * workers will be called with a null Fim socket.
   *
   * @param fim The Fim to enqueue
   */
  virtual void EnqueueAll(const FIM_PTR<IFim>& fim) = 0;

  /**
   * Check whether there is queued Fims for processing from a sender.
   * The method call the SocketPending() method of each thread in
   * sequence without coordination, so the FimSocket must already
   * cease generating further Fims for the returning value to be
   * meaningful.
   *
   * @param sender The sender
   *
   * @return true if some Fim is pending for the sender
   */
  virtual bool SocketPending(const boost::shared_ptr<IFimSocket>& sender)
      const = 0;
};

}  // namespace server
}  // namespace cpfs
