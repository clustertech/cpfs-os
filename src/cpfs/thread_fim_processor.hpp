#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ThreadFimProcessor.
 */

#include <boost/function.hpp>

#include "fim_processor.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFim;
class IFimSocket;

/**
 * A processor which handle Fims by delegating to a dedicated thread.
 *
 * A thread is created to handle Fims.  When a Fim is received, it is
 * enqueued into either the "server" or "client" queue, depending on
 * whether we know that the Fim comes from a client.  A thread waits
 * until either queue has Fim in it, and select the first Fim in the
 * server queue or, if not found, client queue to handle.  The caller
 * (usually, the communication thread) can thus continue reading Fims
 * once the Fim is enqueued, rather than having to wait until the
 * processing completes.
 */
class IThreadFimProcessor : public IFimProcessor {
 public:
  virtual ~IThreadFimProcessor() {}

  /**
   * Start the thread.
   */
  virtual void Start() = 0;

  /**
   * Stop the thread.
   */
  virtual void Stop() = 0;

  /**
   * Join the thread.
   */
  virtual void Join() = 0;

  /**
   * @param fim The fim
   *
   * @return Whether the Fim would be accepted by the nested Fim
   * processor
   */
  virtual bool Accept(const FIM_PTR<IFim>& fim) const = 0;

  /**
   * Check whether the specified Fim is accepted by this processor
   *
   * @param sender The sender
   */
  virtual bool SocketPending(const boost::shared_ptr<IFimSocket>& sender)
      const = 0;
};

/**
 * Type for changing the behavior of creating IThreadFimProcessor
 * instances.  The argument is the processor to call in the thread to
 * process Fims.
 */
typedef boost::function<IThreadFimProcessor*(IFimProcessor* processor)>
ThreadFimProcessorMaker;

}  // namespace cpfs
