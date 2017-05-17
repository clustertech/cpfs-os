#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define IShapedSender interface.
 */
#include <boost/function.hpp>

#include "common.hpp"
#include "fim.hpp"

namespace boost {

template <class Y> class intrusive_ptr;
template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFim;
class IReqTracker;

/**
 * Interface to send Fims to a target request tracker
 * semi-synchronously.  Fims are sent in a way maintaining the
 * "shaping guarantee", that the number of unanswered Fim is bounded.
 * The implementation is not thread safe, as it is mostly useful when
 * a dedicated thread takes the sole ownership of the shaped sender to
 * send Fims in a controlled manner.
 */
class IShapedSender {
 public:
  virtual ~IShapedSender() {}

  /**
   * Send a Fim, possibly wait until a previously sent Fim is replied
   * first to maintain the shaping guarantee.  The Fim is sent using a
   * transient request entry.  Throw an exception if the received Fim
   * is not a good reply (ResultCodeReplyFim with err_no = 0), or the
   * Fim cannot be waited or queued due to a lost socket
   */
  virtual void SendFim(const FIM_PTR<IFim> fim) = 0;

  /**
   * Wait until all previously sent Fims have been replied.  Throw an
   * exception if some replies are bad or the socket is lost while
   * waiting for them.
   */
  virtual void WaitAllReplied() = 0;
};

/**
 * Type for changing the behavior of creating IShapedSender instances.
 * The first argument is the tracker that will receive the Fims, and
 * the second argument is the number of fims to allow to be unanswered
 * before blocking the sender to wait for replies.
 */
typedef boost::function<
  IShapedSender* (boost::shared_ptr<IReqTracker> tracker, unsigned max_fims)
> ShapedSenderMaker;

}  // namespace cpfs
