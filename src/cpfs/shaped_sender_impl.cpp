/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IShapedSender interface.
 */

#include "shaped_sender_impl.hpp"

#include <deque>
#include <stdexcept>

#include <boost/format.hpp>
#include <boost/functional/factory.hpp>
#include <boost/functional/forward_adapter.hpp>
#include <boost/shared_ptr.hpp>

#include "fim.hpp"
#include "fims.hpp"
#include "req_entry.hpp"
#include "req_entry_impl.hpp"
#include "req_tracker.hpp"

namespace cpfs {

namespace {

/**
 * Implementation of the IShapedSender interface.
 */
class ShapedSender : public IShapedSender {
 public:
  /**
   * @param tracker The tracker to send Fims to
   *
   * @param max_fims The maximum number of outstanding Fims to allow
   */
  ShapedSender(const boost::shared_ptr<IReqTracker> tracker, unsigned max_fims)
      : tracker_(tracker), max_fims_(max_fims) {}

  void SendFim(const FIM_PTR<IFim> fim) {
    while (pending_req_entries_.size() >= max_fims_)
      WaitOneReply();
    boost::shared_ptr<IReqEntry> entry =
        MakeTransientReqEntry(tracker_, fim);
    if (!tracker_->AddRequestEntry(entry))
      throw std::runtime_error("Socket lost during resync");
    pending_req_entries_.push_back(entry);
  }

  void WaitAllReplied() {
    while (!pending_req_entries_.empty())
      WaitOneReply();
  }

 private:
  boost::shared_ptr<IReqTracker> tracker_;
  unsigned max_fims_;
  std::deque<boost::shared_ptr<IReqEntry> > pending_req_entries_;

  void WaitOneReply() {
    const FIM_PTR<IFim>& reply =
        pending_req_entries_.front()->WaitReply();
    if (reply->type() != kResultCodeReplyFim)
      throw std::runtime_error(
          (boost::format("Unexpected reply type %d received during resync")
           % reply->type()).str());
    ResultCodeReplyFim& rreply = static_cast<ResultCodeReplyFim&>(*reply);
    if (rreply->err_no != 0)
      throw std::runtime_error(
          (boost::format("Error reply during resync, code %d")
           % rreply->err_no).str());
    pending_req_entries_.pop_front();
  }
};

}  // namespace

ShapedSenderMaker kShapedSenderMaker = boost::forward_adapter<
  boost::factory<ShapedSender*> >();

}  // namespace cpfs
