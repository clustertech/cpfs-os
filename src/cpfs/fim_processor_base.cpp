/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define implementation of BaseFimProcessor.
 */

#include "fim_processor_base.hpp"

#include <boost/shared_ptr.hpp>

#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "req_tracker.hpp"
#include "time_keeper.hpp"

namespace cpfs {

BaseFimProcessor::BaseFimProcessor() : time_keeper_(0) {}

bool BaseFimProcessor::Accept(const FIM_PTR<IFim>& fim) const {
  return fim->IsReply() || fim->type() == kHeartbeatFim;
}

bool BaseFimProcessor::Process(const FIM_PTR<IFim>& fim,
                               const boost::shared_ptr<IFimSocket>& socket) {
  if (time_keeper_)
    time_keeper_->Update();
  if (!fim->IsReply())
    return fim->type() == kHeartbeatFim;
  IReqTracker* req_tracker = socket->GetReqTracker();
  if (req_tracker)
    req_tracker->AddReply(fim);
  else
    LOG(error, Fim, "Reply ", PVal(fim), " without tracker");
  return true;
}

void BaseFimProcessor::SetTimeKeeper(ITimeKeeper* time_keeper) {
  time_keeper_ = time_keeper;
}

}  // namespace cpfs
