/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define implementation of IStatusDumper.
 */

#include "status_dumper_impl.hpp"

#include <csignal>

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "logger.hpp"
#include "status_dumper.hpp"

namespace cpfs {

namespace {

/**
 * Implementation of IStatusDumper.  It provides signal handling that
 * is needed for all status dumper: after Init() and Run() is called,
 * the Dump() method of the dumpable is invoked whenever SIGUSR2 is
 * received.
 */
class StatusDumper : public IStatusDumper {
 public:
  /**
   * @param dumpable What to dump when SIGUSR2 is received
   */
  explicit StatusDumper(IStatusDumpable* dumpable)
      : dumpable_(dumpable), asio_policy_(0) {}
  ~StatusDumper();

  void SetAsioPolicy(IAsioPolicy* asio_policy);

 private:
  IStatusDumpable* dumpable_;
  IAsioPolicy* asio_policy_;
  boost::scoped_ptr<SignalSet> signal_set_;

  /**
   * Setup signal handling of the dumper.
   */
  void SetupSignal();

  /**
   * Call Dump and reinstall signal handler.
   */
  void HandleSignal(const boost::system::error_code& error,
                    int signal_number);
};

void StatusDumper::SetAsioPolicy(IAsioPolicy* asio_policy) {
  asio_policy_ = asio_policy;
  signal_set_.reset(asio_policy_->MakeSignalSet());
  SetupSignal();
}

StatusDumper::~StatusDumper() {
  if (asio_policy_)
    asio_policy_->SetSignalHandler(signal_set_.get(), 0, 0, SignalHandler());
}

void StatusDumper::SetupSignal() {
  int signals[] = {SIGHUP, SIGUSR2};
  asio_policy_->SetSignalHandler(signal_set_.get(), signals,
                                 sizeof(signals) / sizeof(signals[0]),
                                 boost::bind(&StatusDumper::HandleSignal,
                                             this, _1, _2));
}

void StatusDumper::HandleSignal(const boost::system::error_code& error,
                                int signal_number) {
  if (!error) {
    if (signal_number == SIGHUP) {
      SetLogPath("");
    } else if (signal_number == SIGUSR2) {
      dumpable_->Dump();
    }
    SetupSignal();
  }
}

}  // namespace

IStatusDumper* MakeStatusDumper(IStatusDumpable* dumpable) {
  return new StatusDumper(dumpable);
}

}  // namespace cpfs
