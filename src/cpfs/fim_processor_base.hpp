#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs:BaseFimProcessor implementation of the
 * cpfs::IFimProcessor interface.  It provides generic Fim handling
 * that are typical in the top-level FimProcessor.
 */

#include "fim.hpp"
#include "fim_processor.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFimSocket;
class ITimeKeeper;

/**
 * Base implementation of IFimProcessor.  General FIMs like Heartbeat
 * and replies are processed here.
 */
class BaseFimProcessor : public IFimProcessor {
 public:
  BaseFimProcessor();
  bool Accept(const FIM_PTR<IFim>& fim) const;
  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket);
  /**
   * Set time keeper for updating time when heartbeat is received
   *
   * @param time_keeper The time keeper
   */
  void SetTimeKeeper(ITimeKeeper* time_keeper);
 private:
  ITimeKeeper* time_keeper_;
};

}  // namespace cpfs
