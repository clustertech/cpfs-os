#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines cpfs:CompositeFimProcessor for combining multiple
 * FimProcessor's.
 */

#include <vector>

#include <boost/shared_ptr.hpp>

#include "fim.hpp"
#include "fim_processor.hpp"

namespace cpfs {

class IFimSocket;

/**
 * Base implementation of IFimProcessor.  General FIMs like Heartbeat
 * and replies are processed here.
 */
class CompositeFimProcessor : public IFimProcessor {
 public:
  bool Accept(const FIM_PTR<IFim>& fim) const {
    for (unsigned i = 0; i < processors_.size(); ++i)
      if (processors_[i]->Accept(fim))
        return true;
    return false;
  }

  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket) {
    for (unsigned i = 0; i < processors_.size(); ++i)
      if (processors_[i]->Process(fim, socket))
        return true;
    return false;
  }

  /**
   * Add a component FimProcessor.  The added component will be owned
   * by the CompositeFimProcessor after the call, unless no_dealloc is
   * specified.
   *
   * @param processor The component FimProcessor to add
   *
   * @param no_dealloc If true, don't deallocate the component
   * FimProcessor when the CompositeFimProcessor processor is deleted.
   */
  void AddFimProcessor(IFimProcessor* processor, bool no_dealloc = false) {
    processors_.push_back(processor);
    if (!no_dealloc)
      to_free_.push_back(boost::shared_ptr<IFimProcessor>(processor));
  }

 private:
  std::vector<IFimProcessor*> processors_;
  std::vector<boost::shared_ptr<IFimProcessor> > to_free_;
};

}  // namespace cpfs
