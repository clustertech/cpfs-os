#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs:IFimProcessor interface for processing FIMs
 * received.
 */
#include <boost/intrusive_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"

namespace cpfs {

class IFim;
class IFimSocket;

/**
 * Interface for objects processing Fim's received from FimSocket's.
 */
class IFimProcessor {
 public:
  virtual ~IFimProcessor() {}

  /**
   * Check whether the specified Fim is accepted by this processor
   *
   * @param fim The fim to be processed
   *
   * @return True if the fim can be passed to Process()
   */
  virtual bool Accept(const FIM_PTR<IFim>& fim) const = 0;

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
                       const boost::shared_ptr<IFimSocket>& socket) = 0;
};

}  // namespace cpfs
