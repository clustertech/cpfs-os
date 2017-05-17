#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define interface for the reply set.  The standby MS and the DS uses
 * it to keep recently replicated Fim replies, so that if the Fim ID
 * appear again, the reply is sent and the processing is not repeated.
 */

#include <boost/shared_ptr.hpp>

#include "common.hpp"

namespace cpfs {

class IFim;

namespace server {

/**
 * Keep a set of replies.
 */
class IReplySet {
 public:
  virtual ~IReplySet() {}

  /**
   * Find the reply given the request ID.
   *
   * @param req_id The request ID
   *
   * @return The reply found.  If not found, return the empty pointer
   */
  virtual FIM_PTR<IFim> FindReply(ReqId req_id) const = 0;

  /**
   * Add a reply the the set.
   *
   * @param reply The reply
   */
  virtual void AddReply(const FIM_PTR<IFim>& reply) = 0;

  /**
   * Expire replies that are older than a certain age.
   *
   * @param min_age Minimum age of replies to expire, in seconds.
   */
  virtual void ExpireReplies(int min_age) = 0;

  /**
   * Remove records of replies by a particular client.
   *
   * @param client_num The client number of the client
   */
  virtual void RemoveClient(ClientNum client_num) = 0;

  /**
   * Return whether the map is empty.
   */
  virtual bool IsEmpty() const = 0;
};

}  // namespace server
}  // namespace cpfs
