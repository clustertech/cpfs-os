#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for IDSQueryMgr.
 */

#include <map>
#include <vector>

#include "common.hpp"
#include "fim.hpp"

namespace cpfs {

class IFimProcessor;

namespace server {
namespace ms {

/** The map of group ID to vector of Fim replies from that DS group */
typedef std::map<GroupId, std::map<GroupRole, FIM_PTR<IFim> > > DSReplies;

/**
 * Interface for sending request to DS groups and receiving replies
 */
class IDSQueryMgr {
 public:
  virtual ~IDSQueryMgr() {}

  /**
   * Send request to the specified data server groups and wait until all
   * replies are returned. If the reply is not a reply Fim or of type
   * kResultCodeReplyFim, exception is thrown
   *
   * @param fim The request fim
   *
   * @param group_ids The vector of DS groups to send the FIM
   *
   * @return All replies received from DS groups
   */
  virtual DSReplies Request(const FIM_PTR<IFim>& fim,
                            const std::vector<GroupId>& group_ids) = 0;
};

}  // namespace ms

}  // namespace server

}  // namespace cpfs
