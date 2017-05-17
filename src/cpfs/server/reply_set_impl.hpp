#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the interface for the reply set.  The
 * standby MS and the DS uses it to keep recently replicated Fim
 * replies, so that if the Fim ID appear again, the reply is sent and
 * the processing is not repeated.
 */

namespace cpfs {
namespace server {

class IReplySet;

/**
 * Create an implementation of the reply set.  The implementation is
 * thread safe.
 */
IReplySet* MakeReplySet();

}  // namespace server
}  // namespace cpfs
