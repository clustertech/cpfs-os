#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of request tracker related facilities.
 */

#include <string>

#include "common.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IAsioPolicy;
class IReqTracker;
class ReqIdGenerator;

/**
 * Create an instance implementing the IReqTracker interface.  The
 * instance is thread-safe: multiple methods may be concurrently
 * called.
 *
 * @param name The name to be returned on name()
 *
 * @param policy The Asio policy to run callbacks
 *
 * @param req_id_gen Generator to generate request ID.
 *
 * @param peer_client_num The client number of the request tracker.
 * If 0, the peer is not a client.
 */
boost::shared_ptr<IReqTracker> MakeReqTracker(
    const std::string& name, IAsioPolicy* policy,
    ReqIdGenerator* req_id_gen, ClientNum peer_client_num = kNotClient);

}  // namespace cpfs
