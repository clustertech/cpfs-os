#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of request entries.
 */
#include <boost/shared_ptr.hpp>

#include "fim.hpp"

namespace boost {

template <class Y> class weak_ptr;

}  // namespace boost

namespace cpfs {

class IReqEntry;
class IReqTracker;

/**
 * Create a request entry for normal requests.  This is used for
 * request entries that create an ID and would be waiting for resend
 * if FimSocket is not found.
 */
boost::shared_ptr<IReqEntry> MakeDefaultReqEntry(
    const boost::weak_ptr<IReqTracker>& tracker,
    const FIM_PTR<IFim>& request);

/**
 * Create a request entry for transient requests.  This is used for
 * request entries that create an ID and would simply be skipped if
 * FimSocket is not found.
 */
boost::shared_ptr<IReqEntry> MakeTransientReqEntry(
    const boost::weak_ptr<IReqTracker>& tracker,
    const FIM_PTR<IFim>& request);

/**
 * Create a request entry for replication requests.  This is used for
 * request entries that reuse the request ID and would simply be
 * skipped if FimSocket is not found.
 */
boost::shared_ptr<IReqEntry> MakeReplReqEntry(
    const boost::weak_ptr<IReqTracker>& tracker,
    const FIM_PTR<IFim>& request);

}  // namespace cpfs
