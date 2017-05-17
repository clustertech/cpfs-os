#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the ITrackerMapper interface.
 */

namespace cpfs {

class IAsioPolicy;
class ITrackerMapper;

/**
 * Create an implementation of the ITrackerMapper interface.
 *
 * @param policy The policy to use, for trackers to run callbacks
 */
ITrackerMapper* MakeTrackerMapper(IAsioPolicy* policy);

}  // namespace cpfs
