#pragma once

/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of tracer.
 */

namespace cpfs {

class ITracer;

/**
 * Create an instance implementing the ITracer interface.
 */
ITracer* MakeTracer();

/**
 * Create a 5-element instance implementing the ITracer interface, for
 * unit tests.
 */
ITracer* MakeSmallTracer();

}  // namespace cpfs
