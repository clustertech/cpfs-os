#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IShapedSender interface.
 */

#include "shaped_sender.hpp"

namespace cpfs {

/**
 * Maker to create the actual implementation of IShapedSender.
 */
extern ShapedSenderMaker kShapedSenderMaker;

}  // namespace cpfs
