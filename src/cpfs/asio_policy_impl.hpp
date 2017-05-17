#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of policies for async operations.
 */

namespace cpfs {

class IAsioPolicy;

IAsioPolicy* MakeAsioPolicy();

}  // namespace cpfs
