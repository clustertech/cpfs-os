#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of ThreadFimProcessor.
 */

#include "thread_fim_processor.hpp"

namespace cpfs {

/**
 * ThreadFimProcessorMaker creating processors which calls the inner
 * processor without other processing.
 */
extern ThreadFimProcessorMaker kThreadFimProcessorMaker;

/**
 * ThreadFimProcessorMaker creating processors which perform
 * BasicFimProcessor actions before calling the inner processor.
 */
extern ThreadFimProcessorMaker kBasicThreadFimProcessorMaker;

}  // namespace cpfs
