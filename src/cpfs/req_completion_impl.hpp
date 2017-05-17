#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of request completion checking
 * facilities.
 */

namespace cpfs {

class IReqCompletionChecker;
class IReqCompletionCheckerSet;

/**
 * @return A new request completion checker.
 */
IReqCompletionChecker* MakeReqCompletionChecker();

/**
 * Create a request completion checker set.
 *
 * @return The created checker set
 */
IReqCompletionCheckerSet* MakeReqCompletionCheckerSet();

}  // namespace cpfs
