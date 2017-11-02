#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of operation completion checking
 * facilities.
 */

namespace cpfs {

class IOpCompletionChecker;
class IOpCompletionCheckerSet;

/**
 * @return A new operation completion checker.
 */
IOpCompletionChecker* MakeOpCompletionChecker();

/**
 * Create an operation completion checker set.
 *
 * @return The created checker set
 */
IOpCompletionCheckerSet* MakeOpCompletionCheckerSet();

}  // namespace cpfs
