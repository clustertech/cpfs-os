#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Unit test utilities.
 */

namespace cpfs {

/**
 * Signals to defer during unit tests.
 */
extern const int kUnittestDeferredSigNums[];

/**
 * Number of elements in kUnittestDeferredSigNums.
 */
extern const unsigned kNumUnittestDeferredSigs;

/**
 * Set whether to block signals.
 *
 * @param block Whether to block
 */
void UnittestBlockSignals(bool block);



}  // namespace cpfs
