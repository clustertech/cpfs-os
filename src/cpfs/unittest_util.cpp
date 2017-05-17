/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of unittest utilities.
 */
#include "unittest_util.hpp"  // IWYU pragma: keep

#include <csignal>

namespace cpfs {

const int kUnittestDeferredSigNums[] = {
  SIGINT,
  SIGTERM,
  SIGQUIT,
  SIGUSR1,
  SIGUSR2
};

const unsigned kNumUnittestDeferredSigs =
    sizeof(kUnittestDeferredSigNums) / sizeof(kUnittestDeferredSigNums[0]);

void UnittestBlockSignals(bool block) {
  sigset_t sig_set;
  sigemptyset(&sig_set);
  for (unsigned i = 0; i < kNumUnittestDeferredSigs; ++i)
    sigaddset(&sig_set, kUnittestDeferredSigNums[i]);
  sigprocmask(block ? SIG_BLOCK : SIG_UNBLOCK, &sig_set, 0);
}

}  // namespace cpfs
