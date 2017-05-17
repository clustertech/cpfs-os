#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define functions handling compatibility issues.
 */

#include <sys/time.h>

#include <cassert>

#ifdef COMPAT_UTIMESNS

#ifndef UTIME_OMIT
// Same definition as in bits/stat.h
#define UTIME_OMIT ((1l << 30) - 2l)
#endif

inline int futimens(int fd, const struct timespec times[2]) {
  struct stat stbuf;
  fstat(fd, &stbuf);
  struct timeval tv[2];
  if (times[0].tv_nsec == UTIME_OMIT) {
    tv[0].tv_sec = stbuf.st_atime;
    tv[0].tv_usec = 0;
  } else {
    TIMESPEC_TO_TIMEVAL(tv, times);
  }
  if (times[1].tv_nsec == UTIME_OMIT) {
    tv[1].tv_sec = stbuf.st_mtime;
    tv[1].tv_usec = 0;
  } else {
    TIMESPEC_TO_TIMEVAL((tv + 1), (times + 1));
  }
  return futimes(fd, tv);
}

inline int utimensat(int fd, const char* pathname,
                     const struct timespec times[2], int flags) {
  assert(fd == AT_FDCWD);
  assert(flags == AT_SYMLINK_NOFOLLOW);
  struct stat stbuf;
  lstat(pathname, &stbuf);
  struct timeval tv[2];
  if (times[0].tv_nsec == UTIME_OMIT) {
    tv[0].tv_sec = stbuf.st_atime;
    tv[0].tv_usec = 0;
  } else {
    TIMESPEC_TO_TIMEVAL(tv, times);
  }
  if (times[1].tv_nsec == UTIME_OMIT) {
    tv[1].tv_sec = stbuf.st_mtime;
    tv[1].tv_usec = 0;
  } else {
    TIMESPEC_TO_TIMEVAL((tv + 1), (times + 1));
  }
  return utimes(pathname, tv);
}

#endif
