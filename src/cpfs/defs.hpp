#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define common constants.
 */

/**
 * For getpwent_r and getgrent_r
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/**
 * FUSE API version to use.
 */
#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif

/**
 * For PRIu64 etc
 */
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

/**
 * For nanasleep and clock_gettime.
 */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 199309
#endif

/**
 * Make Boost chrono header-only.
 */
#ifndef BOOST_CHRONO_HEADER_ONLY
#define BOOST_CHRONO_HEADER_ONLY

/**
 * Make Boost Asio use select.
 *
 * The epoll reactor is found to be prone to missing trigger in our
 * test system.  The test runs iozone cluster mode using servers as
 * clients.  For tests where there are at least 3 clients each writing
 * at least 10G data (record size 1M), the server invariantly loses a
 * write event in the middle causing the test to hang.  It is disabled
 * for now, and may be revived if we find it passes our tests in the
 * future.
 */
#define BOOST_ASIO_DISABLE_EPOLL 1

#endif
