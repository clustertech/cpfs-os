#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define utilities.
 */
#include <stdint.h>

#include <cstddef>
#include <ctime>
#include <string>

#include <boost/date_time/posix_time/posix_time.hpp>

namespace cpfs {

/**
 * A simple template to contain a value for which copying requests are
 * silently ignored.  This is useful when creating copyable classes
 * containing mutable values which values are kept privately.  The
 * class looks like a pointer, so that you can use *obj to access the
 * content.
 *
 * @tparam T The type of content contained
 */
template<typename T>
class SkipCopy {
 public:
  T content; /**< The content */
  /**
   * Default constructor.  Does nothing.
   */
  SkipCopy() {}
  /**
   * Copy constructor.  Does nothing.
   */
  SkipCopy(const SkipCopy<T>& other) {
    (void) other;
  }
  /**
   * Assignment operator.  Does nothing.
   */
  SkipCopy<T>& operator=(const SkipCopy<T>& other) {
    (void) other;
    return *this;
  }
  /**
   * Deference operator.
   */
  T& operator*() {
    return content;
  }
  /**
   * Deference operator (const).
   */
  T& operator*() const {
    return const_cast<T&>(content);
  }
  /**
   * Member extraction operator.
   */
  T* operator->() {
    return &content;
  }
  /**
   * Member extraction operator (const).
   */
  T* operator->() const {
    return const_cast<T*>(&content);
  }
};

/**
 * Comparison function of two timespec structures, to be used for container.
 */
struct CompareTime {
  /**
   * @param ts1 The left operand.
   *
   * @param ts2 The right operand.
   */
  bool operator() (const struct timespec& ts1, const struct timespec& ts2) {
    if (ts1.tv_sec != ts2.tv_sec)
      return ts1.tv_sec < ts2.tv_sec;
    return ts1.tv_nsec < ts2.tv_nsec;
  }
};

/**
 * Xor all bytes of an array from another.  Taking out the computation
 * to a function helps the compiler to recognize the possibility of
 * optimization using specialized CPU instructions.
 *
 * @param target The array to modify
 *
 * @param src The source array of bytes for xor
 *
 * @param size The size of the two arrays
 */
void XorBytes(char* target, const char* src, std::size_t size);

/**
 * Convert IP v4 string to uint32_t
 */
uint32_t IPToInt(const std::string& ip_str);

/**
 * Convert uint32_t to IP v4 string
 */
std::string IntToIP(uint32_t ip_int);

/**
 * Convert time to boost::posix_time::time_duration
 *
 * @param t The time in seconds
 *
 * @return The time of boost::posix_time::time_duration
 */
boost::posix_time::time_duration ToTimeDuration(double t);

/**
 * Check whether a file open flag is for writing.
 *
 * @param flag The file open flag
 *
 * @return Whether it is for writing
 */
bool IsWriteFlag(int flag);

/**
 * Format the specified disk size to human readable form
 *
 * @param disk_size The disk size
 *
 * @return The formatted disk size
 */
std::string FormatDiskSize(uint64_t disk_size);

/**
 * Write the process ID to the file specified
 *
 * @param pidfile The path to the PID file
 */
void WritePID(const std::string& pidfile);

/**
 * Create an UUID
 */
std::string CreateUUID();

}  // namespace cpfs
