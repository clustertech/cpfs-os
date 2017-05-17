#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Actions to be used for mocks in unit tests.
 */

#include <stdint.h>
#include <unistd.h>

#include <sys/wait.h>

#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#if !defined(__GNUC__) || __cplusplus >= 201103L
#include <tuple>
#endif
#include <vector>

#include <boost/format.hpp>
#include <boost/scoped_array.hpp>
#include <boost/typeof/typeof.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#if defined(__GNUC__) && __cplusplus < 201103L
#include <tr1/tuple>
#endif

#include "unittest_util.hpp"

namespace cpfs {

/**
 * A manager for use by tests that needs a directory in the FS to
 * operate.
 */
class MockDataPathMgr {
 public:
  /**
   * @param path_template A template ending with 6 'X' characters
   * specifying where to put the directory.
   */
  explicit MockDataPathMgr(const char* path_template)
      : path_(new char[std::strlen(path_template) + 1]) {
    std::strncpy(path_.get(), path_template, std::strlen(path_template) + 1);
    mkdtemp(path_.get());
  }

  ~MockDataPathMgr() {
    Deinit();
  }

  /**
   * Deinitialize the data path.
   */
  void Deinit() {
    UnittestBlockSignals(true);  // Don't let user break for now
    system((boost::format("D=%s; test -e $D && (chmod -R u+rwx $D; rm -rf $D)")
            % path_.get()).str().c_str());
    UnittestBlockSignals(false);
  }

  /**
   * @return The path chosen for the directory.
   */
  char* GetPath() {
    return path_.get();
  }

 private:
  boost::scoped_array<char> path_;
};

/**
 * Make a vector containing command-line argument strings.
 *
 * @param args The arguments.
 */
inline std::vector<std::string> MakeArgVector(const char* args[]) {
  std::vector<std::string> ret;
  while (*args) {
    ret.push_back(*args);
    ++args;
  }
  return ret;
}

/**
 * Convert a vector of command-line argument strings to a vector of
 * char* for use with main() calls.
 *
 * @param arg_vector The vector.
 */
inline std::vector<char*> MakeArgs(std::vector<std::string>* arg_vector) {
  std::vector<char*> ret;
  for (unsigned i = 0; i < (*arg_vector).size(); ++i)
    if ((*arg_vector)[i] == "" || *((*arg_vector)[i].end() - 1))
      (*arg_vector)[i] += '\0';
  for (unsigned i = 0; i < (*arg_vector).size(); ++i)
    ret.push_back(&(*arg_vector)[i][0]);
  return ret;
}

/**
 * Action template to save an array argument somewhere.  Use it like
 * SaveArgPointee, except that it got one more call argument
 * specifying the number of elements to copy.
 */
ACTION_TEMPLATE(SaveArgArray,
                HAS_1_TEMPLATE_PARAMS(int, k),
                AND_2_VALUE_PARAMS(pointer, size)) {
  for (unsigned i = 0; i < unsigned(size); ++i)
    *(pointer + i) = *(::std::tr1::get<k>(args) + i);
}

/**
 * Action template to reset a pointer to smart pointer argument to a
 * particular value.
 */
ACTION_TEMPLATE(ResetSmartPointerArg,
                HAS_1_TEMPLATE_PARAMS(int, k),
                AND_1_VALUE_PARAMS(pointer)) {
  ::std::tr1::get<k>(args)->reset(pointer);
}

/**
 * Functor to sleep for some time.
 */
class Sleep {
 public:
  /**
   * @param num_sec Number of seconds to sleep.
   */
  explicit Sleep(double num_sec) : num_sec_(num_sec) {}
  /**
   * Perform the action.
   */
  void operator() () {
    timespec to_sleep = {time_t(num_sec_),
                         uint32_t((num_sec_ - time_t(num_sec_))
                                  * 1000000000U)};
    nanosleep(&to_sleep, 0);
  }

 private:
  double num_sec_;
};

/**
 * Represent the result of the ForkCapture() call.  This is a safety
 * net for tests using the facility, ensuring that the child is
 * correctly exited.
 */
class ForkCaptureResult {
 public:
  /**
   * Constructor.
   *
   * @param check Whether checking will be performed.
   */
  explicit ForkCaptureResult(int check) : check_(check) {}

  /**
   * @param other The object to copy.
   */
  ForkCaptureResult(const ForkCaptureResult& other) {
    *this = other;
  }

  /**
   * @param other The object to copy.
   */
  ForkCaptureResult& operator=(const ForkCaptureResult& other) {
    check_ = other.check_;
    other.check_ = 0;
    return *this;
  }

  ~ForkCaptureResult() {
    if (!check_)
      return;
    const char msg[] = "ForkCapture child not exited properly!\n";
    write(3, msg, sizeof(msg));
    close(3);
    std::exit(1);
  }

  /**
   * @return Whether checking will be performed on removal of the object.
   */
  operator bool() {
    return bool(check_);
  }

 private:
  mutable int check_;
};

/**
 * Fork a process and capture output.
 *
 * @param buffer The buffer to capture output.
 *
 * @param buf_size The size of the buffer.
 *
 * @param status The exit status of the child.
 *
 * @return For the parent, an object that converts to bool as false.
 * For the child, an object which converts to bool as true, and emit
 * an error message and kill the process on destruction if destructed.
 * The caller should arrange the child to call one of the exec series
 * functions so that the object is never destructed.  (Calling exit()
 * instead would cause valgrind to report errors.)  Note that any code
 * execution before the exec call is not counted towards code
 * coverage.
 */
inline ForkCaptureResult ForkCapture(char* buffer, std::size_t buf_size,
                                     int* status) {
  pid_t child;
  int pipefds[2];
  pipe(pipefds);
  if ((child = fork()) > 0) {
    close(pipefds[1]);
    memset(buffer, 0, buf_size);
    --buf_size;
    int num_read;
    while (buf_size && (num_read = read(pipefds[0], buffer, buf_size))) {
      buffer += num_read;
      buf_size -= num_read;
    }
    close(pipefds[0]);
    waitpid(child, status, 0);
    if (WTERMSIG(*status) == SIGSEGV || WTERMSIG(*status) == SIGBUS)
      ADD_FAILURE() << "ForkCapture child received SIGSEGV/SIGBUS";
    return ForkCaptureResult(0);
  } else {
    close(pipefds[0]);
    dup2(2, 3);
    dup2(pipefds[1], 1);
    dup2(pipefds[1], 2);
    return ForkCaptureResult(1);
  }
}

/**
 * Struct to find type of result of GetMockCall
 */
template<class TMockCls>
struct ResultOfGetMockCall {
  /** The result type */
  typedef BOOST_TYPEOF_TPL(&TMockCls::Call) type;
};

/**
 * Get the pointer to member function named Call, as used in the
 * MockFunction template
 */
template<class TMockCls>
typename ResultOfGetMockCall<TMockCls>::type GetMockCall(const TMockCls&) {
  return &TMockCls::Call;
}

}  // namespace cpfs
