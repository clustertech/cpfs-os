/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define utility for daemon process
 */

#include "daemonizer.hpp"

#include <unistd.h>
#include <sys/stat.h>

#include <cstdlib>
#include <stdexcept>

namespace cpfs {
namespace {

/**
 * Implementation of the Daemonizer.
 */
class Daemonizer : public IDaemonizer {
 public:
  void Daemonize() {
    pid_t pid = fork();
    if (pid < 0)
      throw std::runtime_error("Daemonizer failed to fork");  // Can't cover
    if (pid > 0)
      std::exit(0);
    umask(0);  // Change the file mode mask
    pid_t sid = setsid();  // Create a new SID for the child process
    if (sid < 0)
      throw std::runtime_error("Daemonizer failed to setsid");  // Can't cover
    if ((chdir("/")) < 0)
      throw std::runtime_error("Daemonizer failed to chdir");  // Can't cover
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }
};

}  // namespace

IDaemonizer* MakeDaemonizer() {
  return new Daemonizer;
}

}  // namespace cpfs
