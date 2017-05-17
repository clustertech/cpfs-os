#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define Console.
 */
#include <inttypes.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "common.hpp"

namespace cpfs {

/**
 * Simple wrapper for input / output from Console
 */
class IConsole {
 public:
  virtual ~IConsole() {}

  /**
   * Read a line of user input from console
   *
   * @return The input obtained
   */
  virtual std::string ReadLine() = 0;

  /**
   * Print a line to console
   *
   * @param line The line
   */
  virtual void PrintLine(const std::string& line) = 0;

  /**
   * Print a table to console
   *
   * @param rows Rows of a table to print. The first row contains the header
   */
  virtual void PrintTable(
      const std::vector<std::vector<std::string> >& rows) = 0;
};

}  // namespace cpfs
