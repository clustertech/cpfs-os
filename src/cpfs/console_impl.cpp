/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of Console.
 */

#include "console.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace cpfs {

namespace {

const std::size_t kMaxInputLen = 256;  /**< The max length from user input */

/**
 * Implementation of the Console
 */
class Console : public IConsole {
 public:
  /**
   * Create a console
   *
   * @param in The standard input
   *
   * @param out The standard output
   *
   * @param err The standard error
   */
  Console(FILE* in, FILE* out, FILE* err) : in_(in), out_(out), err_(err) {}

  std::string ReadLine() {
    std::fprintf(out_, "> ");
    std::fflush(out_);
    char input[kMaxInputLen] = {'\0'};
    std::fgets(input, kMaxInputLen, in_);
    const std::size_t kInputLen = strlen(input);
    if (kInputLen > 1 && input[kInputLen - 1] == '\n')
      input[kInputLen - 1] = '\0';
    return std::string(input);
  }

  void PrintLine(const std::string& line) {
    std::fprintf(out_, "%s", (line + "\n").c_str());
    std::fflush(out_);
  }

  void PrintTable(const std::vector<std::vector<std::string> >& rows) {
    // Scan optimal column widths
    std::vector<std::size_t> widths(rows[0].size(), 0);
    for (std::size_t row_n = 0; row_n < rows.size(); ++row_n)
      for (std::size_t col_n = 0; col_n < rows[row_n].size(); ++col_n)
        widths[col_n] = std::max(rows[row_n][col_n].length(), widths[col_n]);
    // Print header
    PrintRow(rows[0], widths);
    // Print horizontal line, each column consumes two ' ' and one '|'
    std::string separator = std::string(rows[0].size() * 3, '-');
    for (std::size_t col_n = 0; col_n < widths.size(); ++col_n)
      separator += std::string(widths[col_n], '-');
    std::fprintf(out_, "%s\n", separator.c_str());
    // Print rows
    for (std::size_t row_n = 1; row_n < rows.size(); ++row_n)
      PrintRow(rows[row_n], widths);

    std::fflush(out_);
  }

 private:
  FILE* in_;  /**< The input */
  FILE* out_;  /**< The output */
  FILE* err_;  /**< The error display */

  /**
   * Print table row
   */
  void PrintRow(const std::vector<std::string>& columns,
                const std::vector<std::size_t>& widths) {
    for (std::size_t i = 0; i < columns.size(); ++i) {
      int padding = widths[i] - columns[i].length() + 1;
      std::fprintf(out_, " %s" "%*s", columns[i].c_str(), padding, " ");
      if (i != (columns.size() - 1))
        std::fprintf(out_, "|");
    }
    std::fprintf(out_, "\n");
  }
};

}  // namespace

IConsole* MakeConsole(FILE* in, FILE* out, FILE* err) {
  return new Console(in, out, err);
}

}  // namespace cpfs
