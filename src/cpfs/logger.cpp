/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Logging facilities implementation.  We use our own frontend, for
 * handling category information provided by the LOG macro.
 */

#include "logger.hpp"

#include <cerrno>
#include <cstdlib>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include <pantheios/backend.h>
#include <pantheios/backends/bec.file.h>
#include <pantheios/frontends/stock.h>

#include "common.hpp"

/**
 * A shorthand for return type of int for Pantheios functions, to
 * avoid misleading cpplint to think this is a style issue.
 */
#define PCINT PANTHEIOS_CALL(int)

/**
 * A shorthand for return type of void for Pantheios functions, to
 * avoid misleading cpplint to think this is a style issue.
 */
#define PCVOID PANTHEIOS_CALL(void)

/**
 * A shorthand for return type of const char* for Pantheios functions,
 * to avoid misleading cpplint to think this is a style issue.
 */
#define PCPCHAR PANTHEIOS_CALL(PAN_CHAR_T const*)

namespace {

/**
 * For storing severity settings.
 */
class LoggerSeveritySettings {
 public:
  LoggerSeveritySettings() {
    ResetSeverity();
  }

  /**
   * Check whether a (possibly extended) severity should be logged.
   *
   * @param severity The severity
   *
   * @return Whether it should be logged
   */
  bool IsSeverityLogged(int severity) {
    return (severity & 0x0f) <= GetSeverityCeiling(severity >> 4);
  }

  /**
   * Get the severity ceiling for a category.
   *
   * @param category The category
   */
  int GetSeverityCeiling(int category) {
    return severity_[category];
  }

  /**
   * Set the severity ceiling for a category.
   *
   * @param category The category.  If 0, set a default to be used for
   * all categories if not specified otherwise.
   *
   * @param ceiling The ceiling to set.  If -1, unset the severity
   * setting for the category / default.
   */
  void SetSeverityCeiling(int category, int ceiling) {
    if (ceiling != -1)
      settings_[category] = ceiling;
    else
      settings_.erase(category);
    ResetSeverity();
  }

 private:
  std::map<int, int> settings_;
  int severity_[cpfs::kMaxCategory + 1];

  /**
   * Reset the severity ceilings using the currently provided settings.
   */
  void ResetSeverity() {
    int def_severity = settings_[0];
    if (!def_severity)
      def_severity = pantheios::notice;
    for (int i = 0; i <= cpfs::kMaxCategory; ++i)
      severity_[i] = (settings_.find(i) == settings_.end()) ?
          def_severity : settings_[i];
  }
};

/**
 * The global severity setting object.
 */
LoggerSeveritySettings* g_severity_setting = 0;

}  // namespace

namespace cpfs {

int GetSeverityCeiling(int category) {
  return g_severity_setting->GetSeverityCeiling(category);
}

void SetSeverityCeiling(int category, int ceiling) {
  g_severity_setting->SetSeverityCeiling(category, ceiling);
}

void SetSeverityCeiling(std::string specs) {
  if (specs == "")
    return;
  std::vector<std::string> splitted;
  boost::split(splitted, specs, boost::is_any_of(":"));
  std::map<std::string, int> categories;
  categories["Server"] = kServerCategory;
  categories["Fim"] = kFimCategory;
  categories["FS"] = kFSCategory;
  categories["Cache"] = kCacheCategory;
  categories["Store"] = kStoreCategory;
  categories["Access"] = kAccessCategory;
  categories["Lock"] = kLockCategory;
  categories["Replicate"] = kReplicateCategory;
  categories["Performance"] = kPerformanceCategory;
  categories["Degraded"] = kDegradedCategory;
  for (unsigned i = 0; i < splitted.size(); ++i) {
    std::vector<std::string> field;
    boost::split(field, splitted[i], boost::is_any_of("="));
    if (field.size() == 0 || field.size() > 2U)
      throw std::runtime_error(std::string("Cannot understand severity field ")
                               + specs[i]);
    int category = 0;
    if (field.size() == 2U) {
      if (categories.find(field[0]) == categories.end())
        throw std::runtime_error(std::string("No log category ") + field[0]);
      category = categories[field[0]];
      field[0] = field[1];
    }
    const char* level_str = field[0].c_str();
    char* endptr;
    long val = std::strtol(level_str, &endptr, 10);  // NOLINT(runtime/int)
    errno = 0;
    if (*level_str == '\0' || *endptr != '\0' || errno || val < 0 || val > 7)
      throw std::runtime_error(std::string("Cannot understand level ")
                               + level_str);
    SetSeverityCeiling(category, val);
  }
}

std::string GroupRoleName(GroupId group_id, GroupRole role_id) {
  return (boost::format("%d-%d") % group_id % role_id).str();
}

void SetLogPath(std::string log_path) {
  static std::string last_log_path;
  if (log_path == "/") {
    if (last_log_path == "")
      log_path = "/dev/stderr";  // Not covered in combined test run
    else
      return;
  }
  if (log_path != "")
    last_log_path = log_path;
  pantheios_be_file_setFilePath(last_log_path.c_str(), 0, 0,
                                PANTHEIOS_BEID_ALL);
}

}  // namespace cpfs

/**
 * The frontend initialization function to use.
 */
PCINT pantheios_fe_init(void* reserved, void**ptoken) {
  (void) reserved;
  (void) ptoken;
  g_severity_setting = new LoggerSeveritySettings;
  return 0;
}

/**
 * The frontend deinitialization function to use.
 */
PCVOID pantheios_fe_uninit(void* token) {
  (void) token;
  delete g_severity_setting;
}

/**
 * The frontend get process identity function to use.
 */
PCPCHAR pantheios_fe_getProcessIdentity(void* token) {
  (void) token;
  return PANTHEIOS_FE_PROCESS_IDENTITY;
}

PCINT pantheios_fe_isSeverityLogged(void* token, int severity, int backEndId) {
  (void) token;
  (void) backEndId;
  return g_severity_setting->IsSeverityLogged(severity);
}
