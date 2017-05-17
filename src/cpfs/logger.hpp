#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define logging facilities.
 */

#include <stdint.h>

#include <cstddef>
#include <cstring>
#include <string>

#include <boost/lexical_cast.hpp>  // IWYU pragma: export
#include <boost/scoped_array.hpp>

#include <pantheios/inserters/fmt.hpp>  // IWYU pragma: export
// IWYU pragma: no_include "pantheios/inserters/fmt.hpp"
#include <pantheios/inserters/integer.hpp>  // IWYU pragma: export
#include <pantheios/pantheios.hpp>  // IWYU pragma: export
// IWYU pragma: no_include "pantheios/pantheios.h"
// IWYU pragma: no_include "pantheios/./internal/stock_levels.hpp"
// IWYU pragma: no_include "pantheios/severity/levels.hpp"

#include "common.hpp"

namespace boost {

template <class Y> class shared_ptr;
template <class Y> class intrusive_ptr;

}  // namespace boost

namespace cpfs {

/**
 * Log message category for server startup and connection status
 * change among them.
 */
const int kServerCategory = 1;
/**
 * Log message category for Fim generation, sending, receiving,
 * parsing and processing.
 */
const int kFimCategory = 2;
/**
 * Log message category for filesystem operations of CPFS.
 */
const int kFSCategory = 3;
/**
 * Log message category for cache handling and tracking.
 */
const int kCacheCategory = 4;
/**
 * Log message category for the underlying store keeping data in
 * CPFS.
 */
const int kStoreCategory = 5;
/**
 * Log message category for access control in CPFS.
 */
const int kAccessCategory = 6;
/**
 * Log message category for locking.
 */
const int kLockCategory = 7;
/**
 * Log message category for replication.
 */
const int kReplicateCategory = 8;
/**
 * Log message category for performance-related messages.
 */
const int kPerformanceCategory = 9;
/**
 * Log message category for degraded mode messages.
 */
const int kDegradedCategory = 10;
/**
 * Highest category number.
 */
const int kMaxCategory = 10;

/**
 * Get severity ceiling for a category.
 *
 * @param category The category
 */
int GetSeverityCeiling(int category);

/**
 * Set severity ceiling for a category.
 *
 * @param category The category.  The special category 0 will set
 * ceiling of all categories.
 *
 * @param ceiling The new ceiling
 */
void SetSeverityCeiling(int category, int ceiling);

/**
 * Set severity ceiling using a string specification.
 *
 * @param specs The specification.  It should be a colon-separated
 * string of either a simple number between 0 to 7, or in the form
 * Category=level, where Category is one of the category ids such as
 * Fim or Store.
 */
void SetSeverityCeiling(std::string specs);

/**
 * Convert the given group ID and role to string representation for logging
 *
 * @param group_id The DS group ID
 *
 * @param role_id The DS role ID
 *
 * @return The string of "<group>-<role>"
 */
std::string GroupRoleName(GroupId group_id, GroupRole role_id);

/**
 * Set the log path to use.  If empty, reuse the path of the last call
 * (causes a re-open).  If it is the special word "/", set it to
 * "/dev/stderr" if it has not been set before, but do nothing otherwise.
 *
 * @param log_path The log path to use
 */
void SetLogPath(std::string log_path);

/**
 * Convert a value to string during Pantheios log.
 *
 * @param val The value
 */
template <typename TVal>
std::string PanValueConvert(const TVal& val) {
  return boost::lexical_cast<std::string>(val);
}

/**
 * Convert a boost::shared_ptr to string during Pantheios log.
 *
 * @param val The value
 */
template <typename TVal>
std::string PanValueConvert(const boost::shared_ptr<TVal>& val) {
  if (!val)
    return "(null)";
  return PanValueConvert(*val);
}

/**
 * Convert a boost::intrusive_ptr to string during Pantheios log.
 *
 * @param val The value
 */
template <typename TVal>
std::string PanValueConvert(const boost::intrusive_ptr<TVal>& val) {
  if (!val)
    return "(null)";
  return PanValueConvert(*val);
}

/**
 * Generic inserter to serve most cases.  Unless specified otherwise
 * with a specific PanValueConvert() template instantiation, it will
 * call boost::lexical_cast<std::string> to generate the string to
 * print.  Such instantiation made is for the boost::shared_ptr and
 * boost::intrusive_ptr classes, which would print "(null)" or the
 * content of the pointer.
 */
template <typename TVal>
class PanValue {
 public:
  /**
   * @param val The value to be printed
   */
  explicit PanValue(const TVal& val) : val_(val) {}

  /**
   * @param other The value to copy
   */
  PanValue(const PanValue<TVal>& other) : val_(other.val_) {}

  /**
   * @return The string representation
   */
  const char* Data() const {
    Construct();
    return repr_.get();
  }

  /**
   * @return The length of the string representation
   */
  std::size_t Length() const {
    Construct();
    return len_;
  }

 private:
  const TVal& val_; /**< The value */
  mutable boost::scoped_array<char> repr_; /**< The representation */
  mutable int len_; /**< The length of representation */

  void Construct() const {
    if (repr_)
      return;
    std::string repr = PanValueConvert(val_);
    len_ = repr.size();
    repr_.reset(new char[len_]);
    std::memcpy(repr_.get(), repr.data(), len_);
  }
};

/**
 * Generate a PanValue class object
 */
template <typename TVal>
PanValue<TVal> PVal(const TVal& v) {
  return PanValue<TVal>(v);
}

/**
 * Inserter data function for PanValue for Pantheios.
 *
 * @param pl The PanValue object
 *
 * @return The string representation
 */
template <typename TVal>
const char* c_str_data_a(const PanValue<TVal>& pl) {
  return pl.Data();
}

/**
 * Inserter length function for PanValue for Pantheios.
 *
 * @param pl The PanValue object
 *
 * @return The length of the string representation
 */
template <typename TVal>
std::size_t c_str_len_a(const PanValue<TVal>& pl) {
  return pl.Length();
}

/**
 * Return the extended level for a level and a category.
 *
 * @param level The level, should be one of debug, informational,
 * notice, warning, error, critical, alert and emergency.
 *
 * @param category The category, should be a 28-bit integer.
 */
#define PLEVEL(level, category) pantheios::level(                       \
    cpfs::k ## category ## Category)

/**
 * Log a message of a certain level and category.
 *
 * @param level The level, should be one of debug, informational,
 * notice, warning, error, critical, alert and emergency.
 *
 * @param category The category, should be a 28-bit integer.
 */
#define LOG(level, category, ...)                                       \
  pantheios::log(PLEVEL(level, category), __VA_ARGS__)

/**
 * Shorthand to create a Pantheios log inserter for an integer.
 */
#define PINT pantheios::integer

/**
 * Shorthand to create a Pantheios log inserter for hex integers.
 */
template <typename TVal>
inline pantheios::integer PHex(TVal val) {
  return pantheios::integer(val, sizeof(TVal) * 2,
                            pantheios::fmt::hex | pantheios::fmt::zeroPad);
}

/**
 * Shorthand to create a Pantheios log inserter for hex chars.
 */
template <>
inline pantheios::integer PHex<char>(char val) {
  return pantheios::integer(uint8_t(val), 2,
                            pantheios::fmt::hex | pantheios::fmt::zeroPad);
}

/**
 * Shorthand to create a Pantheios log inserter for hex unsigned chars.
 */
template <>
inline pantheios::integer PHex<unsigned char>(unsigned char val) {
  return pantheios::integer(uint8_t(val), 2,
                            pantheios::fmt::hex | pantheios::fmt::zeroPad);
}

}  // namespace cpfs

/**
 * The frontend severity ceiling check function to use.
 */
PANTHEIOS_CALL(int)  // NOLINT(readability/casting)
pantheios_fe_isSeverityLogged(void* token, int severity, int backEndId);
