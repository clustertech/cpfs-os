/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Logging facilities implementation for unit tests.
 */

#include "log_testlib.hpp"

#include <pantheios/backends/bec.file.h>

namespace cpfs {

namespace {

LogCallback curr_log_callback;  /**< Current callback */

}

LogRoute::LogRoute(LogCallback callback) {
  old_callback = curr_log_callback;
  curr_log_callback = callback;
}

LogRoute::~LogRoute() {
  curr_log_callback = old_callback;
}

}  // namespace cpfs

/**
 * Pantheios identity for unit tests.
 */
char PANTHEIOS_FE_PROCESS_IDENTITY[] = "unittest_log";

/**
 * Initialization.
 *
 * @param processIdentity The process identity.
 *
 * @param reserved Reserved field.
 *
 * @param ptoken Token to be passed to other functions.
 */
PANTHEIOS_CALL(int) pantheios_be_init(  // NOLINT(readability/casting)
    const PAN_CHAR_T* processIdentity,
    void* reserved,
    void** ptoken) {
  (void) processIdentity;
  (void) reserved;
  (void) ptoken;
  return 0;
}

/**
 * Deinitialization.
 *
 * @param token The token passed.
 */
PANTHEIOS_CALL(void) pantheios_be_uninit(void* token) {
  (void) token;
}

/**
 * Log an entry.
 *
 * @param feToken The token passed from frontend.
 *
 * @param beToken The token passed from backend.
 *
 * @param severity The severity of the message.
 *
 * @param entry The message.
 *
 * @param cchEntry The length of the message.
 */
PANTHEIOS_CALL(int) pantheios_be_logEntry(  // NOLINT(readability/casting)
    void* feToken,
    void* beToken,
    int severity,
    const PAN_CHAR_T* entry,
    std::size_t cchEntry) {
  (void) feToken;
  (void) beToken;
  // Uncomment the following to have the entries printed
  // printf("%d %s %ld\n", severity, entry, cchEntry);
  if (cpfs::curr_log_callback)
    cpfs::curr_log_callback(severity, entry, cchEntry);
  return 0;
}

/**
 * Sets or changes the log file name for a single back-end
 *
 * @param fileName The path of the log file to be used with the given back-end
 *
 * @param fileMask The bitmask controls flags to be interpreted in fileFlags
 *
 * @param fileFlags The flags that control the file creation
 *
 * @param backEndId The backend ID
 */
PANTHEIOS_CALL(int)  // NOLINT(readability/casting)
  pantheios_be_file_setFilePath(
    const PAN_CHAR_T* fileName,
    pan_be_file_init_t::pan_uint32_t fileMask,
    pan_be_file_init_t::pan_uint32_t fileFlags,
    int backEndId) {
  (void) fileName;
  (void) fileMask;
  (void) fileFlags;
  (void) backEndId;
  return 0;
}
