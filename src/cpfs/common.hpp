#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define common types used by CPFS.
 */

#include <stdint.h>

#include <cstddef>
#include <string>
#include <vector>

#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L
/**
 * Unique pointer type to use
 */
#define UNIQUE_PTR std::unique_ptr
/**
 * Move function to use for UNIQUE_PTR
 */
#define MOVE(x) std::move(x)
#else
/**
 * Unique pointer type to use (auto_ptr)
 */
#define UNIQUE_PTR std::auto_ptr
/**
 * Move function to use for UNIQUE_PTR (no-op)
 */
#define MOVE(x) (x)
#endif

/**
 * Namespace for CPFS.
 */
namespace cpfs {

/** Inode numbers */
typedef uint64_t InodeNum;

/** Number of first level base directories for MS and DS data */
const unsigned kNumBaseDir = 4096;

/** Number of hex digit to use to denote base dir allocated */
const unsigned kBaseDirNameLen = 3;

/** Directory entry cookies, used during telldir() and seekdir() */
typedef int64_t DentryCookie;

/** The group id of server groups, starting from 0 */
typedef uint32_t GroupId;

/** The role no in a server group, normally between 0 to 4 */
typedef uint32_t GroupRole;

/** Client numbers, should only use 20 bits */
typedef uint32_t ClientNum;

/** Nonce type */
typedef uint64_t Nonce;

/** Number of bits allocated to client numbers */
const uint16_t kClientBits = 20U;

/** Maximum number of clients */
const ClientNum kMaxClients = (1U << kClientBits) - 1;

/** Represent the information that the node is not a client */
const ClientNum kNotClient = kMaxClients;

/** Represent a condition that the client number is not known yet */
const ClientNum kUnknownClient = kMaxClients + 1;

/** Connection-specific request IDs */
typedef uint64_t ReqId;

/** Fim types */
typedef uint16_t FimType;

/**
 * Store the options parsed for Admin client
 */
struct AdminConfigItems {
  std::vector<std::string> meta_servers;  /**< The meta servers */
  std::string log_severity;  /**< The log severity used by pantheios */
  std::string log_path;  /**< The log path used by pantheios */
  double heartbeat_interval;  /**< The heartbeat interval in seconds */
  double socket_read_timeout;  /**< The socket read timeout in seconds */
  bool force_start;  /**< Whether to forcibly start MS when rejected */
  std::string command;  /**< The command to run */
};

/**
 * MS states
 */
enum MSState {
  kStateHANotReady = 0,  /**< Server is running in HA mode, role not elected */
  kStateStandalone,  /**< Server is running in non HA mode */
  kStateActive,  /**< Server is elected as Active */
  kStateStandby,  /**< Server is elected as Standby */
  kStateFailover,  /**< Sticky state: failover in progress */
  kStateResync,  /**< Sticky state: resync in progress */
  kStateShuttingDown,  /**< Sticky state: Server is shutting down */
  kStateDegraded,  /**< Never set, only for CLI */
};

/**
 * Get a string representation of MSState
 *
 * @param state The meta server state
 *
 * @return String representation of the state
 */
inline const char* ToStr(MSState state) {
  static const char* state_map[] = {
    "Not ready",
    "Standalone",
    "Active",
    "Standby",
    "Failover",
    "Resync",
    "Shutting Down",
    "Degraded",
  };
  if (std::size_t(state) >= (sizeof(state_map) / sizeof(state_map[0])))
    return "Unknown";
  return state_map[state];
}

/** Number of server per DS group */
const unsigned kNumDSPerGroup = 5;

/** Number of bytes per segment */
const std::size_t kSegmentSize = 32768;

/** Number of bytes per checksum group */
const std::size_t kChecksumGroupSize = (kNumDSPerGroup - 1) * kSegmentSize;

/**
 * @param off A DSG offset
 *
 * @return Start of checksum group containing the offset
 */
inline std::size_t CgStart(std::size_t off) {
  return off / kChecksumGroupSize * kChecksumGroupSize;
}

/** The idle timeout of the socket write stream to generate heartbeat */
const double kDefaultHeartbeatInterval = 9.0;

/** The idle timeout of the socket read stream to delcare connection drops */
const double kDefaultSocketReadTimeout = 28.0;

/** The number of inodes to sync in each DS sync phase */
const unsigned kDefaultDataSyncNumInodes = 32U;

/** The max retry count for initial connection */
const unsigned kDefaultInitConnRetry = 3;

/** Number of segments in degraded cache, in units of kSegmentSize
 * bytes */
const unsigned kDegradedCacheSegments = 32768;

/** Max number of resync fims to send before reply is received */
const unsigned kMaxNumResyncFim = 1000;

/** Min interval for peer last seen time persistence, in unit of seconds */
const unsigned kLastSeenPersistInterval = 60;

/** Time for max replicate and write delay, in seconds > heartbeat interval */
const unsigned kReplAndIODelay = 120;

/** Amount of time between iterations of MS Cleaner */
const unsigned kMSCleanerIterationTime = 10;

/** Number of seconds for reply set in MS to hold replies */
const unsigned kMSReplySetMaxAge = 60;

/** Number of seconds for shutdown timeout */
const double kShutdownTimeout = 2 * 60.0;

/** Amount of time between iterations of DS Cleaner */
const unsigned kDSCleanerIterationTime = 10;

/** The minimum time when inode removal are retried */
const unsigned kDSMinInodeRemoveRetryTime = 15;

/** The maximum age of unauthenticated socket */
const unsigned kUnauthenticatedSocketAge = 5;

/** The maximum size of extended attribute value */
const uint64_t kXattrValueMax = 65536;

/** The maximum size of list of extended attribute names */
const uint32_t kXattrListMax = 65536;

/** Order to use for DurableRange */
const int kDurableRangeOrder = 5;

/**
 * Default location for the authentication key
 */
const char CPFS_KEY_PATH[] = "/etc/cpfs.key";

/**
 * Namespace for program entry points
 */
namespace main {
}  // namespace main

/**
 * Namespace for client modules
 */
namespace client {
}  // namespace client

/**
 * Namespace for server modules
 */
namespace server {

/**
 * Namespace for DS modules
 */
namespace ds {
}  // namespace ds

/**
 * Namespace for MS modules
 */
namespace ms {
}  // namespace ms

}  // namespace server

/**
 * Namespace for system test modules
 */
namespace systest {
}  // namespace systest

}  // namespace cpfs
