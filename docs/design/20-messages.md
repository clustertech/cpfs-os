# CPFS file information messages (FIM) #

CPFS clients need to communicate with the servers.  CPFS servers
also need to communicate with each other.  Both are achieved by CPFS
File Information Messages (FIM) in a TCP stream.  Here we explain the
design of FIMs in general.  For detailed FIM format, refer to the
source code.

FIMs are sent in native byte-order of the sender.  So a little-endian
(big-endian) machine will send FIMs in little-endian (big-endian).
For cross-endian operations, the receiver may need endian conversions
of fields (may not be supported in initial versions).

All fields in a FIM are of fixed length, except that some FIM types
have a trailing "tail" field.  This makes it easy for a C / C++ to
access fields in FIMs.

All fixed length fields in a FIM have simple C++ fixed-width types,
e.g., `char`, `uint32_t`, etc.  An `int` or `struct stat` will never
be used.  This allows correct 32-bit to 64-bit communication.

Each FIM has the following 16-byte header.

  * `uint32_t len`.  This allows segmentation of the FIM stream.
  * `uint16_t type`.  There are multiple types of FIMs.  Whether a
    FIM is a "request" or a "response" is implied by the `fimtype`.
    The FIM type also determines the FIM structure.
  * `uint16_t protocol`.  A short protocol version string (initially,
    1) will be attached to each FIM.  This allows cross-version
    communication in the future.  The low-order byte will always be
    larger than the high-order byte, for byte-order detection.
  * `int64_t req_id`.  A request ID will be attached to all FIM, so
    that "response" FIMs can be matched to "request" FIMs of the same
    connection.  If a FIM needs no response, the request ID -1 may be
    used.

After that, it might contain other fields.  E.g.,

  * `uint64_t inode`.  The inode number being operated on.  The FIM
    interface uses a low-level API, where the inode number is used to
    identify files.
  * `uint64_t optime`.  The time of operation.  The servers do not use
    its own date/time in the user-visible fields.  Instead, when a
    file operation is performed, the client also sends the local time
    to the server, and the server always trusts this time to be
    accurate.
  * `uint64_t off` and `uint64_t size`.  The offset and size of the
    file being operated on.  Note that in some cases (e.g., a FIM
    requesting for file write or xattr set), there is not a need for
    passing `size`, since it is implied by `msglen`.
  * `uint32_t uid`, `uint32_t gid`, `uint32_t mode` and `uint32_t
    nlink`.  All of them are expected to be 32-bit numbers.
  * `uint32_t name_len`: for calls requiring 2 variable length fields
    (e.g., `symlink`, `rename` and `setxattr`), the `name_len` field
    signifies the dividing point (as the length of the first field).
