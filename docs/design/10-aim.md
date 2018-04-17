# Design aim #

CPFS (Clustertech Parallel FileSystem version 2) is designed to
provide a solution for cluster storage that can achieve the following
goals:

 1. High and scalable I/O throughput.  This is the space that is
    normally played by other parallel filesystems like Lustre or
    Gluster.
 2. High small-file I/O performance.  This is the space that is
    normally played by other distributed filesystems like NFS.
 3. High availability.  In other systems, this is usually added
    using tools like RAID and cluster managers such as Pacemaker or
    RHCS.
 4. Cost effective in hardware.  Reduce the number of components that
    serve the only purpose to expose the capability of other
    components, like "OSS" or "MDS" in Lustre.
 5. Cost effective in software.  Rely on recent technologies and
    advances to reduce the software development costs.  Avoid costly
    development platform like in-kernel software development.
 6. Reasonable POSIX conformance, so that applications don't need to
    be rewritten for the FS.
 7. Portable across many systems.  Currently the system would run only
    on Linux, but in the future it should be possible to provide an
    API for BSD or even Windows systems to access.

We achieve the above in our filesystem with the following strategy.

  * For (1), we use a single (logically) meta-data server to couple
    with multiple data server.  Clients access these servers directly.
    Data servers can be added in RAID groups (see below).
  * For (2), we make the access API light-weight.  Clients use caches
    to improve the system performance.  Adding these to a low-latency
    network inter-connect, we would have good IOPS.
  * For (3), we divide the data server into RAID groups.  Data in each
    RAID group have redundancy within the group, to protect against
    the loss of servers.  A couple of meta-data servers provide
    redundancy for them.  The data in each meta-data server and data
    server can also be stored using RAID, to protect against the loss
    of disks within servers.
  * For (4), we integrate the storage initiators and the targets.
    Both the meta-data server and the data server directly store their
    data in their local storage.  This way we fully utilize the
    capability of all servers in the system.
  * For (5), we use FUSE (Filesystem in USEr space) to build our
    software, coupled with Boost Asio to provide asynchronous
    programming constructs.  The system directly uses the available
    local filesystem as storage, rather than devising its own.  These
    are made possible with the recent advance of local filesystem
    performance.
  * For (6), we provide the normal filesystem API.  Client cache are
    invalidated actively, so that modification done by one client is
    quickly reflected in other clients.
  * For (7), we only use libraries that are sufficiently portable.
