# ClusterTech Parallel Filesystem

The open-source version of CPFS, the ClusterTech Parallel File System.
CPFS provides you with a high-performance distributed parallel
filesystem which is fault tolerant while being very easy to set up.
The interface is highly POSIX compliant, so most applications do not
need to be rewritten to enjoy the high speed and fault tolerance
provided by CPFS.

A CPFS cluster consists of 1 or 2 (preferred) meta-data server, and a
multiple of 5 data servers.  Meta-data servers store directory data
and replicates them like a RAID-1 disk array.  Data servers store file
data and replicates them like a RAID-5 disk array.  FUSE-based clients
are included for accessing the contents.  Meta-data is fully
consistent.  Close-to-open consistency is implemented for data.

Data resync after a server crash is automatic, although the system
will not be responsive during the process.  Unlike an actual disk
array, the replication is file-level, allowing fast system recovery if
the amount of data to resync is not large.
