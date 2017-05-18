# ClusterTech Parallel Filesystem

The open-source version of CPFS, the
[ClusterTech](http://www.clustertech.com) Parallel File System.  CPFS
provides you with a high-performance distributed parallel filesystem
which is fault tolerant while being very easy to set up.  The
interface is highly POSIX compliant, so most applications do not need
to be rewritten to enjoy the high speed and fault tolerance provided
by CPFS.

A CPFS cluster consists of 1 or 2 (preferred) meta-data server, and a
multiple of 5 data servers.  Meta-data servers store directory data
and replicates them like a RAID-1 disk array.  Data servers store file
data and replicates them like a RAID-5 disk array.  FUSE-based clients
are included for accessing the contents.  Meta-data is fully
consistent.  Close-to-open consistency is implemented for data.

Data resync after a server crash is automatic, although the system
will not be responsive during the process.  Unlike an actual disk
array, the replication is file-level, allowing fast system recovery if
the amount of data to resync is not large.  At present, servers will
not respond to filesystem requests during the resync.

The project is hosted in
[GitHub](https://github.com/cpfs-clustertech/cpfs-os).  In its
"releases" section you can find installation packages for it.  Further
information can be found as follows:

  * [Installation and usage guidelines](docs/user-guide/install.md)
    * [In Chinese](docs/user-guide/install-cn.md)
  * [Build instructions](docs/user-guide/build.md)
  * [System design](docs/design)
  * [Development guidelines](docs/devel)

This project is released under AGPL 3.0.  Like most other projects,
this project uses a few third-party software, with
[their respective copyrights](docs/third-party-copyrights).
