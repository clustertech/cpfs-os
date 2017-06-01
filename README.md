# ClusterTech Parallel Filesystem

This is open-source version of CPFS, the
[ClusterTech](http://www.clustertech.com) Parallel File System.  CPFS
is a high-performance distributed parallel filesystem which is fault
tolerant while being very easy to set up.  The interface is highly
POSIX compliant, so most applications do not need to be rewritten to
enjoy the high speed and fault tolerance provided by CPFS.

A CPFS cluster consists of 1 or 2 (preferred) meta-data server, and a
multiple of 5 data servers (compile time constant for now).  Meta-data
servers store directory data and replicate them like a RAID-1 disk
array.  Data servers store file data and replicate them like a RAID-5
disk array.  The server data is accessed via FUSE-based clients.
Node-local cache provided by FUSE is used, with cache coherency
implemented.  Meta-data is fully coherent.  Close-to-open coherency is
implemented for data.

The software supports automatic failover and data resynchronization
upon server crash and recovery.  At present, the system is not
responsive during resynchronization.  (We are now trying to modify the
software to allow partially online resynchronization.)  Unlike an
actual disk array, the replication is file-level, allowing fast system
recovery if the amount of data to resync is not large.

The project is hosted in
[GitHub](https://github.com/cpfs-clustertech/cpfs-os).  In its
"releases" section you can find installation packages for it.  Further
information can be found as follows:

  * [Installation and usage guidelines](docs/user-guide/install.md)
    * [In Chinese](docs/user-guide/install-cn.md)
  * [Build instructions](docs/user-guide/build.md)
  * [System design](docs/design)
  * [Development guidelines](docs/devel)

CPFS is released under AGPL 3.0.  It uses a few third-party libraries,
with [their respective license](docs/third-party-copyrights).
