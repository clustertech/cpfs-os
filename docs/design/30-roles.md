# CPFS roles #

Machines in CPFS serve one of a few roles: the filesystem client
(FC), meta-data server (MS) or data server (DS).  There are at most 2
MSs, while there can be many DSs organized as DS groups each
containing 5 DSs.  The arrangement is described in more details in
this document.

## CPFS clients (FC) ##

Clients, denoted as FCs, "mounts" the filesystem for users to access,
and run other applications utilizing the filesystem.  It is
implemented as a FUSE-based program to interact with the MSs and DSs.

An FC makes a persistent connection to the active MS and all DSs of
the system, and handle requests from FUSE, e.g., create or modify
files or directories, change their attributes, etc.  Most operations
are forwarded to the MS or the DSs.

When the FC connects to each server, it goes through a basic
challenge-resopnse authentication with locally stored secret.  This
ensures that only the system builders can interact with the internal
parts of the CPFS filesystem, as long as the network itself is
properly protected.  On the other hand, to avoid degrading system
performance, further communications are not encrypted or signed.

Upon the first connection to the MS, the FC is allocated a client
number.  This client number is used to determine the range of request
IDs that the client will use in its requests, so that such request IDs
are globally unique.  Upon failover the same client numbers are
requested from the new MS, for the new MS to identify which Fims have
previously been processed before.

There may be information that we want to provide other than regular
filesystem API.  E.g., we might want to provide an API allowing one to
know the usage in each DS.  Such data are provided via the `ioctl`
interface rather than a separated API.

## CPFS meta-data servers (MS) ##

Meta-data servers (denoted MS) store the filesystem directory
structure as well as file attributes, using a 2-level local directory.
For illustration, suppose we would like to represent the following
directory structure (as seen in FCs):

    [mount_point] (directory, inode 1)
    +- a (regular file, inode 2)
    +- b (directory, inode 3)
    |  +- c (symlink to ../a, inode 4)
    |  +- d (block device file, major/minor 1/5, inode 5)
    |  \- e (regular file, inode 6)
    \- f (regular file, inode 6)

Since we cannot dictate the inode number used by the underlying
filesystem, and since we cannot search for a file in the underlying
filesystem based on inode number, the above directory must be
represented in a more accessible form in MSs.  We choose to represent
it using a local directory with the following structure.

    [export_point]
    +- 000
    |  +- 0000000000001
    |  |  +- a (symlink to R0000000000000002)
    |  |  +- b (directory with xattr user.cpfs.ino:0000000000000003)
    |  |  \- f (symlink to R0000000000000006)
    |  +- 0000000000001x (empty file)
    |  +- 0000000000002 (file containing a big hole)
    |  +- 0000000000002x (empty file)
    |  +- 0000000000003
    |  |  +- c (symlink to L0000000000000004)
    |  |  +- d (symlink to K0000000000000005)
    |  |  \- e (symlink to R0000000000000006)
    |  +- 0000000000003x (empty file)
    |  +- 0000000000004 (symlink to ../a)
    |  +- 0000000000004x (empty file)
    |  +- 0000000000005 (block device file, major/minor 1/5)
    |  +- 0000000000005x (empty file)
    |  +- 0000000000006 (file containing a big hole)
    |  +- 0000000000006.1 (same inode as export_point/0...06 in MS)
    |  +- 0000000000006x (empty file)
    |  \- c
    +- 001
    \- ...

This shows the following characteristics of the mapping between the
filesystem directory structure and the representation in MS:

  1. Each inode number in hexidecimal is broken into two parts.  The
     first 3 hex digits are used to choose a directory to store the
     file representating it.  The remaining 13 hex digits will be
     discussed below.  Upon first initialization, the 4096 possible
     directories (000, 001, ..., fff) are created.
  1. A symlink called `c` holds the next unallocated inode number for
     each of the first-level directory.
  1. Each inode in the filesystem (e.g., inode 4) is represented in MS
     by an entry in the second level of the MS representation (here,
     `/000/0000000000004`), of the same type as the file it represents
     (here, a symlink).  It is named by the 13 hex digits after those
     used to choose the first-level directory.  We call such entries
     in MS "inode files" or "inode directories".  The root directory
     has the fixed inode 1, and is represented by the inode directory
     `/000/0000000000001`.
  1. Each inode file has a corresponding empty control file storing
     extended attributes that CPFS uses.  The name is the same except
     for a "x" suffix.  E.g., the root directory has the control file
     `/000/0000000000001x`.
  1. Each directory entry in the filesystem (e.g., `/b/c`) is
     represented in MS by a directory entry in the third level of the
     MS representation (here, `/000/0000000000003/d`), under the inode
     directory representing the parent (here, `/000/0000000000003`,
     which represents `/b`).
  1. For directory entries which are subdirectories (e.g., `/b`), the
     representation in MS (here, `/000/0000000000001/b`) is also a
     directory.  It has an extended attribute `cpfs.ino` in the user
     namespace to keep the inode number of the directory it represents
     (here, `0000000000000003`).  This representation ensures that the
     directory have the correct link count, for the benefits of
     programs like `find` which takes an inode count of 2 to mean
     "this directory has no subdirectories and thus we don't need to
     pre-scan it for subdirectories during depth-first search".
  1. For other directory entries (e.g., `/a`), the representation in
     MS (here, `/000/0000000000001/a`) is a symlink with the link
     target being the inode number of the entry (here,
     `R0000000000000002`).  The file type is encoded as a character:
     R, L, H, K, S and P represents Regular files, symLinks, cHaracter
     devices, blocK devices, Sockets and named Pipes respectively.
     The character I would represent dIrectories, but it is actually
     not used because they would not be represented by symlinks.  A
     symlink without such file type identification is also possible
     (this is the reason we avoided A-F as representation characters),
     for legacy reasons (the type identification would be added upon
     full resync).  The type information is returned as part of the
     readdir FUSE method call.
  1. Multiple directory entries (e.g., `/b/e` and `/f`) may share the
     same inode (here, `000/0000000000006`), a situation caused by
     hard-linking.  We make suffixed inode files in the second level
     of the MS representation (here `/000/0000000000006.1`) to
     maintain correct link count in the MS.  We maintain the invariant
     that if there are n directory entries in the filesystem referring
     to a non-directory inode x, the representation would have n - 1
     suffixed inode files starting from `x.1` referring to the same
     file `x`.  This invariant is utilized by operations like `link()`
     and `unlink()` creating / removing such files.

When there are many files, each of the first level directory of the
export point contains many inode files / directories.  The local
filesystem of the MS thus needs a hash-based directory (e.g., ext4
htree) to provide adequate performance.  This requirement likely leads
to superior performance than home-brew directory hashing methods, say,
hashing inode number and store them in multiple levels, because less
hashing work and directory traversal work are required by the MS.

On the other hand, we do have a first level directory, representing
the first 12 bits of its inode number.  It is used to improve
allocation for filesystems such as XFS, which have a concept of
allocation group and parallelize better on different groups.  Having
multiple directories thus allows better parallelism within the kernel.
For simplicity, the splitting is consistent with DS: directories are
created with random first 12 bits, while files have the first 12 bits
the same as its parent.  So if the files in the above layout are
created in-place, it is likely that b, c, d and e have the same first
12 bits in the inode number (chosen randomly when b is created), but
they are unlikely to be all 0.  The top bits are used for the purpose,
so that we can have a fully sorted list of inodes by listing each of
the directories is sequence.

Only contents of regular files are stored outside the MS.  The regular
files in the MS contain only file holes, created by the `truncate`
system call.  The DS groups storing the file are kept in an extended
attribute of the control file, provided to FCs during file `open` so
that FCs can send requests to the right DSs.

All other information of the files named by their inodes (1, 2, 3, 4,
5 and 6 above), like the ownership, permissions, permissions, size,
etc., are the information of the file / directory they represents.
There is one exception: the ctime is stored in an extended attribute
of the control file, not in the inode.  This allows the client time to
be used instead of the server time (the Unix filesystem API does not
allow modification of file ctime).  This also enables us to fully
recover the externally visible filesystem state when rebuilding the
MS.

When files are opened for writing, it is inefficient to keep all these
information up-to-date: the FC will have to send a Fim to the MS to
update the current file size and mtime for every single write that it
performs, apart from sending the information to the appropriate DS.
This easily overwhelms the MS leading to a performance bottleneck.
Instead, we record that the inode is "volatile" whenever an inode is
opened for writing by any FC.  Each file update causes the DS to keep
such attributes.  This attribute is replicated to the DS holding the
checksum together with the checksum update.  Whenever `getattr` is
called on the inode, the MS queries the associated DS for the
attribute updates, and the consolidated result is provided to the FC.
This also happens when the inode is finally closed, and once the
updated attributes are obtained and saved in the MS, the MS clears the
volatile status of the inode.  As an optimization, this is done only
if an FC reports that the inode attribute is dirty, i.e., the FC has
written something to the file.  If the MS finds that no FC has written
to a volatile file at such times, the attribute updates are skipped.
Finally, if `setattr` is called and have these attributes changed, the
operation will first lock the DS and update the attributes there as
well.

It should be noted that symlinks are used extensively in the
filesystem.  They must not be directly dereferenced by the MS.  This
is not as difficult as it might seem, because (1) files within
inode-named directories are all known in advance to be symlinks, and
(2) files represented by inode-named files will never have type
changed, so by a simple stat call we know the file type and will never
have race conditions associated.

On the other hand, if the inode file itself is a symlink, it is a
symlink to be interpreted in the FCs.  Care must be taken by MS to
never make a symlink dereferencing system call to it, like access(),
stat() or creat().  Only non-derefercing system calls like lstat(),
readlink(), rename() or unlink() may be used for them.

There may be up to 2 MS in the system, one serving as the active MS
and the other being a backup.  The active and backup MS is elected
(using a randomized approach) rather than hard-coded.  When a DS or a
FC is started, it connects to both MS, but backup MS closes the
connection immediately afterwards, rather than completing the initial
handshake.

Some additional information are kept in the underlying filesystem.  At
present, a directory called `d` holds the dirty inodes, and a
directory called `r` holds additional resync information, in
particular, which inodes have recently been removed (so that they can
be removed in the standby MS as well after a potential failover /
resync).

The system also keeps other states of the filesystem, e.g., the locks
held by FCs on various files, or the write token possessed by FCs.
These are held in memory of MS, and are re-synchronized if a fail-over
takes place.  When both the MS and its backup fail, these data will
vanish, and thus the whole filesystem (FCs, MSs and DSs) needs to be
restarted.  This can lead to the loss of recently written data.

In the implementation, the MS is organized as one I/O thread getting
data from all other servers and clients, together with a few worker
threads handling such requests, getting and putting data into the data
directory as described above.  The inode number of the operation is
used to choose a worker thread for handling the request.  For
operations with multiple inode numbers, we use the "primary" inode
number of the operation, which is typically the inode of the parent
directory where an entry is going to be created.  Mutexes are also
used to prevent corruption of the data directory when it is accessed
in parallel.  In fact, the choice of primary inode above is mainly to
facilitate the use of such mutexes.  For more information about this,
see the comment in fims.hpp.

## CPFS data servers (DS) ##

The data servers, denoted DSs, hold file data and checksum.  In
contrast to MSs, the structure of DSs is simple.  It consists only of
something like the following:

    export_point
    +- 000
    |  +- 0000000000001.d (the data it stores for /a)
    |  +- 0000000000001.c (the checksum it stores for /a)
    |  +- 0000000000006.d (the data it stores for /b/e)
    |  \- 0000000000006.c (the checksum it stores for /b/e)
    +- 001
    ...

The `.d` file holds the file data, while the `.c` file holds the
checksum.  They are kept in separated files, so that when we read from
a file in a cluster working fully, only the `.d` files are read, and
the `.c` files do not need to be read at all.  This aims to avoid
pollution in the read-ahead buffer.

The structure is the same for the whole group of DSs.  Like MS, the
first level directory is the first 3 hex digits of the inode number,
and the second level directory is the remaining 13 hex digits.  The
two level structure is mainly to play nice with filesystems like ext4
and XFS, which has a concept of block group or allocation group.
Files within the same directory is likely to be given the same group,
thus are less costly to seek around.  In CPFS, if a new directory is
created, a random first-level directory is chosen, and an inode number
is allocated from it.  This has the effect of allocating new
directories in random groups, like local filesystems.  On the other
hand, when a new file is created under a directory, it is allocated an
inode number using the same first level directory as the directory
containing it.  Again, this makes the allocation strategy consistent
with local filesystems.

Each DS group contains 5 servers.  File data is partitioned into
segment groups.  Each segment group is fixed in size, at 128 kbyte,
and is divided into 4 segments, each of 32 kbytes.  The segments of a
file are thus arranged like this (`g` is segment group number, `S` is
segment number):

    g |           S
    --+------------------------
    0 |  0     1     2     3
    1 |  4     5     6     7
    2 |  8     9    10    11
      ...

A checksum segment is created by XOR of these 4 segments.  Segment
groups are distributed to DS groups in round-robin fashion, and within
each DS group, the data is distributed to the DS in round-robin
fashion.  Consider an inode `i = 3` to be distributed to `n = 2` DS
groups.  Segment groups 0, 2, 4, ... would then be given to the first
DS group, while segment groups 1, 3, 5, ... given to the second group.
We start from the third DS (because `i = 3`) for each segment group,
leading to the following layout for data:

    Group  |           G[0]          |           G[1]          |
    Server |  0    1    2    3    4  |  0    1    2    3    4  |
      g    |                         S                         |
    -------+-------------------------+-------------------------+
     0/1   |  2    3         0    1  |  6    7         4    5  |
     2/3   | 11         8    9   10  | 15        12   13   14  |
     4/5   |      16   17   18   19  |      20   21   22   23  |
     6/7   | 24   25   26   27       | 28   29   30   31       |
     8/9   | 33   34   35        32  | 37   38   39        36  |

There are "holes" in the above layout, because each DS group has 5 DS,
while each segment group has 4 segments.  Whenever we see a hole
above, the server would hold a checksum of the corresponding segment
group.  So the checksum segment for segment 0 to 3 will be stored in
server 2 of the `G[0]`, and checksum segment for segment 12 to 15 will
be stored in server 1 of `G[1]`.

Here are the formula to aid the implementation.  Suppose we need to
access one segment of user data:

  * Inode `i`
  * Server groups `G`, with `n` groups (as noted below, `G` is
    different for different files)
  * Segment size `N` bytes, `N = 32768`
  * File position `x` to `x + N - 1`, where `x % N == 0`

So we are accessing segment `S = x / N`.  We first split this segment
number to a group number and a segment number within the group
(integer arithmetic is used the formula here).

  * Segment group `g = S / 4`
  * Segment number in segment group `s = S % 4`
  * Segment group within DS group `g' = g / n`
  * Start segment number of segment group in DS group `T = g' * 4`
  * Segment number within DS group `S' = s + T`

Here the segment group and number within DS group is a renumbering of
the segments so that each DS group sees contiguous segment numbers.
E.g., in the above example, `g'` and `S'` looks like this:

    Group  |           G[0]          |           G[1]          |
    Server |  0    1    2    3    4  |  0    1    2    3    4  |
      g'   |                         S'                        |
    -------+-------------------------+-------------------------+
      0    |  2    3         0    1  |  2    3         0    1  |
      1    |  7         4    5    6  |  7         4    5    6  |
      2    |       8    9   10   11  |       8    9   10   11  |
      3    | 12   13   14   15       | 12   13   14   15       |
      4    | 17   18   19        16  | 17   18   19        16  |

Then the DS used can be found with:

  * Server group = `G[g % n]`
  * Server within group for data = `(S' + i) % 5`
  * Server within group for checksum = `(T + i + 4) % 5`

And the data location can be found with:

  * File position of data within server = `(S' / 5) * N`
  * File position of checksum within server = `(g' / 5) * N`

The DS storing the checksum of a segment is called the checksum DS of
the segment.  The above computation makes it depends on the inode
number (`i`) and the segment group (`g`).  By randomizing the ordering
of DS in `G` for different files, we attempt to spread the checksum
load to all servers.  In particular, when a file is first created, the
current policy is used to determine the number of groups to be used.
Then we determine the groups to be used, by choosing a group at
probability depending on the amount of space remaining in each group
(the exact algorithm to use is not yet determined).  Then the ordering
of the resulting group list is randomized, and is recorded in the
xattrs of the control file in the meta-data server.

Recall that the FC sends information to DS about the current file size
and mtime, which is replicated to the checksum server.  In both case
the information is kept in extended attributes, as `user.fs` and
`user.mt` respectively.  This is always modified to `.d` file, and the
`.c` file will not receive the update.  During checksum update, if the
`.d` file is not present, it will be created as well.  This ensures
that whenever there is data stored, the `.d` file is present.

Some coherency mechanism is needed so that the segment accesses,
especially the checksum segment accessed, do not have race conditions.
During normal operations, each file write is processed by the DS with
a file read followed by a file write and a *checksum change request*
using the XOR of the old and new file data[^OPT-SMALL].  The length of
the checksum change may be shorter than one segment.  Note that there
are two DS operations here: reading the old data, and replacing it
with the new data.  They must be done without intervening operations
on the same segment.  The checksum change request is processed by the
checksum DS, by reading the original checksum segment from the file,
XOR the change into it, and writing the result back to the
file[^OPT-SUM].  Again, the two operations to read the old checksum
and replace it with new checksum must be done without intervening
operations on the same (checksum) segment.

During degraded operation when a DS is determined to have failed, the
checksum segment owner will help to handle file read and write.  For
details, see the section about "CPFS error handling and recovery".

[^OPT-SMALL]: If the file is a short one containing only one segment,
we can optimize the flow by simply sending the file content rather
than the change (so that the checksum server can save a file read).
For this to work, all DS must know whether it is a short file, and the
checksum server should report failure if file content (rather than
change) is received but the file is not short.

[^OPT-SUM]: If an FC writes a whole segment group, we can optimize the
operation by letting the FC computing the checksum.  Then all the read
operations are unnecessary.  For this to work, we must be completely
sure that no DS will process another write request on the same
segment group.

A small amount of meta-data is stored in the DS files.  In particular,
each of these files keeps partial information of the mtime and file
size of the inode.  The information is updated alongside with any
write, checksum update or truncate operation, as described in the
section about MS.

Before moving on to the next section, we note that there are some
interesting interaction between the file truncate operation (in FUSE,
the setattr() method) and data write performed in parallel.  In our
implementation, the truncate is done by having the MS sending truncate
Fims to all DS storing the file.  If this Fim is received in the data
DS after the data write, but before the checksum update, the checksum
update could write to somewhere that we have just truncated.  This can
lead to data corruption when the file is extended later.

To prevent this, the MS sends a "DS lock Fim" to the DS involved,
specifying the affected inode.  The DS replies such Fim only when all
completed writes in the DS modifying the inode has the checksum update
Fim replied.  After the lock Fim is received, the DS suspends
servicing further write requests to that inode until it is unlocked.
The MS will perform a truncate only when all DS has been locked, and
unlock it only after the truncate operation is completed, effectively
making it impossible to have truncate and data write operation in
parallel.

## Cache coherency ##

FCs use the kernel page cache to improve file read performance.  This
is implemented by (1) not setting the FUSE `direct_io` mount option
and `drop_cache` open flags, and (2) setting a high entry parameter
and inode timeout value.  Upon data changes, invalidation FIMs are
sent to FCs so that they can invalidate their caches.

When an FC gets the attributes of a file, it makes a request to the
MS.  The MS maintains a record that the file might be cached in the
FC.  This record is kept until the MS sends the invalidation FIM to
the FC (or until some other events due to system errors).  A request
is sent to MS if a read is performed and the client cache has been
invalidated, so that invalidation FIMs are sent to the FC again.

When an FC sends write requests of a file to the MS, it also updates
the MS with the minimum file size and the new mtime.  Like the MS, the
DS also maintains records about which FCs might be caching the file
data.  Upon write requests, invalidation FIMs are sent to these FCs.

The above does not provide even the open-close consistency guarantee:
in the unlikely situation that the invalidations are delayed, an open
call occurring after close() completes may still read old data.
Eventually we plan to provide configuration options so that certain
system calls (e.g., close, flush, flock, etc) will not return until
all invalidation FIMs caused on the file are acknowledged, so that
such guarantees can be provided.

Although it might be beneficial to FS performance, we do not plan to
support write-back cache (i.e., write() returns before the data is
written to even the DS storing it) in the initial version.  The
biggest complication is that if two clients write to the same file,
the first writer will cause the second writer to receive an inode
invalidation request, after write() returns.  At this point, if the
second writer needs to read() the file, it must be careful to wait
until its own write() completes, before it issues a request to reread
the file.  Otherwise, it is possible for the result of a write to
temporarily disappear from the writer, which will confuse
applications.
