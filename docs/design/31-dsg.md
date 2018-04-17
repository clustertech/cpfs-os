# Multiple Data Server Groups #

CPFS supports multiple data server groups (DSGs) by design.  The
following sections describe our strategies in this support.

## Creating DSGs

Upon the first start of CPFS, the total number of DS group(s) defaults
to 1, which is reflected as a configuration value of CPFS.  This value
can then be queried and updated using the CLI.  Setting this
configuration value changes the number of DS groups.  Upon start-up
and update, the value is persisted to the data directory.

At first, a DS group is in the state "Creating", meaning that the
group has not been fully connected before, and is waiting for servers
to connect.  Once servers within the group are connected, the group
will become "Ready" and be marked as `Created` in the persistent
storage of both master and slave meta servers.  The group will then be
available for allocation to new files (see the following for DSG
selection algorithm).

Upon whole system restart, groups created before will be in "Pending"
state while waiting for server connections.  At any time, CPFS
operates when all `Created` groups become "Ready" or "Degraded", while
allowing the last group to be still in the "Creating" state.  CPFS
will be blocked when there is "Pending" group.

The command line interface supports removing DS group in the
"Creating" state, by dropping the group states and DS connections,
with the change propagated to the slave meta server.

## Allocating DSGs to files ##

A file may span multiple DSGs.  The DSGs to use is determined at file
creation time, and will not change thereafter.

The number of DSGs to use is determined by an extended attribute
`user.CPFS.ndsg` set to the parent directory of the file (or the
current number of DSGs of the cluster).  If the attribute is not set
or cannot be interpreted, 1 DSG is allocated.  Because normally the
attribute is left unset, we normally create files which span only one
DSG.  When a new directory is created, this extended attribute is
inherited from the parent.  The user extended attributes space is
exploited for the purpose, so that it is easy for stock utilities to
store and manipulate the attribute.

## DSG Selection Algorithm ##

Files are allocated DSGs more or less randomly, so that each DSG tends
to have similar loading.  However, since some files can be radically
larger than others, it is possible that a DSG gets much higher loading
than another DSG.  In such situations, when allocating DSGs to new
files we prefer DSGs that are less loaded, so that eventually balance
is restored.  The algorithm is described as follows.

In simple words, we try to allocate DSGs in such a way that the
probability of a DSG to be used is proportional to square of its
remaining space, defined to be the minimum remaining space over DSs in
the DSG.  The squaring is used so that over time the amount of space
remaining in each DSG will tend to balance (without squaring, the
proportion of remaining space tends to remain the same over time).
The remaining space is counted in bytes, not in percentage of storage
used, because the number of bytes used by the new file will be the
same whichever DSG it gets allocated.

While the idea is simple, the algorithm still needs to be carefully
designed to cater for complications when allocating multiple DSGs.

 1. Determine the number of DSGs, `n`, to allocate.
 2. Obtain free space statistics of DSGs to determine the minimum
    remaining space `S[i]` for each DSG `i`.  This should be cached in
    the server already, rather than fetched at file creation time.
 3. Calculate the unnormalized probability of each DSG to get used,
    `P[i] = S[i] * S[i]`.
 4. Sort the list `L` containing pairs `(i, P[i])` in descending
    `P[i]`.  This sorting is needed to ensure that `n` DSGs are chosen
    whenever possible.
 5. Do the following until `L` becomes empty or `n` becomes 0:
     1. Normalize `P[i]` in `L` to sum to `n`.
     2. Pop the first element `(i, P[i])` out of L.
     3. If `P[i]` is 1 or above, pick DSG `i`.  Otherwise, pick DSG
        `i` at probability `P[i]`.
     4. Decrement `n`.
 6. Shuffle the DSGs that we have chosen and allocate it to the file.
