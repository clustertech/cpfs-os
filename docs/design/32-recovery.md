# CPFS error handling and recovery #

We aim to design CPFS so that, when configured with two MSs, it can
handle the loss of a MS, or a DS for each DS group, minimizing the
disturbance to FCs.  When running with only one MS, MS failures are
more visible from FCs.  This section describes our plan.

## Terminology ##

The command line options of all MSs, DSs and FCs can specify up to two
MSs.  The first of them is called the *primary* MS, and the second is
called the *secondary* MS.  The configuration must be consistent among
the whole system.  If there are two, at any time only one of them is
*active*, accepting connections and handling Fims from the rest of the
system.  The other MS is *standby*, which only makes a single
connection to the active MS, processing *replication Fims* from it.
When a MS gains connection to another MS and has determined that it
serves the standby MS, it *resynchronize* itself with the active MS,
i.e., ensure that the two have exactly the same data in the data
directory.  When a DS and a FC initially connects to the active MS, a
connection is accepted, and a DS group ID / role and a client ID is
allocated; if they connects to the standby the connection is rejected.

When a standby MS / checksum DS is available, data modification
requests sent by FCs are replied twice.  The first reply is called the
*initial reply*, sent at an appropriate time notifying whether the
operation is successfully performed, and if so, sending the data
requested if any.  In case the reply is positive, there is a second
reply called the *final reply*, sent only when the replication is
completed, so the FC can forget about the request.

If the active MS is found to have failed, a *failover* occurs. the standby
MS switches to transient active state, start accepting connections from
DSs and FCs (both new FC and FCs that have previously been connected to
the failed MS). Reconnected FC should give the same ID to the DS / FC.
The reconnected FCs *reconfirm* to the standby MS the Fims which has been
given initial reply but not final reply by the failed MS. Such Fims are called
*reconfirmation Fims*, and are guaranteed to succeed in normal operations.
It also reestablishies states stored in the MS, e.g., file open state,
cache state and file lock state.  Once all reconfirmation Fims are handled
and state are reestablished, the standby MS switches to an active MS, and
full filesystem service is restored.  At this point the FCs resend the
requests that they have sent to the old active but did not receive even the
initial reply.

Each DS is in one *DS group*, and each DS group has 5 DSs.  Each
request to a DS is replicated to another DS, using RAID-5-like
checksum techniques.  Initially, the DS group is only considered
*active* when all 5 DSs are working.  Once started, one of the DS in
each DS group may fail.  When the MS detects such scenarios, all peers
are notified that the DS group is *degraded* if only one DS of the
group is currently failed, and is *failed* if more servers have
failed.

## General strategy ##

  * We consider only host failures.  Disk failures are not considered,
    so some RAID system should be used in the local storage of MSs and
    DSs to reduce the chance of such scenarios.  Link failures are not
    considered.  If the system admin / owner is worried, some sort of
    bonding should be used.
  * Transparent failover is only supported if the new active server
    (previous standby) survives until a new standby is made available
    and is fully resynchronized with the new active server.
  * The active MS is the authoritative source of information about
    which servers are available, about whether the system as a whole
    is active or pending initialization, whether each DS group is
    active, and if so, whether they are degraded.
  * Initially the system is only activated when all the configured MSs
    and DSs are running.  After the system is activated, if both MS
    failed, or more than one DS failed within the same DS group, the
    system fails, and must be manually restarted.
  * For simplicity of implementation, when the standby MS is
    resynchronizing itself with the active MS, services to FC are
    suspended: only heartbeat Fims are processed, other Fims are
    queued.  In contrast, DS resynchronization is partially online:
    After the directory is resynchronized, service to FCs will resume
    when the DS group is performing data resynchronization.
  * Files are kept in various servers whenever the system is degraded,
    to speed up resynchronization (e.g., to avoid the need to rescan
    all inodes).
  * Failures are detected by the absence of heartbeat Fims to the MS,
    and when that happens the peer is assume to have failed.  This
    results into a topology change on the active MS side, and a MS
    failover on the other side (except on the slave MS, where it
    starts allowing connections from DSs and FCs).  Connections to DS
    does not have heartbeat Fims sent, and we assume that TCP will
    recover all transient errors on these connections.

## MS replication ##

The effect of MS operations depend on the execution ordering of them.
For example,

  * Consider two operations Unlink and Link applied to the same
    directory entries completed successfully in that order.  If the
    operations are reversed, the Link operation probably fails because
    the directory entry is still being occupied.  In our current
    design, the two operations may be concurrently performed by two MS
    worker threads, with their effects determined by a lock on the
    original parent directory.
  * The Rename MS operation has multiple effects which might interfere
    each other.  It removes the source dentry, creates a new target
    dentry, and possibly removes the dentry which sits in the way at
    the target.  In the current design, two Rename operations may run
    concurrently as long as they don't have the same new parent.  To
    give a well defined result, a few locks are acquired during such a
    rename, including the old and new parent directory, as well as the
    inode of the dentry that originally sitting in the way at the
    target.

To ensure consistency between MSs, we use the following strategies
when replicating the effects of successful MS requests.

  * The replication Fim is sent in an ordering fully controlled by the
    active MS.  It is constrained by the "locking order" of the active
    MS: If two threads are both performing an operation affecting the
    same inode, they both would hold a common mutex.  One thread must
    have the mutex locked before the other.  The former would also has
    the replication Fim sent before the latter.
  * When the standby MS receives the replication Fims, it guarantees
    that the result is the same as if the replication Fims are
    processed one by one, in the same ordering as the replication
    Fims.  In this way, we guarantee that the active and the standby
    would eventually reach exactly the same externally visible state.

## Reconfirmation Fims ##

In our HA scheme, FC takes a role in backing up the active MS in
replicating operations: the FC may receive a reply from the active MS
when the latter has not received the reply to the replication, and the
FC is expected to be able to resend the request to the standby MS in
case the active MS fails.  When a MS replies to an FC on any request
(not necessarily doing modification), it commits the standby MS to be
able to handle the request in the same way in case of a failover.
This require careful accounting for failover to be transparent.

To illustrate the issue, consider two requests sent by two different
FCs: a Create request creating a file, and a later Getattr request to
read it.  If the MS replies to the Create request, the FC receiving
the reply would proceed assuming that the file is created, e.g.,
notifying the application about the successful completion.  So if the
MS fails now, we must somehow ensure that the Create operation would
also be completed successfully in the standby MS, and must return the
same data.

Less obviously, if the MS replies to the Getattr request, it commits
to the successful creation of the file.  Consider the following
"obvious" scheme: the MS would wait until the replication is completed
before replying to modifications like the Create request, but replies
to non-modifying requests like the Getattr request are sent
immediately.  It might happen that the MS fails before sending the
replication of the Create request, but have already replied to the
Open request.  If the Create request eventually fails at the standby
MS for some reason (e.g., because another FC resends a Fim which Mkdir
on it), the FC is left in a state where an inode number is obtained
but the file doesn't exist at all from the point of view of the newly
activated MS.  There is no way to deal with this inconsistency without
causing trouble to the FC (e.g., making a subsequent Link to reply
EIO).

To address the issue, we first ensure that operations replicated to
the standby MS are always successful if the FC reconfirms it later:

  * When an FC makes any filesystem request, it carries a request ID
    which is unique across the whole system before the client ID is
    reused.  This is implemented by including the client ID as part of
    the request ID, and having the request ID sufficiently long that
    it never overflows.
  * The replication Fims also carry this message ID.  Upon receiving
    the replication Fim, the standby MS keeps the ID together with its
    result in a *recent reply set* for some time.
  * Upon failover, the reconfirmation Fims sent by the FC contains the
    same ID.  The standby MS uses the recent reply set to avoid
    redoing the requests that has already been completed.  Instead,
    the standby MS simply sends a stored successful reply to the FC.
    The operation is said to *succeed trivially*.
  * Entries in the recent reply set is kept for some configurable time
    normally.  In case of failover, they are kept until some time
    after the standby switches to the active MS.

Now we build a strategy to fully address the issue about the timing of
response.  It is built on top of a data structure in the active MS
called the *replicating inode list*.  Each inode awaiting reply of a
replication Fim has an entry in the list, telling the last replication
Fim[^REPLICATION-FIM] modifying (writing) the inode.

[^REPLICATION-FIM]: Replication Fims sent to the same target has a
total ordering, so the value of a simple 64-bit integer counter
suffices to identify a replication Fim.

 1. When the MS receives a request from an FC, it is processed
    immediately, making mutex locks as needed.
 2. If the operation fails, or if the operation succeeds but no
    standby MS is available (e.g., no secondary MS configured, or one
    MS has failed), the reply is also sent immediately, and the
    processing completes.
 3. Otherwise, if the operation succeeds and a replica is available,
    the replication Fim is sent to the standby MS.
 4. The MS determines the inodes that the operation reads and writes,
    and determines the last replication Fim in the replicating inode
    list for these inodes.
 5. If there is no such replication Fims, the MS replies to the
    request immediately.  Otherwise it arranges the reply to be sent
    once reply to the replication Fim found in the last step is
    received.
 6. The replicating inode list is updated for the inodes written by
    the current operation, and the processing completes.

The rationale to the above scheme is as follows:

 1. When a request is initially replied, we guarantee that the
    operations that establish the inode state have already been
    replicated.
 2. For such requests, before the request is replicated, we guarantee
    that no other requests modifying the same inodes would be
    initially replied.
 3. So in case of a failover, when such a request is reconfirmed, it
    is either found to have been replicated already (and thus succeed
    trivially), or that the inode state is exactly the same as the
    state seen by the failed MS.

One optimization is possible if we add logic to the FC to provide the
"initial reply order guarantee": reconfirmations are sent in the same
order as the initial reply.  In point 4 of the 6 point scheme above,
we can then ignore an inode being read or written if the inode is only
waiting for replications that are triggered by the FC making the
current request, and all requests for these replications have already
been initially replied.  Instead of depending on the guarantee that
the operation establishing the inode state have already been
replicated, we depend on the guarantee that such operations either
have been replicated or would be reconfirm before the current one (due
to the initial reply order guarantee).  This can lead to important
performance improvement when one FC is operating on files in some
directories which are not modified by another FC, since essentially
all replies are sent without any further waits.

Finally, we need to ensure that the inode data in DS is not lost when
the active MS frees it, if the active MS later fails and the standby
MS cannot complete the resent request.  The active MS thus defers
freeing of the inode data upon final unlink of an inode, until the
replication message is received.  In case the active MS fails, the
standby MS takes the responsibility to free all the recently freed DS
inode data.

## MS failover procedures ##

The actual MS failover procedures proceed in phases as follows.

 1. Some part of the system note that the active MS connection is
    lost.  Those requests pending answer by the active MS are
    abandoned.  For requests from FCs, they will be awaiting reconfirm
    or resend, and for requests from DS they will be simply dropped.
    If the DS holds states related to the MS (usually locks), they are
    dropped.  They start trying to connect to the standby MS, although
    the standby MS would reject all such connections, so the
    connection requests need to be repeated until the next phase
    occurs.
 2. The standby MS also note that the active MS connection is lost.
    At this point it starts accepting connections from the rest of the
    system. The DSs and FCs will start getting new MS connections.
    Care must be taken to ensure that the connection cannot be used
    normally.  E.g., at this point, if FC receives a filesystem request
    that needs the MS, no Fim should be sent yet.  Connections to /
    from MS are not performed at this moment, to avoid interfering
    with the failover procedures.
 3. The standby MS determined that all servers and clients previously
    connecting to the active MS have connected to it, and sends a Fim
    to each connected server and client about system ready. The connection
    from DS to MS is now fully activated.  The FC sends reconfirmation Fims
    to MS for requests that has been initially replied, which are
    processed by the standby MS using the using the recent reply set
    if possible.  Replies from MS, if any, will not be processed by
    the FC other than to log a message in case of errors.
 4. The standby MS determined that the system is quiescent, i.e., no
    new reconfirmation Fims are received.  Recent requests to free
    inode data are sent to the DS, and then a Fim is sent to each
    connected server and client about the activation of the MS.  The
    standby MS becomes the active MS at this point.  The FCs respond
    by resending all Fims that has no initial reply received, and
    arrange for the connection to the MS to fully operate.
    Connections to / from MS are allowed again, which would allow the
    system to regain full HA status.

## Degraded mode ##

When a DS group switches to the degraded mode, a failover is
initiated.  We do not need the complex procedures in MS, since DS
operations are generally idempotent (can be repeated with no harmful
effect) and will not fail normally[^DS-HAZARD].  We only need to make
the FCs resend Fims that has not been fully replied, to the original
checksum server.  After switching to the degraded mode, the processing
of future operations are adjusted to accommodate the loss of the DS:

[^DS-HAZARD]: It is possible that, if two FCs are writing to the same
inode, the result read by a FC is inconsistent with the actual
ordering taken by the DSs.  Such cases are rare enough (few
applications would allow two FCs writing to the same file at around
the same time, as the result is easily undefined) that we accept the
possibility of returning garbled data in the also rare case of DS
group degradation.

  * For write operations of segments which checksum DS is the failed
    DS, the checksum diff sending is skipped.
  * For read / write operations of segments normally stored in the
    failed DS, the requests are sent to the checksum DS instead.
  * The segment may be stored in the checksum DS (in a cache) already,
    or it may need to be recovered using the checksum.
      * To perform the recovery, the checksum DS sends a data recovery
        request Fim to all the other remaining DS.  The Fim triggering
        is operation is deferred, as is any further operation needing
        the same data in the checksum DS.
      * The DS receiving such a Fim send the segment content in a
        recovery data Fim.
      * The checksum DS waits until all the remaining DS replies to
        recover the lost segment.  Meanwhile, once the recovery data
        Fim is received from a DS, processing of further checksum Fims
        of the same DS for the same segment is
        deferred.[^DEGRADED-CHECKSUM]
      * Once all the remaining DS sends their recovery data, the
        checksum server uses its own checksum to regenerate the data
        lost, put it into a cache, and re-run all the deferred
        operations for the segment.
  * The recovered segments are cached in memory using LRU eviction, so
    that repeated access would not require multiple recovery.


[^DEGRADED-CHECKSUM]: Here is the rationale behind deferring the
checksum updates: we want to recover the lost data using the recovery
data Fims received, and to do that, the checksum server needs to have
the checksum data including all checksum updates up to the recovery
data, and excluding all checksum changes for writes that happens after
the recovery data is read.  Since Fims are sent to the checksum
servers in the same order as they are first created, we know that the
two types of checksum updates can be differentiated by checking
whether the Fim is received before or after the recovery data Fim.  By
not processing the checksum update Fims of the latter type, the
checksum data needed can be found by reading the current checksum
block content at the time when the last recovery data Fim is received.

## Procedures in case of DS loss ##

Like MS failover, the switching of a DSG to degraded mode upon DS loss
proceed in phases.  They are described as follows.

 1. Some DS and FC found that the connection to the DS is lost.  Those
    requests pending answer are handled.  Requests from FCs will wait
    for resend.  Pending checksum change requests from other DSs are
    treated as completed and are full-replied.
 2. The MS found that the connection to the DS is lost.  Pending lock
    requests will be dropped.  It then sends a topology change Fim to
    all servers and clients asking them to drop their connection to
    that DS, and a state change Fim to each DS in the same DSG
    declaring that the DS is lost.
 3. Once a DS (a "remaining DS") receives the state change Fim, it
    changes its internal state to accept Fims from FC that would
    originally be processed by the failed DS, and discard checksum
    change Fims that would otherwise be processed by the failed DS.
    At this point the DS starts degraded mode operations.  The DS then
    acknowledges their reception of the topology change Fim to the MS.
 4. When the MS receives acknowledgements from all remaining DSs, it
    sends state change Fims to all FCs to tell them that the DS is
    lost.
 5. Once a FC receives this state change Fim, it resends the requests
    originally heading the failed DS to the checksum DS instead.  The
    internal state is changed so that future requests are handled
    similarly.

## Resynchronization ##

When a replacement MS / DS is started or restarted, it is started in
an inactive state.  A simple stop-the-world strategy is used to
rebuild the restarted or new MS:

  * The active MS is notified, so that it can coordinate the whole
    cluster to a mode to get the new or restarted MS / DS ready.
  * The active MS stops FC Fim processing by shutting down the
    processors. For rebuilding DS, the active MS sends a DSG state
    change Fim to each DS in the DSG switching them to a recovering
    state, so that they refrain from processing any FC initiated Fim
    except Heartbeats.

Such simple strategy is used because we feel that the need of
replacing servers is rare, so correctness and ease of implementation
are more important than user experience.

To rebuild the restarted or new DS, the stop the world strategy is
also used up to the point where all the DS in the DS group knows which
files need resynchronization.  After that, the restarted / new DS
coordinates phases of data resynchronization, each reconstructing a
number of inodes (32 by default, can be modified by command line
option).  During such a phase, FC requests to these inodes are
deferred, but requests to other inodes proceed as if the DS is still
degraded (in case resynchronization is already completed, the FC is
requested to resend the request to the target DS).

### Rebuilding MS ###

Once active MS processors are stopped, and all DSs have
acknowledged the DSG state change Fim (for DS resync), the target
MS is populated.

  * The active MS sends a stream of Fims about the content of the data
    directory of the active MS, opened / pending unlink inode
    information and topology information of FC and DS.
  * Once all resync Fims are sent, the active MS sends an "end" Fim to
    the target MS and waits for reply.
  * The MS is set to be standby, and the FC Fim processors in the
    active MS are restarted.

When a MS is restarted sufficiently quickly, the data in it are still
mostly valid.  So it is possible to optimize the resynchronization by
sending only inodes modified after the failover. In particular, we
need to resynchronize an inode of a previously failed server only if
(a) it is modified before the failure but the replication reply is not
yet received, or (b) it is modified or (c) deleted in the currently
active server.

It is too hard to check for (a) efficiently, so instead we check
whether the file is very new compared to a time when the server is
known to be in a good state.  Active and standby MS record the time
last Fim or Heartbeat received from peer MS, as the peer last seen
time, while all DSs record the time when the DSG is in the ready
state.  The restarted server removes inodes with modification time
closes to the these time, and resync them later.

The processing for the MS is as follows:

  * The Active MS uses the peer last seen time as the last
    modification time of the peer meta directory, scans its meta
    directory and sends only inodes with modification time after that
    time point.  In order to properly handle both case (a) and (b),
    the time point is moved backward a bit to compensate for
    replication / disk write delay and heartbeat loss.
  * When failover is started, the active MS starts persisting inodes
    removed.  During resynchronization, these inodes removed will be
    sent to the target MS and be removed accordingly. This handles
    case (c).

### Rebuilding DS ###

To allow DS data resynchronization to be carried out when FC requests
are still being serviced, DS resynchronization is more involved.  We
need to ensure that the inodes being resynchronized are properly
deferred before such resynchronization can start.  We also need to
ensure that there is no data truncation request from the MS during the
whole period of time.

To initialize, the following processing is done, during which the MS
and all DSs will defer requests from FCs:

  * The active MS announces the DS to be in resync state.  The MS and
    all DSs move to a state where all FC requests are deferred.
  * Once all DSs acknowledge the state change, the MS sends a message
    to all DSs to start the resynchronization.
  * Each non-target DS generates a stream of DS directory Fims
    containing recently updated inodes that it stores.  If the target
    DS is new and contains no data, the non-target DSs simply send all
    inode numbers they keep.
  * The non-target DSs send a stream of recently removed DS, and upon
    receiving them the target DS deletes the involved inodes.
  * The target DS checks its own directory to find the inodes that are
    recent, and remove them.  This list is combined with the updated
    inode lists sent by the peers to from the consolidated resync
    inode list.
  * The non-target DSs requests for the consolidated resync inode list
    with multiple Fims, until the whole list is obtained.
  * The target DS sends a message to the MS to signify the completion
    of partial resynchronization.  The MS responds by restarting the
    FC Fim processors, thus allowing FC requests to be handled again.

Like MS resynchronization, the non-target DSs keep lists of "recently
modified" and "removed" inodes in degrade mode, so that finding such
lists upon DS restart involves no scanning.  In such cases, these
steps are expected to be fast.  Once completed, phases of data
resynchronization starts:

  * The non-target DS requests for a list of inodes to resynchronize
    in a new phase, which also signifies the end of data for any
    previous phase.
  * Once the target DS receives such requests from all other DSs, it
    performs the following steps:
      * It completes all pending file regeneration in previous phase.
      * It extracts the leading inodes from the consolidated resync
        inode list to form the resyncing inode list.
      * It replies the inode list request of the non-target DSs with
        the resyncing inode list.
      * It sends a request to the MS to prepare for the resyncing
        inode list to be resynchronized.
  * When the non-target DS receives the resyncing inode list, they set
    it to their own "deferment" data structures, requesting that FC
    requests to these inodes to be deferred while allowing FC requests
    for other inodes to be serviced.  A "phase ready request" is sent
    once the pending checksum updates of inodes in the resyncing inode
    list are all completed.
  * The MS receiving the request does the following:
      * It sets the inode list to a data structure which defers the MS
        to issue DS truncate requests for these inodes.
      * It replicates the requests to the standby MS, so that in case
        an MS failover occurs at this point the new MS also knows that
        the inodes are being resynchronized.  It then waits for the reply
        of this replication before continuing.
      * It waits until the already started truncations of those inodes
        completes, and replies the request.
  * Once the target DS received the ready requests from all other DSs,
    and received the reply from the MS about resynchronization
    preparation, it performs the following steps:
      * It replies all other DSs, allowing them to start sending the
        resynchronization data.
      * It sets the resyncing inode list to its own deferment
        structure, so that FC requests to other inodes are serviced.
  * Once the non-target DS receives the reply, they start sending the
    target DS a stream of Fims containing:
      * File information such as mtime and file size.
      * Inode-offset tagged file content of the inodes, in a
        well-defined ordering.
  * The target DS uses these Fims to regenerate the file mtime and
    date, compute the checksum and rebuild the file content.
  * Once a non-target DS completes sending file content for the
    resyncing inode list of the current phase, it loops back and
    requests for a new resyncing inode list.
  * A new phase starts once the target DS receives such requests from
    all other DSs.

Once all inodes in the consolidated resync inode list are processed,
the target DS sends to the MS an inode list with no inode, signifying
the end of data resync.  A similar no-inode list is replied to the DS
to terminate their loops sending resynchronization data.  The MS sets
the DSG state to ready, and broadcast the information to the DSG and
the standby MS.

## Disk full ##

We make the following assumptions about disk full conditions:

  * It can only happen to DS, not MS.
  * If the local filesystem of DS becomes full, the failure is
    reported at the time when `pwrite()` cannot be completed, not
    later.
  * A disk full condition cannot happen when `pwrite()` overwrites
    pre-existing data.

One limitation of the Unix filesystem interface is that there is no
way to punch a hole in an existing file.  Furthermore, nothing
guarantees that `pwrite()` is atomic.  As a result, once a `pwrite()`
fails due to disk allocation problem, we cannot deallocate the space
the operation allocated.  Nevertheless, we can overwrite the space
allocated with previous data without cost to the no-error case.  This
is because we need to read the data for checksum computation anyway,
we just need to keep that data long enough.  This is exactly how the
DS services a write or checksum update request.

On the other hand, there are still complications when checksum update
fails.  If we don't also revert the data of the corresponding write
operation on the DS storing the actual data, the checksum is now
incorrect.  But the DS would have already considered the write
operation as completed, and thus might already have accepted
additional writes to the same file location (and, because the checksum
disk might have some file removed, the corresponding checksum update
might succeed).  This makes it very hard to revert the changes.

We take a simplistic approach to handle the issue.  When the MS, due
to space maintenance, determines that it is likely that the DS will
become full soon, it asks the DS to operate in distressed mode.  In
this mode, all writes of all DS inodes are serialized: they wait until
the success or failure of the checksum update to be known, before
allowing the next operation on the same inode to proceed.  This is
a rather slow method, but protects the filesystem against corruption.

We also note that we use the NFS approach to report disk full errors:
it is reported on the next `write()` or `close()` system call.  Fsync,
close, getattr and setattr operations wait until all previous write
operations to have replicated before proceeding.

It can also happen that the disk full condition occurs when a failed
DS is being recovered.  In this case the error is silently ignore at
the moment (later we will probably change the behavior so that the
condition is detected and cause the resynchronization to abort).

## Full Shutdown ##

We provide a way to gracefully shutdown the CPFS:

  * The shutdown process is initiated when active MS received SIGUSR1. The
    SIGUSR1 is chosen instead of SIGTERM / SIGINT in order to distinguish
    user initiated CPFS shutdown from system shutdown.

  * Active MS broadcasts a "shutdown" request to all connected MS, FCs
    and DSs. Both Active and Standby MS will change states to "shutting down"
    and stop processing FC initiated Fims.

  * When FC received the "shutdown" request, it stops sending Fims to MS
    and DSs, while replies from MS or DS will still be processed. This ensures
    MS and DS could finish Fim processing as soon as possible.

  * When DS received the "shutdown" request, it waits until queued requests
    are processed. After that, it changes its state to "shutting down" and
    acknowledges MS about this.

  * When all DS from all DS groups have acknowledged active MS about the
    shutdown, this means DS group data are consistent and shutdown is ready.
    Active MS sends "halt" requests to all MS, FCs and DSs connected.

  * When FC received the "halt" request, it terminates itself by raising
    SIGTERM to trigger FUSE cleanup.

  * When Standby MS or DS received the "halt" request , it terminates itself
    by stopping the IO event loop.

  * Once Active MS determined that the "halt" requests are sent to all
    connected peers successfully, and connection to standby MS is lost, it
    will terminate itself by stopping the IO event loop. This ensures Active MS
    is the last component to shutdown.

The "shutting down" state has a timeout, so that during "shutting down" state,
if the MS, FC or DSs cannot receive "halt" request within time limit, they
will terminate itself, regardless of whether there is unprocessed fims.
