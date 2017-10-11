# Defer management #

## Introduction ##

A lot of the DS worker code is to ensure that the FIM processing will
not be performed at a time when it is inconvenient to do so.  Because
the DS workers need to work asynchronously, processing of FIMs that
cannot be processed immediately must be put aside in some data
structure.  This document explains the strategies used.  The same
strategy may be used in MS in case asynchronous operations are
desirable.

## Why defer? ##

There are a few reasons that a FIM needs to be deferred.  They include:

  * DS resynchronization ("all-defer").  During DS resynchronization,
    the processing of all FC-generated FIMs are deferred.
  * Inode truncation ("inode-defer").  Inode truncation cannot be done
    at the same time when a write is being done, otherwise chaos ensue.
    So the MS performs an inode truncation only after the DS are be
    instructed to temporarily defer writes to that inode.
  * Distress mode, i.e., low disk space ("distress-defer").  When disk
    space runs low in at least one DS of the DSG, there is a danger
    that a write results in ENOSPC.  To make write roll-back possible
    in such cases, requests to the same segment are done
    synchronously.  When one request is being performed, other
    requests to the same segment are deferred.

The above are listed in the order in which FIMs are checked against
these deferments, which are explained below in more details.

## Defer ordering ##

Because there are multiple reasons to defer FIMs, it is possible that
when one of these deferments is lifted, another takes its place to
defer the same operation.  The operation must thus be migrated from
the data structure waiting for one reason to the data structure
waiting for another reason.

However, the DS needs to provide an illusion that everything is going
on "as usual".  In particular, the following must happen all the time:

  * If a DS receives two writes to the same location of the same file
    in a sequence (say write A and then write B), then after both
    operations succeed the data in the file must be that of the later
    write.

This poses requirements on such migrations across data structures.  As
an example, suppose we have two write FIMs to the same file and
location from the same DS: A and then B.  A is deferred because of the
need for inode truncation.  Right before the inode truncation
deferment is lifted, a resynchronization occurs, as well as another
write B.

Upon lifting of the truncation deferment, it finds that the FIM needs
to be deferred again, due to resynchronization.  But the DS must
arrange it to be done before B.

We pose a conceptual ordering in which FIMs enter the data structures:
the FIM is first processed for all-defer, then for inode-defer, and
finally for distress-defer.  In the situations above, the inode-defer
handling of a FIM completes when all-defer is active.  We use two data
structures to handle the case, which we describe below.

## FIM defer manager ##

A per-DS FIM defer manager is used for each type of deferments.  FIMs
queueing in a defer manager is treated as not processed yet.  Because
what is being deferred differs in each case, the data structures also
differ in minor ways.  The data structure supports the following
operations:

  * Queueing a FIM-sender pair to the manager (for some inode, for
    some inode/segment pair).
  * Checking whether the manager contains any FIM-sender pairs.
  * Dequeuing a FIM-sender pair from the manager.
  * Setting an inode/segment to be in a locked state, and unsetting it
    later, i.e., actively queueing FIM-sender pairs.
  * Checking whether an inode/segment is in a locked state.
  * Getting a list of inode/segment in a locked state.

Care must be exercised so that the state of a manager is synchronized
with the state of the FIM manager in some way.  E.g., for the
all-defer situation, we use a reader-writer lock to synchronize the
change of the DSG state of the DS.  During any FIM processing, a read
lock is held, so that the DSG state change can only occur when all FIM
processing is suspended.  During the read lock, the FIM processing
will check whether its own all-defer manager needs to be activated,
and is assured that if it is not, the same state is maintained
throughout the FIM processing.

In contrast, inode-defer is handled completely in the worker.  This is
because only one worker will handle the inode specified by the MS
truncation FIM, unlike the case of all-defer where all workers are
affected.

## FIM defer callback slot ##

In case an earlier deferment is active when a later deferment
completes, we cannot just put the FIM back to the earlier defer
manager: that would violate the ordering constraint mentioned above.
Instead, we put the work needed in a separate queue called the
"callback slot", containing callbacks to be performing at the time
when the FIM deferment is lifted.  This secondary queue makes it
possible for the processing already passed a defer manager to "jump
the queue", arriving at a correct final ordering.

It is also interesting to note that in case two defer managers enqueue
callbacks to the same callback slot, the ordering is still correct in
all cases.  This is because the worker runs in a single thread, so the
deferments are lifted one by one.

## Replying to triggering messages ##

The all-defer or inode-defer managers are triggered by a FIM from MS.
This FIM is replied to tell the MS that the DS is acknowledged about
the deferment.  This is safe only if the pending operations of the DSs
operating on the affected inodes are all fully replicated.  The inode
completion checker is used for the purpose.  This is done per-DS: each
DS checks that its own operations are all completed, and once the MS
receives the reply from all DS, it can conclude that the DSG is in a
quiescent state for those inodes.

## Synchronous operations ##

The final type of deferment, distress-defer, is of particular
interest.  When activated, FIMs must wait for the currently
replicating FIM to complete.  When this happens, the queueing FIMs
should be processed one-by-one, until a FIM requires write
replication.  Then further processing is deferred until the completion
of this replication.

It is notable that when activated, both read and write need to be
deferred.  This is because we use a write-through strategy when
dealing with writes (in the low-level client library): the write is
treated as performed, and the write request is at the same time sent
to the DS.  When a later read comes, we also send the read request to
the DS, but it is up to DS to ensure that this later read will not
jump the queue and be processed before the write is actually
processed.  This necessitates that reads are deferred just like
writes.

On the other hand, truncates are not deferred at the same time, so
they can be reordered with writes.  The client ensures that if it
wants to perform a truncate call, it will wait until replication of
all writes to the same inodes have been completed.  Because it is not
legal for a client to issue a new write to a file when there is an
incomplete truncate call, we do not have an issue for the reordering.
