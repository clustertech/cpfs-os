# Code overview #

This document aims to provide an overview of the CPFS codebase, so
that new comers can have a rough picture of CPFS when they read the
source code.  It also provides a quick reference for reviewing the
full source code architecture.  It does not completely describe or
even mention all source files.  For that, directly read the source.

## Common themes ##

  * Reduce efforts in maintaining the code.  In quite a number of
    places, a little bit of complexity is added so that we do not need
    to repeat code.  This theme extends to the build framework: we
    typically do not need to modify any `Makefile` fragments when we
    create a new source file.
  * The code is close to completely testable.  Most of the reported
    missing coverage from gcovr are false positives.  The only
    untestable code are ones added defensively to guard against rare
    errors.
  * Proper layering of code.  We use a strict structure where the root
    directory provides the basic infrastructure, on top of it the
    `client` directory provides the building blocks for running an FC,
    and the `server` directory provides common facilities for MS and
    DS.  On top of root and `server`, the `server/ms` and `server/ds`
    directories provides the building blocks for running an MS and DS.
    A few modules in the `main` directory does all the integration.
    E.g., there is no dependencies between the `server/ds` and
    `client` directories.
  * Namespaces matches directories.  E.g., definitions in `server/ms`
    are made into the namespace `cpfs::server::ms`.  This allows the
    definitions to access also the `cpfs::server` and `cpfs`
    namespaces.  With the strict layering as stated above, most code
    does not need any namespace specifier.  The exception is the
    integration code in `cpfs::main`, which needs specifying the
    namespaces like `cpfs::server::ms`, abbreviated as `ms` using
    namespace alias.

## Entry points ##

The CPFS server is started from the `main()` function defined in
`main/cpfs_server.cpp`.  Depending on the options provided, it either
creates a MS (meta data server) or a DS (data server) using
`main/server_main.cpp`.  The CPFS client is started from the `main()`
function defined in `main/cpfs_client.cpp`, which creates a FC
(filesystem client) using `client/client_main.cpp`.  Both
`main/server_main.cpp` and `client/client_main.cpp` are integration
classes, with the primary aim to inject all the dependencies into the
server / client objects.  The `main/server_main.cpp` module is also
responsible for argument parsing (so that it knows whether to create a
`MetaServer` or `DataServer`), whereas the same task for the client is
delegated to the client object itself.

In either case, the returned object implements the `IService`
interface (see `service.hpp`).  This interface is used by the `main()`
functions to initialize, run and shutdown the service.

## Server and client integration ##

Many server components need to find other components or details of the
server.  This calls for a server object which the components can
access.  On the other hand, such server objects would require
integrating many components of the system, and as a result would
depend on all modules of the system.  It is undesirable for such
object to be exposed to each component, causing all components to also
depend on all modules of the system.  Apart from that, to be able to
unit test the object, we must be able to call the method of every
class we write, including the `Run()` method of `IService`, without
triggering the full system.

Therefore, we use the following scheme for servers.  A server object
is defined in `server/base_server.hpp`, and is refined in
`server/ms/base_ms.hpp` and `server/ds/base_ds.hpp` for MS and DS
respectively.  They provide getter and setter for each component used
by the server object in a base class (used for unit testing).  They
are further extended to actual production classes to perform
initialization actions, which can be tested using mock dependencies.
Finally, the `main/server_main.cpp` module depends on all the needed
modules.  It instantiates the needed server class template, and
injects all the needed components into the server objects, so that it
is ready to be used by `main/cpfs_server.cpp` to run the service.
The classes in `main/server_main.cpp` carefully avoid defining a
`Run()` method, so that no such method needs to be tested in the unit
test (which would otherwise triggers the use of the objects in many
other modules).

A similar but simpler scheme is used by the clients.  A client object
is defined in `client/base_client.hpp` to provide the getter and
setter, and the `Run()` method (for testing with mock dependencies).
Then `client/client_main.cpp` creates a `Client` object, injected all
dependency into the object, for use by `main/cpfs_client.cpp`.

## Common facilities ##

### Common definition headers ###

A few headers serve defines no runnable components, but define types
and structures so that other modules can share their definitions.
They include:

  * `defs.hpp`: Contain some variables affecting the behavior of
    external headers included.  Every C++ source file (`.cpp` files)
    implicitly #include this, as indicated in the Makefile.
  * `common.hpp`: Contain most base filesystem abstractions like Inode
    number, Dentry cookie, client number, group ID and roles, request
    IDs, Fim types, configuration, defaults, etc.  Also document all
    namespaces used.
  * `asio_common.hpp`: Contain type aliases for commonly used types in
    the Boost.Asio framework.  Usually, unit tests do not want to call
    Asio, and instead use mocked calls.  The real types are still
    used, but the functions operating on them is encapsulated using a
    asio policy class.  The real functions are defined in `AsioPolicy`
    (see `asio_policy.hpp`), while the unit tests use
    `MockIAsioPolicy` (see `asio_policy_mock.hpp`).

## Unit testing and Mocking ##

All modules are unit tested using the `googletest` framework.  Test
files has the same name as the file being tested, with a `_test`
suffix before the file extension.  The `gcovr` tool, based on `gcov`,
is used to check code coverage.  Unluckily, `gcov` can produce false
positives (which is typically 1% to 2% of lines which generate code).
Most of them are filtered by the `show_gcovr_missing` script, or
avoided by some simple idioms, so as to minimize the manual efforts
needed to interpret the results.

To enable full coverage, we use mocking as defined in the `googlemock`
framework.  Typically, classes are written to implement a fully
abstract interface, and mock classes are created to implement the
interface.  Code uses the classes only via the interface, so that if
we want to use the mock, we can somehow inject the mock class to the
user.  The mock classes are typically created using the facilities
defined in `mock_helper.hpp`, which avoids the need to count number of
arguments.  The definitions needed can usually be generated using the
`mock_gen` script.

We normally inject dependencies using one of the following ways:

 1. Classical constructor or setter injection: a constructor or setter
    of the object being tested takes a pointer to the dependent class,
    which can either be the real or a mock object.
 2. Server / client object injection: the constructor of the object
    takes a server or client object, where the dependencies are
    injected via setters there.
 3. Maker injection: at times the class being tested creates the
    object that needs to be mocked.  In such cases, the object uses a
    maker to create the object.  The maker usually is just a
    `boost::function` that creates the object.  In real runs a maker
    that creates the real dependency is given to the object (usually
    done using `boost::factory`); in test runs a maker that creates
    the mock object is given instead.

The testing usually needs to perform some actions that are not
typically done in the production builds, e.g., creating a temporary
directory to be removed later, building a program argument list using
a canned list, or sleeping for some time.  Such actions are defined in
`mock_actions.hpp`.

## Logging ##

The `logger.hpp` module provides facilities to easily perform logging
with Pantheios.  Logging messages are categorized, and severity level
of messages to show can be set per-category.  In general, the severity
level is set as follows:

  * *debug* (7): For messages useful during debugging, but is not of
    interest to system admins during normal operations.  Enabling the
    option can lead to excessively large amount of messages to be
    logged to seriously degrade system performance.  It is usually
    desirable to select only a certain subset of log categories to
    have debug log output.
  * *informational* (6): For messages useful during debugging, but is
    not of interest to system admins during normal operations.  The
    number of such messages is small though, so it is safe to enable
    anyway if the extra messages do not bother the admins.
  * *notice* (5): For messages of interest to system admins, but are
    not an indication of malfunctioning of the system.  This is the
    default logging level.
  * *warning* (4): For messages that indicate system malfunctioning,
    but the system can continue to work.  This usually indicates
    configuration problems.
  * *error* (3): For messages that indicate system errors.  The system
    may continue but is unlikely to work fully.
  * The *critical* (2), *alert* (1) and *emergency* (0) levels defined
    by Pantheios are currently unused.

The `logger.hpp` module also defines available logging categories.

To ease unit testing, the `log_testlib.hpp` modules are used in tests.
With this, the log messages are "logged" by calling a custom object
method, which can be tested using normal mocking techniques.

## Utility modules ##

A number of modules provide some commonly used utilities.

  * `io_service_runner.hpp`: allows a thread to be created for running
    each of a number of Boost.Asio I/O services.
  * `mutex_util.hpp`: allows long Boost.Thread mutex locking to be
    logged.
  * `intrusive_util.hpp`: provide classes to help usage of the
    `boost::intrusive_ptr` class template.
  * `util.hpp`: provides a number of commonly used facilities like IP
    to int conversions.
  * `periodic_timer.hpp`: provides a class `IPeriodicTimer` for
    deferred execution of code using Boost.Asio.  In the code,
    `IPeriodicTimer` is always used with a Boost `shared_ptr`.
  * `time_keeper.hpp`: provides the API `ITimeKeeper` to record the
    last time when some event occurred.  It has provisions for clock
    moving backwards.
  * `timed_list.hpp`: provides the template `TimedList` for easily
    creating lists with elements that can be requested to expire after
    certain age.
  * `dir_iterator.hpp`: provides the API `IDirIterator` so that the
    other code does not need to use the complex `readdir_r` system
    call directly.
  * `shutdown_mgr.hpp`: provides the base `IShutdownMgr` for proper
    shutdown of the system.

## Communication ##

### Connection initialization ###

As a distributed system, the servers and clients need to have network
connections to each other.  So the server and client objects need to
have components that passively listen to connections or actively make
connections.  Both of these are done using Boost.Asio.

The base server include a listening socket (see
`listening_socket.hpp`) to listen to connections.  (Clients do not
listen to connections.)  It also defines handler methods that are run
upon connections.  The listening socket registers itself with the Asio
I/O service object, so that the actual listening is done
asynchronously by the thread running the main `IOService` object of
the server.  The listening socket would generate an Boost.Asio
`TcpSocket`, which is passed to the handler upon connection to make a
full `FimSocket` (see the section about them below).

Except the primary MS, all clients and servers need to make
connections.  They do it with a "ConnMgr" class.  Such classes uses
the connector (see `connector.hpp`) of the server or client to connect
to the desired server and create a `FimSocket` out of the resulting
connection.  The connector uses a `ConnectingSocket` to actually
create the Asio socket required and register the connection request
with Asio.

### Fim objects ###

Servers and clients communicate among each other by exchanging FIMs
(Filesystem Information Messages).  Each FIM is represented in the
memory as a Fim object (see `fim.hpp`).  The `IFim` interface can be
used to access a Fim of all different types.

Different FIMs differ in the number and type of fields.  To allow them
to be easily accessed, different FIM types are represented by
different Fim object types implementing the `IFim` interface.  They
are defined in `fims.hpp`.  Each type is defined by a "FimPart"
followed by a macro determining the type code to store in the FIM, and
the compile-time flags determining how such Fim objects are handled.

Many FimParts share significant portions of their content.  The
`finfo.hpp` header defines such portions as a structure.  Such
structures usually need special concern of alignment, as is the
FimPart themselves.

Given some raw memory storing a complete FIM or its header, we can
create a Fim object using the global `FimMaker` (see `fim_maker.hpp`).
We put a hook in the Fim-defining macro of `fims.hpp`, allowing code
in `fim_maker.hpp` to generate code fragment to handle each Fim type
as the Fim type is defined.

In the code, a Fim object is always used with a Boost `intrusive_ptr`.
The normal `shared_ptr` is not used, since such objects are
sufficiently short-lived and frequently need to make the difference in
cost significant.

### `FimSocket` ###

The `FimSocket` (see `fim_socket.hpp`) is a thin layer of abstraction
to extract data from an Asio socket to generate `Fim` objects upon
data reception, and to write Fims into the socket for sending.  It
handles the buffering of partially received message, the buffering of
asynchronously queued messages for sending, the generation of
heartbeats and handling of their loss, and the handling of Asio
errors.  It also holds a reference to the `ReqTracker` (see below),
for logging purposes and for Fim processors to easily perform
request-response handling.

In the code, `FimSocket` is always used with a Boost `shared_ptr`.

A `FimSocket` may be associated with a `ReqTracker` and a
`FimProcessor` (see the following sections).  They are useful when
handling Fims received.

### `ReqTracker` and `TrackerMapper` ###

CPFS servers and clients usually need to send requests and wait for
the associated reply.  These are managed by the `ReqTracker` class
(see `req_tracker.hpp`).  When a request is added, the message is
queued to the associated `FimSocket`, and a request entry is returned
allowing one to (optionally) run some callback upon reception of a
reply, and (optionally) wait for the reply.  Proper reference counting
is done: if we don't need a reply, no special action needs to be done
apart from ignoring the returned request entry.

For FCs, the request tracker is also responsible to keep the data
required for resending upon connection loss.

When a server or a client want to make a request, it usually needs to
do so for `ReqTracker` identified by type and number (e.g., the DS
tracker of group `g` and role `r`), rather than having an existing
reference to the needed `ReqTracker`.  To obtain such a reference, the
`TrackerMapper` is used.

Upon the establishment of a connection, the `FimSocket` is associated
with the `ReqTracker` found or created using the `TrackerMapper`.

In the code, `ReqTracker` is always used with a Boost `shared_ptr`.

### Fim processing ###

Some processing can be done upon reception of a Fim.  The processing
required is abstracted with the `IFimProcessor` interface (see
`fim_processor.hpp`).  Different processing is done using different
implementation of the interface, although there are a few commonly
used class of interest.

  * `CompositeFimProcessor` (see `composite_fim_processor.hpp`): A
    processor that process by using a few component processors.  The
    components may either be owned by the composite Fim processor or
    owned externally.
  * `BaseFimProcessor` (see `base_fim_processor.hpp`): A processor
    capable of interpreting replies and putting them to request
    trackers, and ignoring heartbeat Fims.  Other processors are
    usually subclass of this, or a composite processor including a
    base fim processor.
  * `MemberFimProcessor` (see `member_fim_processor.hpp`): A processor
    which handles different type of Fim with different handlers
    registered to the `MemberFimProcessor`.  The handler has the real
    Fim type argument, simplifying the implementation.
  * `ThreadGroup` (see `server/thread_group.hpp`): A processor which
    queue Fims to one of the worker `FimProcessor`s, according to the
    first 8 bytes (supposingly an inode number) of the Fim body.
  * The `Worker` for MS (see `server/ms/worker.hpp`) and DS (see
    `server/ds/worker.hpp`) described in the section about Servers
    below also implements the FimProcessor interface, and are normally
    used by having a thread group delegating Fims to them.

## Client ##

### FUSE interactions ###

CPFS produces a filesystem using FUSE (Filesystem in USEr space),
avoiding direct kernel coding.  The interactions with FUSE is
performed using a `BaseFuseObj` with a `BaseFuseRunner` (see
`fuseobj.hpp`).  The latter uses C++ SFINAE (Substitution Failure Is
Not An Error) techniques to automatically discover methods defined in
the `BaseFuseObj`.  It creates a wrapper method suitable to be used in
the FUSE initialization structure, and perform exception handling as
needed.  The main filesystem code is in a class derived from
`BaseFuseObj` called `CpfsFso` (see `client/cpfs_fso.hpp`).  The
`client/client_main.hpp` module instantiates the class during the
integration process.

The `BaseFuseObj` is compile-time polymorphic for the "FUSE method
policy", so that the unit the need to use the actual FUSE functions.
Instead, the unit test would instantiate the class template with the
`MockFuseMethodPolicyF` (see `fuseobj_mock.hpp`), which forward calls
to the `MockFuseMethodPolicy` class that can be mocked as needed.

### Caching of inodes and entries ###

FUSE allows filesystems to utilize the kernel cache for inode
attributes, file contents and directory entries.  It also allows them
to notify the kernel about asynchronous changes to these information,
which is essentially to a networked filesystem like CPFS.

On the other hand, such notification typically needs to acquire a lock
on the kernel that is also acquired during other filesystem
operations, which can lead to deadlocks if not done properly.  E.g.,
when FUSE call our method to write some data to a file, the inode lock
is acquired.  If we attempt to notify the kernel of invalidation of
the same file, it requires the same lock.  So if the write will wait
for the notification to complete, we end up in a deadlock.

To avoid such situations, the client have a separate thread in the
`CacheMgr` (see `client/client_cache.hpp`) for performing
invalidations asychronously.  When performing a lookup, the client
uses the `CacheInvRecord` structure to ensure that there is no
invalidation done between the initiation of the lookup and the return
of information.  If so, caching would be disabled.

Some further inode information is kept by the client.  In particular,
the client records the files which are currently opened by its
processes, using the `BaseInodeUsage` class (see `inode_usage.hpp`).
The information is communicated to the MS so that it can aggregate the
usage information from all clients, and perform delayed freeing of
inode data accordingly.

## Servers ##

### General structure ###

The MS and the DS are organized in similar ways.  The Fim processing
is done by a composite Fim processor, including a base Fim processor
for reply processing, a control Fim processor for handling control
Fims, and a thread group (also a Fim Processor) for performing inode
operations.

Each thread of the thread group is a `Worker` (see
`server/ms/worker.hpp` and `server/ds/worker.hpp`), which handles the
communication with other servers and clients.  A `BaseWorkerReplier`
(see `server/worker_util.hpp`) is used to send the reply, handling
errors appropriately, and C++ RAII techniques are used to ensure that
C++ exceptions causes error Fims to be replied.

The actual work modifying the server storage is delegated to `Store`
(see `server/ms/store.hpp` and `server/ds/store.hpp`).  The main
objective of the two tier structure is that the `Store` methods can be
called without referencing any communication abstraction, so it is
easier for methods in `Store` to call each other.  This also makes
unit testing easier.

To ease system administration, the same server port is used to handle
connections from different servers and clients.  The
`InitFimProcessor` (see `server/ms/fim_processors.hpp` and
`server/ds/fim_processors.hpp`) differentiate among the possibilities.
Once this is done, the `FimSocket` will be modified to process Fims
using a composite Fim processor.  For most requests, this ends up
putting the Fim to the thread group, which feed the Fims to the
workers.

### Request completion checking ###

At times it is handy to perform some work once we know that all inode
operations already submitted to an inode is already completed.  This
is currently needed when we do DS locking (see the corresponding
section below).  This is done using the `OpCompletionCheckerSet` (see
`op_completion.hpp`), where non-atomic inode modifying operations
register themselves to the checker, so that other code may arrange a
callback to be called when all such operations are completed for an
inode.  A simplified version is used by the client when we make some
operations to provide a consistency guarantee.  In this case, the
client FUSE file handle includes a `ReqCompletionChecker` initiating
actions once operations requested for the file are all completed.

### Miscellaneous utilities ###

  * The servers stores data in a normal directory.  Utilities
    interpreting them can be found in `server/storage.hpp`.
  * The servers have to know whether some clients may be caching
    information for its inodes.  This is done using the
    `CCacheTracker` (see `server/ccache_tracker.hpp`).
  * The replication protocol requires that the servers know whether a
    request has already been processed, and the replies in case they
    are requested again.  The `ReplySet` (see `server/reply_set.hpp`)
    is used to store the replies to recently replicated requests.

## MS ##

### DS groups and inode allocation ###

The MS maintains the topology information of the cluster, which is
synchronized to the standby MS if one exists.  This task is done by
the `TopologyMgr` (see `server/ms/topology.hpp`).  The topology
manager also assigns DS ids to new or rejoining servers, and FC ids to
clients.

The MS is also responsible for allocating inode numbers.  This is done
with `InodeSrc` (see `server/ms/inode_src.hpp`), where the used inode
numbers used are recorded in the MS permanent storage.  Some caching
mechanism is used here to avoid having every file or directory
creation operation to require updating the structure.

When a new plain file is created, one or more DS groups must be
assigned to the file.  This is done by the `DSGAllocator` (see
`server/ms/dsg_alloc.hpp`).

Disk usage of the DSs is an important information that would be useful
for handling disk full and for allocating DSGs to new files.  Such
usage information are fetched by the main communication thread
periodically, using the `StatKeeper` (see
`server/ms/stat_keeper.hpp`).  Other code may query the latest
allocation fetched, or may trigger a fetch immediately and have a
callback called once it is completed.

### File permission handling ###

We allow either Lustre style permission checking or client-side
permission checking for CPFS.  For the Lustre style checking, the
user and group ID mappings are read by the `UgidHandler` (see
`server/ms/ugid_handler.hpp`) during initialization, and is re-read
whenever the file is updated.  Before the MS `Store` performs an
operation that would like to have permission checked, it switches its
kernel FS uid and gid using the `UgidHandler`.  This mechanism checks
only the configured supplementary GIDs.

### MS replication and Inode locking ###

The MS replication mechanism as described in the design document is
implemented using the `Replier` class (see `server/ms/replier.hpp`),
which checks for things like whether the running MS is active or
standby, whether a standby is attached, whether we can immediately
reply according to the protocol, etc.  The protocol relies on that we
have related inodes locked, and the task is done by the
`InodeMutexArray` (see `server/ms/inode_mutex.hpp`).

Replication and caching requires the MS to clean up some information
periodically.  The `Cleaner` (see `server/ms/cleaner.hpp`) uses a
separate thread is used to perform such clean ups.

### DS locking ###

There is a technicality when performing truncates: if the MS performs
a `Truncate` at the same time when the DS is performing some `Write`,
we can end up with an inconsistent file.  As a result, the MS ensures
that the DS is not doing writes before performing the truncate
operation.  This mechanism is called DS locking.  In the MS side, the
work is done by the `DSLocker` (see `server/ms/ds_locker.hpp`), and in
the DS side the work is node by the `InodeFimDeferMgr` (see
`server/fim_defer.hpp`).

### Dirty attributes ###

As discussed in the design document, we need to know which inodes are
dirty.  They have to be kept in the disk as well, so that upon
restart, the file remains dirty and the associated attributes will be
updated before given to the FC.  The `DirtyInodeMgr` keeps these
information in the server, and persist them as needed.  It also
coordinate the update process: a generation number is associated with
each dirty inode, so that outdated attribute updates will not cause a
file to become clean incorrectly.

### MS Resynchronization ###

The MS resynchronization mechanism as described in the design document
is implemented using the `ResyncMgr` class (see
`server/ms/resync_mgr.hpp`).  The manager resynchronizes the content
of the data directory of the active MS, opened / pending unlink inode
information and the topology information of FC and DS.

The data directory content is scanned to select inodes by ctime, and
converted to Fims by using `MetaDirReader` (see
`server/ms/meta_dir_reader.hpp`).

Opened / pending unlink inodes are retrived from `MSInodeUsage` and
converted to Fims. Topology information of FC and DS are sent by APIs
from `TopologyMgr`.

As the resynchronization process could take time to complete, a
separate thread is created to handle resynchronization. To prevent
flooding peer MS with unhandled resync Fims, request send will be
blocked if the number of not replied Fim reached a threshold.

## DS ##

The DS defines a service where representation files are stored.  The
mapping of the user-visible file to the representation file is in
general done in the MS or FC, using a `FileCoordManager` (see
`ds_iface.hpp`).  It converts user-visible `FileCoord` to the
representation information `Segment`, which can then be used to drive
the invocation of the DS.

DS forms groups, and at any time the DS group is in one of the
possible states.  Such states are defined in `dsg_state.hpp`.  It also
define a `DSGStateInfo` structure, allowing the state to be stored
with other auxillary information.  A shared mutex is included in it,
which is usually needed for users to synchronize when the DSG state
changes.

The DS `Worker` handles requests from other FCs, the active MS, and
other DSs in the same DS group.  Modification requests from FCs need
to be replicated.  At times the DS needs to be "frozen", that new FC
requests are to be deferred when waiting for old requests to be
completed.  This is accompanished by the coordination of the `Worker`
with two abstractions.  Whenever the DS sends a Fim for replication,
it is registered with the `OpCompletionCheckerSet` (see
`op_completion.hpp`), which keeps a list of inodes that has
incompletion requests.  When a freeze is needed, it waits until all
registered requests of an inode of the whole system has been
completed, and use the `FimDeferMgr` (see `server/fim_defer.hpp`) to
defer Fims for that inode until the inode is unfreezed.

If a peer DS fails, the DS group moves into the degraded mode.  In
this mode, the DS serving checksum for a data block also serve to
recover data blocks lost in the failed DS.  The recovery procedure is
handled by the `RecoveryMgr`, ensuring that the same recovery is not
done twice concurrently.  The recovered data will be kept in a cache,
implemented in `DegradedCache`.  Both are defined in
`server/ds/degrade.hpp`.

If previously failed peer DS runs again, it first resynchronizes the
data so that it contains data consistent with the other DS.  The other
DSs send data to the previously failed DS using a `ResyncSender`, run
as a new thread in the `ResyncMgr` of the DS.  The failed DS receives
the data using a `ResyncFimProcessor`, part of the Fim processor in
the DS handling data from other DSs.  These are defined in
`server/ds/ds_resync.hpp`.
