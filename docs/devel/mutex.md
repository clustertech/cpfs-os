# Mutex usage #

## Introduction ##

Mutex is a means to ensure that concurrent threads will not step on
each other.  It works by disallowing concurrency: when one thread has
successfully acquired a mutex, another thread also acquiring it has to
wait until the first one releases the mutex.  While this mechanism is
simple and at times necessary, it poses three difficulties.

  * It restricts parallelism.  If mutex is used in places where it is
    not required, operations that could be concurrent have to wait.
  * It is CPU intensive.  Acquiring an uncontended mutex requires
    around 60 CPU cycles with all cache hit.  If it is contended, the
    cost will have to include the CPU cycles needed to switch to the
    OS kernel and put the thread to the OS wait queue, and this easily
    requires hundreds or even thousands of cycles, not counting the
    actual waiting.
  * It can cause deadlock.  If a thread X acquires mutex A and then B,
    while another thread Y acquires mutex B and then A, concurrent
    execution of X and Y will cause X to wait for B and Y to wait for
    A forever.  This obviously extends to longer chains of calls.

CPFS uses a few means to reduce their impact.

 1. ASIO.  Many operations are done using single-threaded asynchronous
    I/O.  If some data structure is only accessed by a single ASIO
    thread, the data structure needs not support concurrent access,
    and thus do not need a mutex.
 2. Concurrent data structures.  Some data structures can support
    concurrent access without using any mutex, and instead rely only
    on atomic variable accesses.  All the three problems of mutex do
    not apply for these data structures.
 3. Reduced locking scope.  Instead of acquiring a mutex and hold it
    for the remainder of the method call, we at times release it long
    before the method returns.  This is especially important when we
    have to access methods of different objects, or even callbacks
    provided by other modules, as leaving mutex acquired can easily
    lead to deadlocks.
 4. Mutex hierarchy.  To prevent deadlocks, all data mutexes of all
    modules are put into an ordered list called the mutex hierarchy.
    The common modules are considered to be lower in the hierarchy
    than server and client specific code.  See the Mutex hierarchy
    section below for details.

## Mutex usage convention ##

  * Many classes have mutexes which protect the data of the class
    objects during external method calls.  These mutexes should always
    be called `data_mutex_`.  Methods expecting that `data_mutex_` is
    already acquired upon calling should have names ending with `_`.
  * All other mutexes usage should be listed in this document and
    explained clearly in code comments.
  * Whenever possible, mutexes should be acquired and released within
    a method with `MUTEX_LOCK_GUARD`, which always release the mutex
    upon method exit, and logs the mutex use in case the log level has
    `Lock=7`.
  * If the mutex needs transfer or temporary release by conditions,
    use `MUTEX_LOCK` instead.
  * Modules using mutexes directly or indirectly via other modules
    need to be listed in this document in the mutex hierarchy.

## Callback naming convention ##

To make it easy to spot places where callbacks are used, we use the
following conventions when creating callbacks.  Here the "callback
user (function)" refers to the function receiving the callback as
argument.

  * Case 1: The callback is only called synchronously, during the
    invocation of the callback user.  It is similar to how the
    standard C library uses the comparison function.  In such cases,
    the name of the formal function argument should look like
    `compare_func`, and the function type, if named, should look like
    `CompareFunc`.  The name of the callback function and the callback
    user function should not indicate asynchronous feature.
  * Case 2: The callback user function is a request for certain
    operation asynchronously, and the callback function will be
    invoked at the end of the operation.  It is similar to how the
    Asio library uses callbacks.  In such cases, the name of the
    formal function argument should look like `completion_handler`,
    and the function type, if named, should look like
    `AttrUpdateHandler`.  The callback user function should be named
    like `AsyncUpdateAttr`, and the callback function should be named
    like `UpdateAttrCompleted` or something more intuitive.  E.g.,

        typedef boost::function<void(int)> AttrUpdateHandler;
        void AsyncUpdateAttr(AttrUpdateHandler attr_update_handler);
        void UpdateAttrCompleted(int result);
        ...
        void AsyncUpdateAttr(UpdateAttrCompleted);

  * Case 3: The callback is registered to be called asynchronously at
    a later time, when certain event occurs.  Usually it makes sense
    for multiple callbacks to be registered.  In such cases, the name
    of the formal function argument should look like
    `new_stat_callback`, and the function type, if named, should look
    like `NewStatCallback`.  The callback user function should be
    named like `OnNewStat`, and the callback function should be named
    like `NewStatTriggered` or something more intuitive.  E.g.,

        typedef boost::function<void(boost::shared_ptr<Stat>)>
            AttrUpdateHandler;
        void OnNewStat(NewStatCallback new_stat_callback);
        void NewStatTriggered(boost::shared_ptr<Stat> stat);
        ...
        void OnNewStat(NewStatTriggered);

Note that functions dedicated to be used as callback should be named
passively, while other functions should be named actively.  Names of
the above mentioned forms, i.e., `xxx_func`, `XxxFunc`, `xxx_handler`,
`XxxHandler`, `xxx_callback` `XxxCallback` and `OnXxx`, should be
avoided for other uses.

## Mutex hierarchy ##

In this section, we refer to all classes and functions in `xxx.hpp`
and the corresponding `xxx.cpp` and `xxx_impl.cpp` to be "the module
`xxx`".

To prevent deadlock, a module X holding a mutex of its own can only
call methods in another module Y if (a) module Y does not use mutexes
at all, or (b) Y below X in the mutex hierarchy.

A module X can install a callback to another module Y if (a) the
callback is known to be called without any mutexes locked (e.g.,
directly from Asio, or documented as such in API), or (b) the callback
only calls methods in modules which (i) do not use mutex, (ii) are
below module Y in the mutex hierarchy, or (iii) are non-dependent in
the sense that they don't hold its own mutex when calling other
modules at all (N.B.: modules in MS / DS does not normally pass this
criteria, even if they are listed as "non-dependent" below).

The requirement on callbacks is a little stringent.  We expect that in
some cases we need to arrange the actual work of the callback to
happen after the callback is called, e.g., by putting them in some
task queue.

Note that even with these precautions, complex situations can lead to
deadlock across servers / clients, e.g., due to waiting for messages
or the use of non-data mutexes.  So adequate testing must be done.

### Common modules ###

The common modules need careful analysis for their interactions of
mutex actions, as calls among them are otherwise unstructured
relatively.

#### Non-dependent common modules ####

The following modules use mutex to protect their own data structure,
but will never call other mutex-using modules during the time when the
mutex has been acquired.  They thus do not need special consideration,
can be positioned at bottom in the mutex hierarchy, and may be called
by callbacks.

  * `config_mgr`
  * `event`
  * `fim_socket` (except Shutdown(), which may only be called without lock)
  * `inode_map`
  * `req_entry` (except SetReply(), which may only be called without lock)
  * `shutdown_mgr_base`
  * `time_keeper_impl`
  * `tracer`
  * `server/inode_removal_tracker`

#### Mutex hierarchy ####

Other than the above, the common modules mutex hierarchy are as follows.

  * `dsg_state`
  * `req_limiter`
  * `req_tracker`
  * `thread_fim_processor`
  * `op_completion`
  * `ds_iface DSFimRedirector`
  * `tracker_mapper`
  * `connector`
  * `authenticator`

#### Mutex dependencies ####

The above lists are obtained by locating all the source files with the
keyword "mutex", and analysing all cross-module calls being made
during the time when the mutex has been acquired by a method.  The
following lists the actual dependencies, which may be useful when the
hierarchy needs changing.

  * `authenticator`
  * `connector` -> `authenticator`
  * `ds_iface DSFimRedirector` -> `tracker_mapper`
  * `dsg_state` -> `req_tracker` -> `op_completion` (in `server/ds/worker`)
  * `op_completion` -> (#)
  * `req_limiter` -> `req_tracker`
  * `req_tracker`
  * `tracker_mapper`

(#): Various modules via callbacks, could be some deadlocks waiting
for us!

### MS modules ###

Dependencies for servers and clients are not traced as carefully.  In
particular, we assume that servers and clients will always be higher
than common facilities in the mutex hierarchy, so we ignore references
to common facilities.

#### Non-dependent MS modules ####

Like common modules, some MS modules will never hold and wait, not
even waiting for common modules.  These are considered to be
non-dependent, located in the mutex hierarchy together with the common
non-dependent modules.  They may also be safely called from callback
functions.

  * `server/ms/inode_src`
  * `server/ms/inode_usage`
  * `server/ms/resync_mgr`
  * `server/ms/ugid_handler`

Some modules using mutexes which only calls common modules during the
time the mutexes have been acquired.  They are said to be
semi-dependent.  They can be considered to be at the bottom of the MS
mutex hierarchy (but above the common modules mutex hierarchy).

  * `server/ms/dirty_inode`
  * `server/ms/ds_locker`
  * `server/ms/inode_mutex`
  * `server/ms/replier`
  * `server/ms/topology`

Modules marked with (*) above do not use other mutexes at all, and
will not call other modules requiring those either.  Thus they may be
safely called from callback functions.

#### Mutex hierarchy ####

Only the following MS modules remain, which does not call each other.

  * `server/ms/failover_mgr` (use various other modules)
  * `server/ms/store` (use `inode_mutex`)
  * `server/ms/stat_keeper` (call `topology` in its callback)
  * `server/ms/state_mgr` (call `resync_mgr` in its callback)

### DS and client modules ###

Currently all DS modules using mutexes are non-dependent.  The
`client/cache` module is also non-dependent, and `client/cpfs_fso`
depend on it.  They do not necessitate a mutex hierarchy yet.

## Special mutex ##

  * `server/ms/inode_mutex` - recursive locks for inode operations
  * `dsg_state` - reader-writer lock for control
  * `op_completion` - object liviness mutex
