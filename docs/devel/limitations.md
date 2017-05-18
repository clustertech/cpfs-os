# Limitations #

This is a random collection of things that the filesystem might differ
from the expectation of some users.

  * Upon MS failover, directory entry ordering within a directory (as
    seen by `readdir(2)`) may change.  If care has been taken to
    create underlying file system using the same seed in the primary
    and the secondary MS, such problem will only happen when there is
    a hash collision in the directory entries, which is very unlikely
    unless one has very large directories.
  * In the above case, if a traversal of the directory is done
    encompassing the failover time, multiple entries / skipped entries
    may results.
  * Filesystem statistics (as reported by `statvfs(2))` are not
    accurate, and will change after MS and DS failover and resync.
  * Multi-segment writes are not atomic.  If an operation fails, e.g.,
    due to disk full, part of its changes may already be made, and
    they are not rolled back.  An error will be reported, probably in
    some later write(2) or close(2) calls.  We guarantee that the
    filesystem as a whole remains consistent, though.
