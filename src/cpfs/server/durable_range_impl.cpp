/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the IDurableRange interface.
 */

#include "server/durable_range_impl.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/scope_exit.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/unordered_set.hpp>

#include "common.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "server/durable_range.hpp"

namespace cpfs {
namespace server {
namespace {

/** Type of set of inode number */
typedef boost::unordered_set<InodeNum> InodeNumSet;
/**
 * Size in number of Inodes to use for temporary internal buffers
 * during file read / write
 */
const int kBufSize = 512;

/**
 * Implement IDurableRange.
 */
class DurableRange : public IDurableRange {
 public:
  // It is important to reduce the number of calls to sync operations,
  // so that the implementation is reasonably fast, especially when
  // there are contention.  We achieve this by aggregating Add
  // requests: Apart from the two sets "latched" and "added" mandated
  // by the API, we also keep a set "queued".  When Add() is called,
  // if nothing yet is queued, the thread waits for some time so that
  // other threads can join, by depositing the range in the "queued"
  // set.  So the life-cycle of a range is normally queued -> added
  // (should also be in file) -> latched -> removed.  Ranges added /
  // latched are guaranteed to be in the file (unless something really
  // bad happen).  There is an optimization in case the range is
  // already latched, see the comment in FastLockCheck_().

  // A slightly elaborated synchronization scheme is needed to
  // coordinate access to these data structures.  A shared mutex is
  // used, so that Add() can share-lock it to put things into the
  // queue, while Load(), Latch() and Clear() will exclusive lock it
  // to ensure that no Add() is in progress.  Add() uses another
  // mutex-condition pair to wait for completion of the file writer.
  // This mutex is locked after the shared-mutex.  The main data
  // structure is protected by a data mutex, which is locked after the
  // above two mutexes.

  /**
   * @param filename Name of file to persist data to
   *
   * @param order The order: each range is of size 2^order
   */
  DurableRange(const char* filename, int order)
      : filename_(filename), mask_((InodeNum(1) << order) - 1),
        conservative_(false),
        fd_(-1), queue_mutex_(MUTEX_INIT), queueing_(false),
        data_mutex_(MUTEX_INIT) {}

  ~DurableRange() {
    CloseCachedFile_();
  }

  bool Load() {
    {
      MUTEX_LOCK_GUARD(op_mutex_);
      MUTEX_LOCK_GUARD(data_mutex_);
      latched_.clear();
      added_.clear();
      return LoadFile_();
    }
  }

  void SetConservative(bool conservative) {
    MUTEX_LOCK_GUARD(op_mutex_);
    CloseCachedFile_();
    conservative_ = conservative;
  }

  void Add(InodeNum inode1, InodeNum inode2, InodeNum inode3, InodeNum inode4) {
    InodeNumSet ranges, rranges;
    // First stage: fast-path argument preparation.
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      AddPrepInode_(inode1, &ranges);
      AddPrepInode_(inode2, &ranges);
      AddPrepInode_(inode3, &ranges);
      AddPrepInode_(inode4, &ranges);
      if (ranges.empty()) {  // Keep braces to avoid missing coverage
        return;
      }
    }
    {  // Non-trivial, share-lock op_mutex_
      MUTEX_LOCK(boost::shared_lock, op_mutex_, my_lock);
      // Second stage: Check for piggybacking
      {
        MUTEX_LOCK(boost::unique_lock, queue_mutex_, lock);
        BOOST_FOREACH(InodeNum range, ranges) {
          queued_.insert(range);
        }
        if (queueing_) {  // Piggyback on awaiting thread
          MUTEX_WAIT(lock, queue_emptied_);
          return;
        }
        queueing_ = true;
      }
      // Third stage: Can't piggyback, do it ourselves
      usleep(10000);  // Wait for 10 ms so that others can join
      MUTEX_LOCK_GUARD(queue_mutex_);
      {
        MUTEX_LOCK_GUARD(data_mutex_);
        BOOST_FOREACH(InodeNum range, queued_) {
          if (!FastLockCheck_(range))
            rranges.insert(range);
        }
        queued_.clear();
      }
      if (!rranges.empty())
        AppendFile_(rranges);
      MUTEX_LOCK_GUARD(data_mutex_);
      BOOST_FOREACH(InodeNum range, rranges) {
        added_.insert(range);
      }
      queueing_ = false;
      queue_emptied_.notify_all();
    }
  }

  void Latch() {
    {
      MUTEX_LOCK_GUARD(op_mutex_);
      InodeNumSet new_latched;
      {
        MUTEX_LOCK_GUARD(data_mutex_);
        // The following also makes future Add()'s wait if it needs to
        // hit the disk.  If it adds a range already in added_, it can
        // skip the wait because we will ensure the file will still
        // contain the range.
        latched_.clear();
        new_latched = added_;
      }
      SetFile_(new_latched, false);
      {
        MUTEX_LOCK_GUARD(data_mutex_);
        latched_ = new_latched;
        added_.clear();
      }
    }
  }

  void Clear() {
    {
      MUTEX_LOCK_GUARD(op_mutex_);
      MUTEX_LOCK_GUARD(data_mutex_);
      SetFile_(InodeNumSet(), true);
      latched_.clear();
      added_.clear();
    }
  }

  std::vector<InodeNum> Get() {
    std::vector<InodeNum> ret;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      std::copy(latched_.begin(), latched_.end(), std::back_inserter(ret));
      std::copy(added_.begin(), added_.end(), std::back_inserter(ret));
    }
    std::sort(ret.begin(), ret.end());
    std::vector<InodeNum>::iterator it = std::unique(ret.begin(), ret.end());
    ret.resize(std::distance(ret.begin(), it));
    return ret;
  }

 private:
  std::string filename_;
  int mask_;
  // Three locks in this class.  To avoid deadlock, lock in the
  // following order: (1) op_mutex_, then (2) queue_mutex_, and
  // finally (3) data_mutex_.
  mutable boost::shared_mutex op_mutex_;  // Some Add()s / a Latch() is ongoing
  bool conservative_;
  int fd_;
  mutable MUTEX_TYPE queue_mutex_;  // Protect queued_ and queueing_
  InodeNumSet queued_;
  bool queueing_;  // Whether an Add() is in progress
  boost::condition_variable queue_emptied_;  // Queue content now in file
  mutable MUTEX_TYPE data_mutex_;  // Protect data below
  // Latched ranges will be discarded upon next Latch() in added_.
  InodeNumSet latched_;
  // Ranges added and flushed to disk recently.
  InodeNumSet added_;

  // Prepare Inode argument in Add() by converting it to a range and
  // adding it to "ranges" if it has not been persisted before.
  // Before calling this, data_mutex_ should have been locked.
  void AddPrepInode_(InodeNum inode, InodeNumSet* ranges) {
    if (inode == InodeNum(-1))
      return;
    InodeNum range = inode & ~mask_;
    if (!FastLockCheck_(range))
      ranges->insert(range);
  }

  // Do a quick check about whether "range" is in either in latched_
  // or added_ (i.e., it is already persisted).  In the latter case,
  // add range to added_.
  bool FastLockCheck_(InodeNum range) {
    if (added_.find(range) != added_.end())
      return true;
    if (latched_.find(range) == latched_.end())
      return false;
    // If a range is allowed to pass fast-lock-check when it is
    // latched, it becomes recently accessed, so it is in added_
    // again.
    added_.insert(range);
    return true;
  }

  void SetFile_(const InodeNumSet& ranges, bool clear) {
    size_t pos = filename_.rfind('/');
    std::string dir = ".";
    if (pos != std::string::npos)
      dir = filename_.substr(0, pos + 1);  // Include the '/' as well
    if (clear) {
      remove(filename_.c_str());
    } else {
      std::string new_file = filename_ + ".new";
      {  // Write new file and sync
        int fd = open(new_file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0666);
        if (fd < 0)
          throw std::runtime_error(
              (boost::format("Cannot open %s for writing: %s")
               % new_file % strerror(errno)).str());
        fchmod(fd, 0666);
        BOOST_SCOPE_EXIT(fd) {
          close(fd);
        } BOOST_SCOPE_EXIT_END;
        WriteFileContent_(fd, ranges);
        fdatasync(fd);
      }
      CloseCachedFile_();
      if (std::rename(new_file.c_str(), filename_.c_str()) < 0)
        throw std::runtime_error(
            (boost::format("Cannot put new file to %s: %s")
             % filename_
             % strerror(errno)).str());  // Extreme condition, can't cover
    }
    // Sync the directory to make rename result durable
    int fd = open(dir.c_str(), O_RDONLY);
    if (fd < 0)
      throw std::runtime_error(
          (boost::format("Cannot open %s for sync: %s")
           % dir % strerror(errno)).str());  // Extreme condition, can't cover
    BOOST_SCOPE_EXIT(fd) {
      close(fd);
    } BOOST_SCOPE_EXIT_END;
    fdatasync(fd);
  }

  // Write content to a new file
  void WriteFileContent_(int fd, const InodeNumSet& ranges) {
    InodeNum buf[kBufSize];
    int bufUsed = 0;
    for (InodeNumSet::const_iterator it = ranges.cbegin();
         it != ranges.cend(); ++it) {
      buf[bufUsed++] = *it;
      if (bufUsed == kBufSize) {
        if (write(fd, buf, sizeof(buf)) < 0) {
          // Don't raise exception: we don't want this to crash the FS
          LOG(warning, Store,  // Extreme condition, can't cover
              "Failed writing ", filename_, ": ", strerror(errno));
        }
        bufUsed = 0;
      }
    }
    if (bufUsed)
      if (write(fd, buf, bufUsed * sizeof(buf[0])) < 0) {
        // Don't raise exception: we don't want this to crash the FS
        LOG(warning, Store,  // Extreme condition, can't cover
            "Failed writing ", filename_, ": ", strerror(errno));
      }
  }

  // Append ranges to a file.  fs_mutex should be locked
  void AppendFile_(const InodeNumSet& ranges) {
    boost::scoped_array<InodeNum> buf(new InodeNum[ranges.size()]);
    int i = 0;
    BOOST_FOREACH(InodeNum range, ranges) {
      buf[i++] = range;
    }
    if (fd_ < 0) {
      int flags = O_WRONLY | O_APPEND;
      if (conservative_)
        flags |= O_SYNC;
      fd_ = open(filename_.c_str(), flags);
      if (fd_ < 0) {  // Don't chmod if not creating file
        fd_ = open(filename_.c_str(), flags | O_CREAT, 0666);
        if (fd_ < 0)
          throw std::runtime_error(
              (boost::format("Cannot open %s for appending: %s") % filename_
               % strerror(errno)).str());
        fchmod(fd_, 0666);
      }
    }
    if (write(fd_, buf.get(), ranges.size() * sizeof(InodeNum)) < 0) {
      // Don't raise exception: we don't want this to crash the FS
      LOG(warning, Store,  // Extreme condition, can't cover
          "Failed writing ", filename_, ": ", strerror(errno));
    }
  }

  // Load the file as latched.  fs_mutex and data_mutex should be locked
  bool LoadFile_() {
    CloseCachedFile_();
    int fd = open(filename_.c_str(), O_RDONLY);
    if (fd < 0)
      return false;
    BOOST_SCOPE_EXIT(fd) {
      close(fd);
    } BOOST_SCOPE_EXIT_END;
    for (;;) {
      InodeNum buf[kBufSize];
      int num_read = read(fd, buf, sizeof(buf));
      if (num_read < 0)
        throw std::runtime_error(
            (boost::format("Error reading %s: %s")
             % filename_ % strerror(errno)).str());
      if (num_read == 0)
        break;
      for (int i = 0; i < num_read / int(sizeof(buf[0])); ++i)
        latched_.insert(buf[i]);
    }
    return true;
  }

  // Ensure that others will not reuse the file.  fs_mutex should be locked
  void CloseCachedFile_() {
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }
};

}  // namespace

IDurableRange* MakeDurableRange(const char* filename, int order) {
  return new DurableRange(filename, order);
}

}  // namespace server
}  // namespace cpfs
