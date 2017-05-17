#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define the IStoreChecker interface.
 */
#include <stdexcept>
#include <string>

#include "common.hpp"

namespace cpfs {

class IConsole;

namespace server {
namespace ms {

class IStore;
class IStoreChecker;

/**
 * Interface for meta-server directory checker plugin.  In general,
 * the plugin should work in either of two modes: check-mode or
 * correct-mode.  In either case, it would do whatever work required
 * to validate the store content when a method is called by the store
 * checker during various stages of the checking process.  If any
 * error is found, the method will be flagged by returning false, and
 * in correct-mode, modification is made to the FS attempting to
 * correct the problem found.  If correction fails, the method should
 * throw an exception to stop the checker.  A method concerning an
 * item, e.g., a dentry or an inode, may throw a SkipItemException to
 * ask the checker to stop processing of the item in other plugins,
 * and it is permissible for such methods to either flag it as an
 * error or not.  Any other exception, if thrown, will terminate the
 * checker.
 *
 * The methods are called in the following sequence: first Init() is
 * called.  Then InfoFound() is called for each server info found,
 * followed by a single call to InfoScanComplete().  After that,
 * InodeFound() or UnknownNodeFound() is called for each node in the
 * root directory, followed by a single call to InodeScanComplete().
 * Finally, DentryFound() or UnknownDentryFound() is called for each
 * dentry in each directory held, followed by a single call to
 * DentryScanComplete().
 */
class IStoreCheckerPlugin {
 public:
  virtual ~IStoreCheckerPlugin() {}

  /**
   * Called during initialization of the checker.
   *
   * @param checker The checker
   *
   * @param do_correct Whether to attempt correcting errors found
   *
   * @return Whether checks passed
   */
  virtual bool Init(IStoreChecker* checker, bool do_correct) = 0;

  /**
   * Called when some server information is found.
   *
   * @param name The name of the information
   *
   * @param value The value of the information
   *
   * @return Whether checks passed
   */
  virtual bool InfoFound(std::string name, std::string value) = 0;

  /**
   * Called when server info scan completes.
   *
   * @return Whether checks passed
   */
  virtual bool InfoScanCompleted() = 0;

  /**
   * Called when an inode is found.
   *
   * @param inode The inode number
   *
   * @param stbuf The stat buffer storing the inode stats
   *
   * @param copy The link suffix in the MS filename, 0 if there is
   * none, -1 for control file
   *
   * @return Whether checks passed
   */
  virtual bool InodeFound(InodeNum inode, struct stat* stbuf,
                          int copy) = 0;

  /**
   * Called when a node cannot be interpreted as an inode.  At least
   * one plugin must throw a SkipItemException with is_error=false for
   * an error not to be flagged.
   *
   * @param name The name of the node
   *
   * @return Whether checks passed
   */
  virtual bool UnknownNodeFound(std::string name) = 0;

  /**
   * Called when inode scan completes.
   *
   * @return Whether checks passed
   */
  virtual bool InodeScanCompleted() = 0;

  /**
   * Called when a directory entry is found.
   *
   * @param parent The parent inode number
   *
   * @param name The name of the entry
   *
   * @param inode The inode number
   *
   * @param stbuf The stat buffer of the entry
   *
   * @return Whether checks passed
   */
  virtual bool DentryFound(InodeNum parent, std::string name,
                           InodeNum inode, struct stat* stbuf) = 0;

  /**
   * Called when a dentry cannot be recognized as referring to an
   * inode.  At least one plugin must throw a SkipItemException with
   * is_error=false for an error not to be flagged.
   *
   * @param parent The parent
   *
   * @param name The name of the dentry
   *
   * @return Whether checks passed
   */
  virtual bool UnknownDentryFound(InodeNum parent, std::string name) = 0;

  /**
   * Called when inode scan completes.
   *
   * @return Whether checks passed
   */
  virtual bool DentryScanCompleted() = 0;
};

/**
 * Exception to notify dentry loops detected.
 */
class SkipItemException : public std::runtime_error {
 public:
  /**
   * @param path The path of the item
   *
   * @param is_error Whether to signify an error
   */
  explicit SkipItemException(std::string path, bool is_error = true)
      : std::runtime_error(std::string("Skip ") +
                           (is_error ? "unexpected " : "") +
                           "item at " + path),
        path(path), is_error(is_error) {}
  virtual ~SkipItemException() throw() {}
  const std::string path; /**< Path to skip */
  bool is_error; /**< Whether to signify an error */
};

/**
 * Interface for meta-server directory checker.
 */
class IStoreChecker {
 public:
  virtual ~IStoreChecker() {}

  /**
   * Register a plugin.  The plugin will then be owned by the store.
   *
   * @param plugin The plugin
   */
  virtual void RegisterPlugin(IStoreCheckerPlugin* plugin) = 0;

  /**
   * Run the checker.  This function should only be called once per
   * checker.
   *
   * @param do_correct Whether correction is done
   *
   * @return Whether the store is valid
   */
  virtual bool Run(bool do_correct) = 0;

  // Plugin API

  /**
   * @return The root of the store
   */
  virtual std::string GetRoot() = 0;

  /**
   * Get the path representing an inode.
   *
   * @param inode The inode number
   *
   * @return The path
   */
  virtual std::string GetInodePath(InodeNum inode) = 0;

  /**
   * Get the path representing a directory entry.
   *
   * @param parent The parent directory inode number
   *
   * @param name The name of the directory entry
   *
   * @return The path
   */
  virtual std::string GetDentryPath(InodeNum parent, std::string name) = 0;

  /**
   * Get the console for showing messages.
   *
   * @return The console used
   */
  virtual IConsole* GetConsole() = 0;

  /**
   * Get the store object used.
   *
   * @return The console used
   */
  virtual IStore* GetStore() = 0;
};

}  // namespace ms
}  // namespace server
}  // namespace cpfs
