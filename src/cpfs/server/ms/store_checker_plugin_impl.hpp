#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IStoreChecker interface and
 * plugins.
 */

namespace cpfs {
namespace server {
namespace ms {

class IStoreCheckerPlugin;

/**
 * @return A store checker plugin that does nothing.  Not useful
 * except to simplify unit test
 */
IStoreCheckerPlugin* MakeBaseStoreCheckerPlugin();

/**
 * @return A store checker plugin that checks the directory tree
 * structure and link counts
 */
IStoreCheckerPlugin* MakeTreeStoreCheckerPlugin();

/**
 * @return A store checker plugin that lets special inode files to pass
 */
IStoreCheckerPlugin* MakeSpecialInodeStoreCheckerPlugin();

/**
 * @return A store checker plugin that checks the inode count link validity
 */
IStoreCheckerPlugin* MakeInodeCountStoreCheckerPlugin();

}  // namespace ms
}  // namespace server
}  // namespace cpfs
