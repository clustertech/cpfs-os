#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of InodeUsage for tracking inode
 * operations in MS.
 */

#include <string>

namespace cpfs {
namespace server {
namespace ms {

class BaseMetaServer;
class IAttrUpdater;
class IDirtyInodeMgr;

/**
 * @param data_path Where data is stored in the corresponding metadir
 *
 * @return A dirty inode manager
 */
IDirtyInodeMgr* MakeDirtyInodeMgr(std::string data_path);

/**
 * @param server The meta server owning the updater
 *
 * @return An attribute updater
 */
IAttrUpdater* MakeAttrUpdater(BaseMetaServer* server);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
