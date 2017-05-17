#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IFimDeferMgr for trackering deferred
 * Fims, and IInodeFimDeferMgr for tracking deferred Fims per inode.
 */

namespace cpfs {
namespace server {

class IFimDeferMgr;
class IInodeFimDeferMgr;
class ISegmentFimDeferMgr;

/**
 * Get a Fim defer manager
 */
IFimDeferMgr* MakeFimDeferMgr();

/**
 * Get an Inode Fim defer manager
 */
IInodeFimDeferMgr* MakeInodeFimDeferMgr();

/**
 * Get a segment Fim defer manager
 */
ISegmentFimDeferMgr* MakeSegmentFimDeferMgr();

}  // namespace server
}  // namespace cpfs
