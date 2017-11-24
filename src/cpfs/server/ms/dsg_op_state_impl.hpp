#pragma once

/* Copyright 2017 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the DSG operation state class.
 */

namespace cpfs {

class IOpCompletionCheckerSet;

namespace server {
namespace ms {

class IDSGOpStateMgr;

/**
 * Create an DSG operation states manager.
 *
 * @param checker_set The checker set to use
 */
IDSGOpStateMgr* MakeDSGOpStateMgr(IOpCompletionCheckerSet* checker_set);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
