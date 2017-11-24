#pragma once

/* Copyright 2017 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the DSG operation state class.
 */

namespace cpfs {
namespace server {
namespace ms {

class IDSGOpStateMgr;

/**
 * Create an DSG operation states manager.
 */
IDSGOpStateMgr* MakeDSGOpStateMgr();

}  // namespace ms
}  // namespace server
}  // namespace cpfs
