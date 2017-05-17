#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of IThreadGroup to process FC FIMs
 * received, by dispatching to multiple cpfs::IThreadFimProcessor.
 */

namespace cpfs {
namespace server {

class IThreadGroup;

/**
 * Create the worker group
 */
IThreadGroup* MakeThreadGroup();

}  // namespace server
}  // namespace cpfs
