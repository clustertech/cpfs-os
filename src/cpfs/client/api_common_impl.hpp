#pragma once

/* Copyright 2015 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of FSCommon.
 */
namespace cpfs {
namespace client {

class IAPICommon;
class IFSAsyncRWExecutor;
class IFSCommonLL;

/**
 * Get the filesystem common facility
 *
 * @param fs The lowlevel filesystem layer
 */
IAPICommon* MakeAPICommon(IFSCommonLL* fs);

/**
 * Get the executor for asynchronus read or write
 *
 * @param api_common API common object to perform requests
 */
IFSAsyncRWExecutor* MakeAsyncRWExecutor(IAPICommon* api_common);

}  // namespace client
}  // namespace cpfs
