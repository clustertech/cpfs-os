#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines IClientFimProcessor for FC
 */

namespace cpfs {

class IFimProcessor;

namespace client {

class BaseFSClient;

/**
 * Create a fim processor for FC to handle control Fims from MS
 *
 * @param client The client
 */
IFimProcessor* MakeMSCtrlFimProcessor(BaseFSClient* client);

/**
 * Create a fim processor when connecting to DS
 *
 * @param client The client
 */
IFimProcessor* MakeDSCtrlFimProcessor(BaseFSClient* client);

/**
 * Create a fim processor which is useful for handling Fims from both
 * MS and DS.
 *
 * @param client The client
 */
IFimProcessor* MakeGenCtrlFimProcessor(BaseFSClient* client);

}  // namespace client
}  // namespace cpfs
