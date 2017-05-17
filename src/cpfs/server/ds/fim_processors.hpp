#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define initial Fim processors for the data server.
 */

namespace cpfs {

class IFimProcessor;

namespace server {
namespace ds {

class BaseDataServer;

/**
 * Create a FimProcessor for DS to handle control Fims from MS.
 */
IFimProcessor* MakeMSCtrlFimProcessor(BaseDataServer* data_server);

/**
 * Create a FimProcessor for use during initial state when the data
 * server receives a connection.
 *
 * @param data_server The data server.
 */
IFimProcessor* MakeInitFimProcessor(BaseDataServer* data_server);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
