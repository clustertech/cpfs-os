#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines Processor for Admin
 */

namespace cpfs {

class IFimProcessor;

namespace client {

class BaseAdminClient;

/**
 * Create a fim processor which is useful for admin Fims handling.
 *
 * @param client The client
 */
IFimProcessor* MakeAdminFimProcessor(BaseAdminClient* client);

}  // namespace client
}  // namespace cpfs
