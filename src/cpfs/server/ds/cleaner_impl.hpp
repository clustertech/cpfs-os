#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define ICleaner interface.
 */

namespace cpfs {
namespace server {
namespace ds {

class BaseDataServer;
class ICleaner;

/**
 * Create an implementation of the DS ICleaner interface.
 *
 * @param server The data server to clean up
 */
ICleaner* MakeCleaner(BaseDataServer* server);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
