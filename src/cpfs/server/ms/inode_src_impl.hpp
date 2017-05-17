#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IInodeSrc interface.
 */

#include <string>

namespace cpfs {
namespace server {
namespace ms {

class IInodeSrc;

/**
 * Create an instance implementing the IInodeSrc interface.
 *
 * @param data_path Where data is store in the corresponding metadir.
 */
IInodeSrc* MakeInodeSrc(std::string data_path);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
