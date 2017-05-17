#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IStoreChecker interface and
 * plugins.
 */

#include <string>

namespace cpfs {

class IConsole;

namespace server {
namespace ms {

class IStoreChecker;

/**
 * Create a MS store checker.
 *
 * @param data_path The data directory
 *
 * @param console The console object for printing messages and errors.
 * Adopted by the checker
 */
IStoreChecker* MakeStoreChecker(std::string data_path,
                                IConsole* console);

}  // namespace ms
}  // namespace server
}  // namespace cpfs
