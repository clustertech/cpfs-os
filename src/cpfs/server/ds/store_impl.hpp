#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IStore interface.
 */

#include <string>

#include "common.hpp"

namespace cpfs {
namespace server {
namespace ds {

class IFileDataIterator;
class IChecksumGroupIterator;
class IFileDataWriter;
class IChecksumGroupWriter;
class IStore;

/**
 * Create an implementation of the IFileDataIterator interface.
 *
 * @param path The path to the data file to iterate
 */
IFileDataIterator* MakeFileDataIterator(const std::string& path);

 /**
 * Create an implementation of the IChecksumGroupIterator interface.
 *
 * @param path The inode path to the files to iterate, without
 * the ".c" or ".d" suffix
 *
 * @param inode The inode number
 *
 * @param role The DS role
 */
IChecksumGroupIterator* MakeChecksumGroupIterator(
    const std::string& path, InodeNum inode, GroupRole role);

/**
 * Create an implementation of the IFileDataWriter interface.
 *
 * @param path The path to the data file to write
 */
IFileDataWriter* MakeFileDataWriter(const std::string& path);

/**
 * Create an implementation of the IFileDataWriter interface.
 *
 * @param path The inode path to the files to write, without
 * the ".c" or ".d" suffix
 *
 * @param inode The inode number
 *
 * @param role The DS role
 */
IChecksumGroupWriter* MakeChecksumGroupWriter(
    const std::string& path, InodeNum inode, GroupRole role);

/**
 * Create a data directory object.  In the implementation returned, it
 * is assumed that two threads will not concurrently call (same or
 * different) methods with the same inode argument.  It is also
 * assumed that the data directory will not be modified in any other
 * ways, e.g., from another program.
 *
 * @param data_path The data directory.
 */
IStore* MakeStore(std::string data_path);

}  // namespace ds
}  // namespace server
}  // namespace cpfs
