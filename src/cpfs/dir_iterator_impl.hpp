#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of the IDirIterator interface for
 * iterating entries in a directory.
 */

#include <string>
#include <vector>

#include "common.hpp"

namespace cpfs {

class IDirIterator;

/**
 * Create a DirIterator
 *
 * @param path Full path to the directory
 */
IDirIterator* MakeDirIterator(const std::string& path);

/**
 * Create a DirIterator that traverses two level directories as if one flat
 * directory
 *
 * @param path Full path to the directory
 */
IDirIterator* MakeLeafViewDirIterator(const std::string& path);

/**
 * Create a DirIterator that scan possible inodes specified as ranges.
 * The iterator does not do a listdir() at all.  Instead, it use
 * lstat() to check for the presence of each of the inodes in the
 * specified ranges.  If the list of ranges is small this can be much
 * faster than listing a large directory.
 *
 * @param path Full path to the directory
 *
 * @param ranges The Inode ranges requested
 *
 * @param append A string to append to the end of the inode number
 * as filename
 */
IDirIterator* MakeInodeRangeDirIterator(const std::string& path,
                                        const std::vector<InodeNum>& ranges,
                                        const std::string& append = "");

}  // namespace cpfs
