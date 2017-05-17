#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs:BaseStatusDumper base class for the
 * cpfs::IStatusDumper interface.
 */

namespace cpfs {

class IStatusDumpable;
class IStatusDumper;

/**
 * @param dumpable What to dump
 *
 * @return A new status dumper
 */
IStatusDumper* MakeStatusDumper(IStatusDumpable* dumpable);

}  // namespace cpfs
