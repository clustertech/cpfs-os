#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of CpfsCLI
 */

namespace cpfs {

class IConsole;

namespace client {

class ICpfsAdmin;
class ICpfsCLI;

/**
 * Get a CPFS CLI
 *
 * @param admin The admin
 *
 * @param console The console
 */
ICpfsCLI* MakeCpfsCLI(ICpfsAdmin* admin, IConsole* console);

}  // namespace client
}  // namespace cpfs
