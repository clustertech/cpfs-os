#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Header for implementation of Console.
 */

#include <cstdio>

namespace cpfs {

class IConsole;

/**
 * Make a console
 *
 * @param in The standard input
 *
 * @param out The standard output
 *
 * @param err The standard error
 */
IConsole* MakeConsole(FILE* in, FILE* out, FILE* err);

}  // namespace cpfs
