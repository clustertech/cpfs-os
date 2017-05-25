# Building CPFS-OS from source

## Building CPFS-OS ##

CPFS-OS can be built using Docker.  A pretty old Docker like the one
in CentOS 6 can do the trick:

    # yum install docker-io

With it, you can build CPFS-OS with the following command, replacing
`<Dockerfile>` by the desired Dockerfile of the target OS and
`<src-dir>` by the directory containing the Dockerfile and other
source files.  (Dockerfiles can be found under the `docker`
directory.)

    # docker build -t cpfs-os-build -f <Dockerfile> <src-dir>

When the command completes, a Docker image called `cpfs-os-build` is
produced.  Packages built are at `/home/builder/build/package`, which
you can extract with the Docker `cp` command.  E.g.,

    # id=$(docker create cpfs-os-build --entrypoint=/bin/bash)
    # docker cp $id:/home/builder/build/package .
    # docker rm $id

## Unit tests ##

You can also use the Docker image to run the unit tests provided with
the source code.  It doesn't really run a CPFS filesystem, but instead
test each module to see whether anything breaks.  But since it does
make use of FUSE and extended attributes, you need to run Docker
privileged, and pass a host directory to it as `/tmp`.  E.g., (replace
`<local-dir>` with a host directory with extended attribute support)

    # chown 1000.1000 <local-dir>
    # docker run -tiv <local-dir>:/tmp --privileged cpfs-os-build bash

Once you have a shell within docker, you can do the following:

    [builder@12345678] mkdir /tmp/cpfs; cd /tmp/cpfs
    [builder@12345678] cmake ~
    [builder@12345678] make -j3 cpfs_unittest unittest_helpers
    [builder@12345678] ./cpfs_unittest

## Not using Docker ##

You can also build CPFS-OS without Docker (this is likely more
appropriate if you want to modify CPFS-OS).  Simply read the
Dockerfile closest to your distribution (they are quite simple) and
follow the commands.

If you run the unit tests as normal user, you need to ensure that the
user has permission to run `fusermount`, and the user can access
`/dev/fuse`.

If you build CPFS for development, it is likely that you want to
create a debug build instead of a release build.  To do so, use
`-DCMAKE_BUILD_TYPE=Debug` when running `cmake`, instead of the
`-DCMAKE_BUILD_TYPE=Release` specified in the Dockerfile.

Building CPFS-OS without root in CentOS 6 is slightly complex because
we need to build our own Boost, Botan, and FUSE library.  Here are
some hints:

  * When configuring such libraries for building, you can use the
    `--prefix=<prefix>` argument to specify a location other than
    `/usr` and `/usr/local` to install the resulting files.
  * When configuring FUSE, you will also need to set these environment
    variables during configure: `MOUNT_FUSE_PATH`, `UDEV_RULES_PATH`
    and `INIT_D_PATH`.  Remove the built `fusermount` afterwards
    (use the one provided by the system, which has the necessary
    permissions).
  * Add the parameter `-DDEP_PREFIX=<prefix>` to the `cmake` command.
