# -*- cmake -*-
# Global settings
cmake_minimum_required(VERSION 2.8.9)
project(cpfs-os)
set(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:/usr/local/lib/pkgconfig")

if(LINK_STATIC)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a;.so")
  set(BUILD_SHARED_LIBRARIES OFF)
endif(LINK_STATIC)

find_package(PkgConfig REQUIRED)
pkg_search_module(FUSE REQUIRED fuse)
file(STRINGS "lib/pantheios-ver" PANVER)

if(LINK_STATIC)
  find_library(FUSELIB NAMES libfuse.a)
else(LINK_STATIC)
  set(FUSELIB ${FUSE_LIBRARIES})
endif(LINK_STATIC)

set(CMAKE_CXX_FLAGS "-include defs.hpp ${FUSE_CFLAGS_OTHER}")
include_directories(SYSTEM include /usr/include/botan-1.10 ${FUSE_INCLUDE_DIRS})
include_directories(src/cpfs)
link_directories(lib /usr/local/lib)

if(LINK_STATIC)
  link_libraries(
    libboost_program_options.a libboost_filesystem.a libboost_thread.a
    libboost_date_time.a libboost_random.a libboost_system.a
    libbotan-1.10.a dl rt)
else(LINK_STATIC)
  link_libraries(
    boost_program_options boost_filesystem boost_thread
    boost_date_time boost_random boost_system
    botan-1.10)
endif(LINK_STATIC)

link_libraries(
  pantheios.1.appl.${PANVER}.file64bit.mt
  pantheios.1.core.${PANVER}.file64bit.mt
  pantheios.1.be.file.${PANVER}.file64bit.mt
  pantheios.1.bec.file.${PANVER}.file64bit.mt
  pantheios.1.util.${PANVER}.file64bit.mt pthread)

# Library
file(GLOB_RECURSE SOURCES "src/cpfs/*.cpp")
file(GLOB_RECURSE SOURCES_TEST "src/cpfs/*_test.cpp")
file(GLOB_RECURSE SOURCES_TEST_EXTRA "src/cpfs/*_test?*.cpp")
file(GLOB_RECURSE SOURCES_MAIN "src/cpfs/main/*.cpp")
list(REMOVE_ITEM SOURCES ${SOURCES_TEST})
list(REMOVE_ITEM SOURCES ${SOURCES_TEST_EXTRA})
list(REMOVE_ITEM SOURCES ${SOURCES_MAIN})
add_library(cpfs_lib ${SOURCES})

# Main executables
add_executable(cpfs_keygen src/cpfs/main/cpfs_keygen.cpp)
target_link_libraries(cpfs_keygen cpfs_lib)

add_executable(cpfs_cli src/cpfs/main/cpfs_cli.cpp)
target_link_libraries(cpfs_cli cpfs_lib)

add_executable(ms_checker src/cpfs/main/ms_checker.cpp)
target_link_libraries(ms_checker cpfs_lib)

add_executable(meta_dir_diff src/cpfs/main/meta_dir_diff.cpp)
target_link_libraries(meta_dir_diff cpfs_lib)

add_executable(checksum_validator src/cpfs/main/checksum_validator.cpp)
target_link_libraries(checksum_validator cpfs_lib)

add_executable(cpfs_client src/cpfs/main/cpfs_client.cpp)
target_link_libraries(cpfs_client cpfs_lib ${FUSELIB})

add_executable(cpfs_server
  src/cpfs/main/cpfs_server.cpp src/cpfs/main/server_main.cpp)
target_link_libraries(cpfs_server cpfs_lib)

# Test executables
add_executable(cpfs_unittest EXCLUDE_FROM_ALL
  src/cpfs/main/unittest_main.cpp
  src/cpfs/main/server_main.cpp
  src/cpfs/log_testlib.cpp
  ${SOURCES_TEST})
target_link_libraries(cpfs_unittest cpfs_lib gmock ${FUSELIB})

add_custom_target(make_test_helper_dirs
  COMMAND mkdir -p tbuild/cpfs/main tbuild/cpfs/client)

add_executable(tbuild/cpfs/mock_actions_test_segv EXCLUDE_FROM_ALL
  src/cpfs/mock_actions_test_segv.cpp)
add_dependencies(tbuild/cpfs/mock_actions_test_segv make_test_helper_dirs)

add_executable(tbuild/cpfs/daemonizer_test_helper EXCLUDE_FROM_ALL
  src/cpfs/daemonizer_test_helper.cpp)
target_link_libraries(tbuild/cpfs/daemonizer_test_helper cpfs_lib)
add_dependencies(tbuild/cpfs/daemonizer_test_helper make_test_helper_dirs)

add_executable(tbuild/cpfs/fuseobj_test_error EXCLUDE_FROM_ALL
  src/cpfs/fuseobj_test_error.cpp)
target_link_libraries(tbuild/cpfs/fuseobj_test_error ${FUSELIB})
add_dependencies(tbuild/cpfs/fuseobj_test_error make_test_helper_dirs)

add_executable(tbuild/cpfs/fuseobj_test_empty EXCLUDE_FROM_ALL
  src/cpfs/fuseobj_test_empty.cpp)
target_link_libraries(tbuild/cpfs/fuseobj_test_empty ${FUSELIB})
add_dependencies(tbuild/cpfs/fuseobj_test_empty make_test_helper_dirs)

add_executable(tbuild/cpfs/fuseobj_test_statable EXCLUDE_FROM_ALL
  src/cpfs/fuseobj_test_statable.cpp)
target_link_libraries(tbuild/cpfs/fuseobj_test_statable ${FUSELIB})
add_dependencies(tbuild/cpfs/fuseobj_test_statable make_test_helper_dirs)

add_executable(tbuild/cpfs/client/cpfs_fso_test_parse EXCLUDE_FROM_ALL
  src/cpfs/client/cpfs_fso_test_parse.cpp)
target_link_libraries(tbuild/cpfs/client/cpfs_fso_test_parse
  cpfs_lib gmock ${FUSELIB})
add_dependencies(tbuild/cpfs/client/cpfs_fso_test_parse make_test_helper_dirs)

add_executable(tbuild/cpfs/main/server_main_test_parse EXCLUDE_FROM_ALL
  src/cpfs/main/server_main_test_parse.cpp
  src/cpfs/main/server_main.cpp)
target_link_libraries(tbuild/cpfs/main/server_main_test_parse cpfs_lib)
add_dependencies(tbuild/cpfs/main/server_main_test_parse make_test_helper_dirs)

add_custom_target(unittest_helpers)
add_dependencies(unittest_helpers
  make_test_helper_dirs
  tbuild/cpfs/mock_actions_test_segv
  tbuild/cpfs/daemonizer_test_helper
  tbuild/cpfs/fuseobj_test_error
  tbuild/cpfs/fuseobj_test_empty
  tbuild/cpfs/fuseobj_test_statable
  tbuild/cpfs/client/cpfs_fso_test_parse
  tbuild/cpfs/main/server_main_test_parse)

# Installation
install(TARGETS cpfs_server cpfs_client cpfs_cli cpfs_keygen
  RUNTIME DESTINATION sbin)
install(PROGRAMS scripts/cpfs_configure_client DESTINATION sbin)
install(PROGRAMS scripts/mount.cpfs DESTINATION /sbin)
install(PROGRAMS scripts/cpfs-meta scripts/cpfs-data DESTINATION /etc/init.d)
install(DIRECTORY docs/third-party-copyrights DESTINATION share/doc/cpfs-os)
install(DIRECTORY docs/user-guide DESTINATION share/doc/cpfs-os)
install(FILES README.md LICENSE DESTINATION share/doc/cpfs-os)
install(FILES default/cpfs-meta default/cpfs-data DESTINATION /etc/default)

# Packaging
INCLUDE(InstallRequiredSystemLibraries)
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY
  "ClusterTech Parallel Filesystem server, client, tools and documentation")
SET(CPACK_PACKAGE_VENDOR "ClusterTech Ltd")
SET(CPACK_PACKAGE_CONTACT "cpfs@clustertech.com")
SET(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
SET(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
SET(CPACK_PACKAGE_VERSION_MAJOR "1")
SET(CPACK_PACKAGE_VERSION_MINOR "2")
SET(CPACK_PACKAGE_VERSION_PATCH "1")
SET(CPACK_PACKAGE_INSTALL_DIRECTORY
  "CMake ${CMake_VERSION_MAJOR}.${CMake_VERSION_MINOR}")
SET(CPACK_SET_DESTDIR ON)
execute_process(COMMAND test -f /etc/debian_version RESULT_VARIABLE IS_DEB)
if(IS_DEB EQUAL 0)
  set(CPACK_GENERATOR "DEB")
  set(CPACK_DEBIAN_PACKAGE_SECTION "admin")
  set(CPACK_DEBIAN_PACKAGE_MAINTAINER "ClusterTech Ltd")
  set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
  set(CPACK_DEBIAN_PACKAGE_DEPENDS "fuse, lsb-base")
  set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/conffiles"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postinst"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postrm")
else(IS_DEB EQUAL 0)
  set(CPACK_GENERATOR "RPM")
  set(CPACK_RPM_PACKAGE_RELEASE 1)
  set(CPACK_RPM_PACKAGE_RELOCATABLE OFF)
  set(CPACK_RPM_PACKAGE_LICENSE "AGPL 3")
  set(CPACK_RPM_PACKAGE_REQUIRES "fuse, redhat-lsb-core")
  set(CPACK_RPM_USER_FILELIST
    "%config(noreplace) /etc/default/cpfs-meta"
    "%config(noreplace) /etc/default/cpfs-data")
  set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/sbin")
  set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postinst")
  set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postrm")
endif(IS_DEB EQUAL 0)
INCLUDE(CPack)
