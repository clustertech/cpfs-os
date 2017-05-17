/* Copyright 2013 ClusterTech Ltd */
#include <fuse/fuse_opt.h>

#include <cstdio>
#include <stdexcept>

#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>

#include <pantheios/frontends/stock.h>  // IWYU pragma: keep

#include "fuseobj_mock.hpp"
#include "client/base_client.hpp"
#include "client/cpfs_fso.hpp"

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "CPFS";  /**< ID for logging */

// Some tests of cpfs_fso generate output on stderr unconditionally.
// This program allows capturing of the output so that it is not
// dumped as test output, and allow capturing of the output for
// checking
int main(int argc, char* argv[]) {
  boost::shared_ptr<cpfs::MockFuseMethodPolicy> mock_fuse_method =
      boost::make_shared<cpfs::MockFuseMethodPolicy>();
  cpfs::client::CpfsFuseObj<cpfs::MockFuseMethodPolicyF> cpfs_fuse_obj(
      mock_fuse_method);
  cpfs::client::BaseFSClient client;
  cpfs_fuse_obj.SetClient(&client);
  fuse_args args = FUSE_ARGS_INIT(argc, argv);
  int ret = 0;
  try {
    cpfs_fuse_obj.ParseFsOpts(&args);
  } catch (const std::runtime_error& e) {
    ret = 1;
    std::printf("%s\n", e.what());
  }
  fuse_opt_free_args(&args);
  return ret;
}
