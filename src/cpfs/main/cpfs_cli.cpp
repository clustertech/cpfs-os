/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Main entry for CPFS CLI.
 */

#include <sys/resource.h>

#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/program_options.hpp>
#include <boost/scoped_ptr.hpp>

#include <pantheios/frontends/stock.h>  // IWYU pragma: keep

#include "common.hpp"
#include "console.hpp"
#include "console_impl.hpp"
#include "logger.hpp"
#include "client/admin_client_impl.hpp"
#include "client/cpfs_admin.hpp"
#include "client/cpfs_admin_impl.hpp"
#include "client/cpfs_cli.hpp"
#include "client/cpfs_cli_impl.hpp"

 /**
  * Load options from command line or environment as configs
  *
  * @param argc The argument count
  *
  * @param argv The arguments specified
  *
  * @param configs The configs parsed
  */
bool LoadClientOpts(int argc, char* argv[], cpfs::AdminConfigItems* configs) {
  namespace po = boost::program_options;
  po::variables_map vm;
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "Produce help message")
      ("meta-server", po::value<std::string>(),
       "host:port, The host:port of the meta server(s), separated by comma")
      ("log-level", po::value<std::string>()->default_value("2"),
       "The logging level (0 - 7).  May also be specified for particular "
       "category, E.g., 5:Server=7 use 7 for Server, 5 for others\n")
      ("log-path", po::value<std::string>()->default_value("/dev/stderr"),
       "Redirect logging information to a file instead of standard error")
      ("heartbeat-interval",
       po::value<double>()->default_value(cpfs::kDefaultHeartbeatInterval),
       (boost::format("The heartbeat interval. Default: %.2f seconds")
            % cpfs::kDefaultHeartbeatInterval).str().c_str())
      ("socket-read-timeout",
       po::value<double>()->default_value(cpfs::kDefaultSocketReadTimeout),
       (boost::format("The socket read timeout. Default: %.2f seconds")
            % cpfs::kDefaultSocketReadTimeout).str().c_str())
      ("force-start", po::value<bool>()->default_value(false),
       "Whether to start MS forcibly when CPFS is not fully ready");
  po::options_description all("All options");
  all.add(desc);
  all.add_options()
      ("command", po::value< std::vector<std::string> >(),
        "Command to run (interactive if not specified)");
  po::positional_options_description p;
  p.add("command", -1);
  po::store(po::command_line_parser(argc, argv)
            .options(all).positional(p).run(), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::stringstream desc_ss;
    desc.print(desc_ss);
    std::fprintf(
        stderr, "CPFS Command Line administration interface\n\n"
        "Run a CLI command if provided, or prompt for commands repeatedly.\n"
        "The 'help' command gives a list of possible commands.\n"
        "\n"
        "Usage: %s [options] [command]\n\n", argv[0]);
    std::fprintf(stderr, "%s", desc_ss.str().c_str());
    return false;
  }

  std::string meta_server;
  if (vm.count("meta-server")) {
    meta_server = vm["meta-server"].as<std::string>();
  } else {
    const char cmd[] = ". /etc/default/cpfs-meta > /dev/null 2>&1 "
                       "&& echo -n \"${METADATA_SERVER}\"";
    FILE* cmd_f = popen(cmd, "r");
    if (cmd_f) {
      char output[4096];
      if (fgets(output, sizeof(output), cmd_f) && feof(cmd_f))
        meta_server = std::string(output);
      pclose(cmd_f);
    }
    if (meta_server.empty()) {
      std::fprintf(stderr, "Please specify --meta-server option or "
                           "set METADATA_SERVER in /etc/default/cpfs-meta\n");
      return false;
    }
  }
  boost::split(configs->meta_servers, meta_server, boost::is_any_of(","));
  configs->log_severity = vm["log-level"].as<std::string>();
  configs->log_path = vm["log-path"].as<std::string>();
  configs->heartbeat_interval = vm["heartbeat-interval"].as<double>();
  configs->socket_read_timeout = vm["socket-read-timeout"].as<double>();
  configs->force_start = vm["force-start"].as<bool>();
  if (vm.count("command")) {
    configs->command = boost::algorithm::join(
        vm["command"].as< std::vector<std::string> >(), " ");
  } else {
    configs->command = "";
  }
  return true;
}

const PAN_CHAR_T PANTHEIOS_FE_PROCESS_IDENTITY[] =
    "CPFS";  /**< ID for logging */

/**
 * Main entry point.
 */
int main(int argc, char* argv[]) {
  setpriority(PRIO_PROCESS, 0, -10);
  try {
    cpfs::SetLogPath("/dev/stderr");
    cpfs::AdminConfigItems configs;
    if (!LoadClientOpts(argc, argv, &configs))
      return 1;
    cpfs::SetLogPath(configs.log_path);
    cpfs::SetSeverityCeiling(configs.log_severity);
    boost::scoped_ptr<cpfs::IConsole> console(
        cpfs::MakeConsole(stdin, stdout, stderr));
    boost::scoped_ptr<cpfs::client::ICpfsAdmin> admin(
        cpfs::client::MakeCpfsAdmin(cpfs::client::MakeAdminClient(configs)));
    boost::scoped_ptr<cpfs::client::ICpfsCLI> cli(
        cpfs::client::MakeCpfsCLI(admin.get(), console.get()));
    if (cli->Init(configs.force_start)) {
      cli->Run(configs.command);
    } else {
      std::fprintf(stderr, "Cannot connect to the meta server(s)");
    }
  } catch (const std::exception& ex) {
    LOG(error, Server, ex.what());
  }
  return 0;
}
