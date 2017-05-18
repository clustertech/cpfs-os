/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of CpfsCLI
 */
#include "cpfs_cli.hpp"

#include <cstddef>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>

#include "admin_info.hpp"
#include "console.hpp"
#include "client/cpfs_admin.hpp"

namespace cpfs {
namespace client {
namespace {

const double kConnectTimeout = 2.0;  /**< MS connection timeout */

/** Help message within CLI */
const char* kHelpMessage =
    "The following commands are supported:\n"
    "\n"
    "  status\n"
    "      Show status of the CPFS nodes\n"
    "  info\n"
    "      Show disk space utilization of CPFS\n"
    "  config list\n"
    "      List all configurable config items and values\n"
    "  config set <target node> <config> <value>\n"
    "      Change config of the target node, i.e. \"MS1\", \"DS 0-1\"\n"
    "      to a new value.  The following <config> are supported:\n"
    "          log_severity   Set logging level (0-7)\n"
    "          log_path       Set log path\n"
    "          num_ds_groups  Set number of DS groups\n"
    "  system shutdown\n"
    "      Halt the whole CPFS system\n"
    "  exit\n"
    "      Exit from this interface\n";

/**
 * Define the CpfsCLI for command line interface
 */
class CpfsCLI : public ICpfsCLI {
 public:
  /**
   * Create the CpfsCLI
   *
   * @param admin The CPFS admin
   *
   * @param console The console
   */
  explicit CpfsCLI(ICpfsAdmin* admin, IConsole* console)
      : admin_(admin), console_(console) {}

  bool Init(bool force_start) {
    console_->PrintLine("Connecting to CPFS");
    std::vector<bool> reject_info;
    if (admin_->Init(kConnectTimeout, &reject_info)) {
      console_->PrintLine("Connected to CPFS");
      return true;
    }
    for (unsigned ms_no = 0; ms_no < reject_info.size(); ++ms_no) {
      if (reject_info[ms_no]) {
        console_->PrintLine("Connection to MS" +
                            boost::lexical_cast<std::string>(ms_no + 1) +
                            " rejected");
        if (force_start) {
          admin_->ForceStart(ms_no);
          console_->PrintLine("System is started forcibly");
          return true;
        }
      }
    }
    console_->PrintLine("Connection to CPFS failed");
    return false;
  }

  void Run(std::string command) {
    if (command.empty()) {
      while (true)
        if (!RunCmd(console_->ReadLine()))
          break;
    } else {
      RunCmd(command);
    }
  }

  /**
   * Run a command and return whether it indicates interactive loop exit.
   *
   * @param input The command
   *
   * @return Whether the interactive loop should terminate
   */
  bool RunCmd(std::string input) {
    std::vector<std::string> parsed;
    boost::split(parsed, input, boost::is_any_of(" "),
                 boost::algorithm::token_compress_on);
    if (parsed[0] == "status") {
      HandleQueryStatus(
          std::vector<std::string>(parsed.begin() + 1, parsed.end()));
    } else if (parsed[0] == "info") {
      HandleQueryInfo(
          std::vector<std::string>(parsed.begin() + 1, parsed.end()));
    } else if (parsed[0] == "config") {
      if (parsed.size() < 2) {
        console_->PrintLine("Usage: config [list|set]");
        return true;
      }
      if (parsed[1] == "list") {
        HandleConfigList(
            std::vector<std::string>(parsed.begin() + 1, parsed.end()));
      } else if (parsed[1] == "set") {
        bool success = false;
        do {
          if (parsed.size() < 5)
            break;
          std::string target = parsed[2];
          boost::to_upper(target);
          if (target.substr(0, 2) != "MS" && target != "DS")
            break;
          if (target == "DS" && parsed.size() < 6)
            break;
          if (target == "DS")
            HandleConfigChange(target + parsed[3], parsed[4], parsed[5]);
          else
            HandleConfigChange(target, parsed[3], parsed[4]);
          success = true;
        } while (0);
        if (!success)
          console_->PrintLine("Usage: config set [target] [config] [value]");
      }
    } else if (parsed[0] == "system") {
      if (parsed.size() < 2) {
        console_->PrintLine("Usage: system [shutdown|restart]");
        return true;
      }
      if (parsed[1] == "shutdown") {
        HandleSystemShutdown();
        return false;
      }
    } else if (parsed[0] == "exit") {
      return false;
    } else if (parsed[0] == "help") {
      console_->PrintLine(kHelpMessage);
    } else {
      console_->PrintLine(
          (boost::format("Unknown command '%s'") % parsed[0]).str());
    }
    return true;
  }

  bool HandleQueryStatus(const std::vector<std::string>& nodes) {
    (void) nodes;
    cpfs::client::ClusterInfo cluster_info = admin_->QueryStatus();
    console_->PrintLine((boost::format("%s: %s")
        % cluster_info.ms_role % cluster_info.ms_state).str());
    for (std::size_t i = 0; i < cluster_info.dsg_states.size(); ++i) {
      console_->PrintLine((boost::format("DSG%d: %s")
          % i % cluster_info.dsg_states[i]).str());
    }
    std::vector<std::vector<std::string> > table;
    std::vector<std::string> headers;
    headers.push_back("Name");
    headers.push_back("IP");
    headers.push_back("Port");
    headers.push_back("PID");
    table.push_back(headers);
    for (std::size_t i = 0; i < cluster_info.node_infos.size(); ++i) {
      std::map<std::string, std::string>& info = cluster_info.node_infos[i];
      std::vector<std::string> columns;
      columns.push_back(info["Alias"]);
      columns.push_back(info["IP"]);
      columns.push_back(info["Port"]);
      columns.push_back(info["PID"]);
      table.push_back(columns);
    }
    console_->PrintTable(table);
    return true;
  }

  bool HandleQueryInfo(const std::vector<std::string>& nodes) {
    (void) nodes;
    std::vector<std::vector<std::string> > table;
    std::vector<std::string> headers;
    headers.push_back("Name");
    headers.push_back("Total");
    headers.push_back("Free");
    table.push_back(headers);
    cpfs::client::DiskInfoList disk_info_list = admin_->QueryDiskInfo();
    for (std::size_t i = 0; i < disk_info_list.size(); ++i) {
      std::map<std::string, std::string>& info = disk_info_list[i];
      std::vector<std::string> columns;
      columns.push_back(info["Alias"]);
      columns.push_back(info["Total"]);
      columns.push_back(info["Free"]);
      table.push_back(columns);
    }
    console_->PrintTable(table);
    return true;
  }

  bool HandleConfigList(const std::vector<std::string>& configs) {
    (void) configs;
    std::vector<std::vector<std::string> > table;
    cpfs::client::ConfigList config_list = admin_->ListConfig();
    for (std::size_t i = 0; i < config_list.size(); ++i) {
      if (config_list[i].first == kNodeCfg) {
        if (!table.empty()) {
          console_->PrintLine("");
          console_->PrintTable(table);
          table.clear();
        }
        std::vector<std::string> headers;
        headers.push_back(config_list[i].second);
        headers.push_back("Value");
        table.push_back(headers);
        continue;
      }
      std::vector<std::string> columns;
      columns.push_back(config_list[i].first);
      columns.push_back(config_list[i].second);
      table.push_back(columns);
    }
    if (!table.empty()) {
      console_->PrintLine("");
      console_->PrintTable(table);
    }
    return true;
  }

  bool HandleConfigChange(const std::string& target,
                          const std::string& key,
                          const std::string& value) {
    return admin_->ChangeConfig(target, key, value);
  }

  bool HandleSystemShutdown() {
    return admin_->SystemShutdown();
  }

 private:
  ICpfsAdmin* admin_;
  IConsole* console_;
};

}  // namespace

ICpfsCLI* MakeCpfsCLI(ICpfsAdmin* admin, IConsole* console) {
  return new CpfsCLI(admin, console);
}

}  // namespace client
}  // namespace cpfs
