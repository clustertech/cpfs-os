#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define ConfigMgr for central retrival and store of configs.
 */

#include <string>
#include <vector>

#include "admin_info.hpp"
#include "logger.hpp"
#include "mutex_util.hpp"
#include "util.hpp"

namespace cpfs {

/**
 * Configuration manager for storing and retriving configs. Most configs
 * are expected to be set only once during initialization.
 *
 * Configs modifiable during run-time are protected with mutex
 */
class ConfigMgr {
 public:
  /**
   * Set the host for MS1
   */
  void set_ms1_host(const std::string& ms1_host) {
    ms1_host_ = ms1_host;
  }

  /**
   * Get the host for MS1
   */
  std::string ms1_host() const {
    return ms1_host_;
  }

  /**
   * Set the port for MS1
   */
  void set_ms1_port(int ms1_port) {
    ms1_port_ = ms1_port;
  }

  /**
   * Get the port for MS1
   */
  int ms1_port() const {
    return ms1_port_;
  }

  /**
   * Set the host for MS2
   */
  void set_ms2_host(const std::string& ms2_host) {
    ms2_host_ = ms2_host;
  }

  /**
   * Get the host for MS2
   */
  std::string ms2_host() const {
    return ms2_host_;
  }

  /**
   * Set the port for MS2
   */
  void set_ms2_port(int ms2_port) {
    ms2_port_ = ms2_port;
  }

  /**
   * Get the port for MS2
   */
  int ms2_port() const {
    return ms2_port_;
  }

  /**
   * Set the role
   */
  void set_role(const std::string& role) {
    role_ = role;
  }

  /**
   * Get the role
   */
  std::string role() const {
    return role_;
  }

  /**
   * Set the DS host
   */
  void set_ds_host(const std::string& ds_host) {
    ds_host_ = ds_host;
  }

  /**
   * Get the DS host
   */
  std::string ds_host() const {
    return ds_host_;
  }

  /**
   * Set the DS port
   */
  void set_ds_port(int ds_port) {
    ds_port_ = ds_port;
  }

  /**
   * Get the DS port
   */
  int ds_port() const {
    return ds_port_;
  }

  /**
   * Set the path of the data directory
   */
  void set_data_dir(const std::string& data_dir) {
    data_dir_ = data_dir;
  }

  /**
   * Get the path of the data directory
   */
  std::string data_dir() const {
    return data_dir_;
  }

  /**
   * Set the log severity
   */
  void set_log_severity(const std::string& log_severity) {
    MUTEX_LOCK_GUARD(*data_mutex_);
    SetSeverityCeiling(log_severity);
    log_severity_ = log_severity;
  }

  /**
   * Get the log severity
   */
  std::string log_severity() const {
    MUTEX_LOCK_GUARD(*data_mutex_);
    return log_severity_;
  }

  /**
   * Set the path of the logging file
   */
  void set_log_path(const std::string& log_path) {
    MUTEX_LOCK_GUARD(*data_mutex_);
    SetLogPath(log_path);
    log_path_ = log_path;
  }

  /**
   * Get the path of the logging file
   */
  std::string log_path() const {
    MUTEX_LOCK_GUARD(*data_mutex_);
    return log_path_;
  }

  /**
   * Set whether MS permission is used
   */
  void set_ms_perms(bool ms_perms) {
    ms_perms_ = ms_perms;
  }

  /**
   * Get whether MS permission is used
   */
  bool ms_perms() const {
    return ms_perms_;
  }

  /**
   * Set whether daemonize is used
   */
  void set_daemonize(bool daemonize) {
    daemonize_ = daemonize;
  }

  /**
   * Get whether daemonize is used
   */
  bool daemonize() const {
    return daemonize_;
  }

  /**
   * Set the path of the PID file
   */
  void set_pidfile(const std::string& pidfile) {
    pidfile_ = pidfile;
  }

  /**
   * Get the path of the PID file
   */
  std::string pidfile() const {
    return pidfile_;
  }

  /**
   * Set heartbeat interval
   */
  void set_heartbeat_interval(double heartbeat_interval) {
    heartbeat_interval_ = heartbeat_interval;
  }

  /**
   * Get heartbeat interval
   */
  double heartbeat_interval() const {
    return heartbeat_interval_;
  }

  /**
   * Set socket read timeout
   */
  void set_socket_read_timeout(double socket_read_timeout) {
    socket_read_timeout_ = socket_read_timeout;
  }

  /**
   * Get socket read timeout
   */
  double socket_read_timeout() const {
    return socket_read_timeout_;
  }

  /**
   * Get the list of runtime configurable config items
   */
  std::vector<ConfigItem> List() const {
    std::vector<ConfigItem> ret;
    ret.push_back(ConfigItem("log_severity", log_severity()));
    ret.push_back(ConfigItem("log_path", log_path()));
    return ret;
  }

  ConfigMgr() : ms1_port_(0), ms2_port_(0),
                ds_port_(0), num_groups_(1),
                ms_perms_(false), daemonize_(false),
                heartbeat_interval_(0), socket_read_timeout_(0) {}

 private:
  SkipCopy<boost::mutex> data_mutex_; /**< Protect fields below */
  std::string ms1_host_;
  int ms1_port_;
  std::string ms2_host_;
  int ms2_port_;
  std::string role_;
  std::string ds_host_;
  int ds_port_;
  int num_groups_;
  std::string data_dir_;
  std::string log_severity_;
  std::string log_path_;
  bool ms_perms_;
  bool daemonize_;
  std::string pidfile_;
  double heartbeat_interval_;
  double socket_read_timeout_;
};

}  // namespace cpfs
