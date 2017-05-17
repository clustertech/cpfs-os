/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define utilities.
 */

#include "util.hpp"

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cmath>
#include <cstdio>
#include <string>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/format.hpp>
#include <boost/random/random_device.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace cpfs {

void XorBytes(char* target, const char* src, std::size_t size) {
  for (std::size_t i = 0; i < size; ++i)
    target[i] ^= src[i];
}

uint32_t IPToInt(const std::string& ip_str) {
  uint32_t addr;
  inet_pton(AF_INET, ip_str.c_str(), &addr);
  return addr;
}

std::string IntToIP(uint32_t ip_int) {
  char ip_str[INET_ADDRSTRLEN] = {'\0'};
  inet_ntop(AF_INET, &ip_int, ip_str, INET_ADDRSTRLEN);
  return std::string(ip_str);
}

boost::posix_time::time_duration ToTimeDuration(double t) {
  double sec, fsec;
  fsec = std::modf(t, &sec);
  return boost::posix_time::seconds(uint64_t(sec)) +
         boost::posix_time::microseconds(uint32_t(fsec * 1000000U));
}

bool IsWriteFlag(int flag) {
  int acc_mode = flag & O_ACCMODE;
  return acc_mode == O_WRONLY || acc_mode == O_RDWR;
}

std::string FormatDiskSize(uint64_t disk_size) {
  static const char* tags[] = {"Bytes", "KB", "MB", "GB", "TB", "PB"};
  static const int kNumTags = sizeof(tags) / sizeof(tags[0]);
  if (disk_size > 0) {
    uint64_t multiplier = 1;
    for (int index = 0; index < kNumTags; ++index, multiplier *= 1024) {
      // Multiplied by 16 to make noticable difference between
      // values like 1024 and 2047
      if (disk_size < multiplier * 1024 * 16 || index == kNumTags - 1) {
        if (index == 0)
          return (boost::format("%d %s") % disk_size % tags[index]).str();
        else
          return (boost::format("%.1f %s") %
                  (double(disk_size) / multiplier) % tags[index]).str();
      }
    }
  }
  return "0";
}

void WritePID(const std::string& pidfile) {
  FILE* fp = fopen(pidfile.c_str(), "w");
  if (!fp)
    return;
  fprintf(fp, "%d\n", getpid());
  fclose(fp);
}

std::string CreateUUID() {
  boost::random_device rng;
  boost::uuids::basic_random_generator<boost::random_device> uuid_gen(&rng);
  return boost::uuids::to_string(uuid_gen());
}

}  // namespace cpfs
