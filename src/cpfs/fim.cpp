/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement common functions for the IFimSocket interface.
 */

#include "fim.hpp"

#include <string>

#include <boost/format.hpp>

#include "fim_maker.hpp"

namespace cpfs {

std::ostream& operator<<(std::ostream& ost, const IFim& fim) {
  char r = 'Q';
  if (fim.IsReply())
    r = fim.is_final() ? 'F' : 'P';
  ClientNum cn = fim.req_id() >> 44;
  ReqId lr = fim.req_id() & ((1ULL << 44) - 1);
  ost << boost::format("#%|1|%|05x|-%|06x|(%||)") % r % cn % lr
      % GetFimsMaker().TypeName(fim.type());
  return ost;
}

}  // namespace cpfs
