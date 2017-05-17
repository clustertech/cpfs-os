#pragma once

/* Copyright 2013 ClusterTech Ltd */

#include "mock_helper.hpp"
#include "server/ms/meta_dir_reader.hpp"  // IWYU pragma: export

namespace cpfs {
namespace server {
namespace ms {

#define OBJ_METHODS                                                     \
  ((ToInodeFim, FIM_PTR<ResyncInodeFim>,                                \
    (const std::string&)))                                              \
  ((ToDentryFim, FIM_PTR<ResyncDentryFim>,                              \
    (const std::string&)(const std::string&)(char)))

class MockIMetaDirReader : public IMetaDirReader {
  MAKE_MOCK_METHODS(OBJ_METHODS);
};

#undef OBJ_METHODS

}  // namespace ms
}  // namespace server
}  // namespace cpfs
