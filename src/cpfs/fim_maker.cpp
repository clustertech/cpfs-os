/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define implementation of GetFimsMaker().
 */

// Be careful not to #include "fims.hpp" indirectly until the end
// of this file.
#include "fim_maker.hpp"

#include <string>

namespace cpfs {

namespace {

/**
 * An internal class template to register a Fim class to the global
 * FimMaker when instantiated for that class.
 */
template <typename TFim>
class FimMakerRegAgent {
 public:
  /**
   * @param name The type name of the Fim class being registered
   */
  explicit FimMakerRegAgent(const char* name) {
    std::string tname = name;
    if (tname.size() >= 3U && tname.substr(tname.size() - 3) == "Fim")
      tname = tname.substr(0, tname.size() - 3);
    GetFimsMaker().RegisterClass<TFim>(tname);
  }
};

}  // namespace

FimMaker& GetFimsMaker() {
  static FimMaker inst;
  return inst;
}

}  // namespace cpfs

/**
 * Hook to instantiate the FimMakerRegAgent to register the class in
 * the Fim maker.  Called from fims.hpp.
 *
 * @param cls The Fim class to instantiate FimMakerRegAgent for
 */
#define FIMS_DEF_HOOK(cls)                                              \
  namespace {                                                           \
  /** Registration agent for name */                                    \
  FimMakerRegAgent<cls> k ## cls ## RegAgent(#cls);                     \
  }  // namespace

// The following causes FimMakerRegAgent to be instantiated for all
// Fim classes
#include "fims.hpp"  // IWYU pragma: keep
