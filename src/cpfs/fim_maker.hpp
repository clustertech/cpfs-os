#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define a FimMaker class for making Fim objects of classes defined
 * from raw bytes.  Provide an instance for making Fim objects defined
 * in fims.hpp.
 */

#include <cstring>
#include <stdexcept>
#include <string>

#include <boost/format.hpp>
#include <boost/function.hpp>
// IWYU pragma: no_forward_declare boost::function
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"

namespace cpfs {

/**
 * Makes Fim objects given a message.  It constructs an object of a
 * Fim object class according to the Fim type stored within the
 * message.
 */
class FimMaker {
 public:
  /**
   * Registers a class to be handled by the maker.  Note that the
   * class to register is in the template argument, and there is no
   * run-time argument of the method.
   *
   * @tparam TFim The Fim class.
   */
  template<typename TFim>
  void RegisterClass(std::string name) {
    from_head_funcs_[TFim::kFimType] = &TFim::FromHead;
    type_names_[TFim::kFimType] = name;
  }

  /**
   * Creates a Fim object by copying from a FIM header.
   *
   * @param msg The FIM header.
   */
  FIM_PTR<IFim> FromHead(const char* msg) {
    const FimHead* head = reinterpret_cast<const FimHead*>(msg);
    FimType type = head->type & ~0x8000;
    if (from_head_funcs_.find(type) == from_head_funcs_.end())
      throw std::out_of_range((boost::format("Unregistered Fim type %d")
                               % head->type).str());
    return from_head_funcs_[type](msg);
  }

  /**
   * @param type A Fim type
   *
   * @return The name of the type
   */
  std::string TypeName(FimType type) {
    boost::unordered_map<FimType, std::string>::iterator it =
        type_names_.find(type);
    if (it == type_names_.end())
      return (boost::format("%||") % type).str();
    return it->second;
  }

  /**
   * Clone a Fim object by copying from a FIM
   *
   * @param fim The fim to copy from
   */
  FIM_PTR<IFim> Clone(const FIM_PTR<IFim>& fim) {
    FIM_PTR<IFim> copied = FromHead(fim->msg());
    std::memcpy(copied->body(), fim->body(), fim->body_size());
    return copied;
  }

 private:
  boost::unordered_map<FimType,
                       boost::function<FIM_PTR<IFim>(const char*)> >
  from_raw_funcs_;
  boost::unordered_map<FimType,
                       boost::function<FIM_PTR<IFim>(const char*)> >
  from_head_funcs_;
  boost::unordered_map<FimType, std::string> type_names_;
};

/**
 * Get the global singleton FimMaker.
 */
FimMaker& GetFimsMaker();

}  // namespace cpfs
