#pragma once
/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines cpfs:MemberFimProcessor.
 */
#include <boost/bind.hpp>
#include <boost/unordered_map.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_processor.hpp"

namespace boost {

template <class Y> class shared_ptr;
template <typename Signature> class function;

}  // namespace boost

namespace cpfs {

class IFimSocket;

/**
 * An IFimProcessor which dispatch to member functions according to
 * Fim type.  Such member functions (handlers) should have two
 * arguments, one having the Fim type and one the FimSocket type, and
 * return a bool indicating if the Fim processing is completed by the
 * handler.
 *
 * @tparam TActual The actual FimProcessor type, which should be a
 * subclass of MemberFimProcessor.
 */
template <typename TActual>
class MemberFimProcessor : public IFimProcessor {
 public:
  bool Accept(const FIM_PTR<IFim>& fim) const {
    return handlers_.find(fim->type()) != handlers_.end();
  }

  bool Process(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket) {
    if (!Accept(fim))
      return false;
    return handlers_[fim->type()](this, fim, socket);
  }

  /**
   * Adds a handler for a particular Fim object class.
   *
   * @param handler The handler to add.
   */
  template<typename TFim>
  void AddHandler(bool (TActual::*handler)(
      const FIM_PTR<TFim>& fim,
      const boost::shared_ptr<IFimSocket>& fim_socket)) {
    handlers_[TFim::kFimType] = boost::bind(&FimHandlerCaller<TFim>,
                                            _1, handler, _2, _3);
  }

 private:
  boost::unordered_map<
    FimType,
    boost::function<
      bool(MemberFimProcessor* processor,
           const FIM_PTR<IFim>& fim,
           const boost::shared_ptr<IFimSocket>& socket)
    >
  > handlers_; /**< Store registered handlers */

  template<typename TFim>
  static bool FimHandlerCaller(
      MemberFimProcessor* processor,
      bool (TActual::*handler)(
          const FIM_PTR<TFim>& fim,
          const boost::shared_ptr<IFimSocket>& fim_socket),
      const FIM_PTR<IFim>& fim,
      const boost::shared_ptr<IFimSocket>& fim_socket) {
    TActual* actual = static_cast<TActual*>(processor);
    return (actual->*handler)(
        boost::static_pointer_cast<TFim>(fim), fim_socket);
  }
};

}  // namespace cpfs
