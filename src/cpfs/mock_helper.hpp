#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Create mock methods.
 */

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/control/iif.hpp>
#include <boost/preprocessor/facilities/is_empty.hpp>
#include <boost/preprocessor/punctuation/comma_if.hpp>
#include <boost/preprocessor/seq/enum.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/seq/for_each_i.hpp>
#include <boost/preprocessor/seq/size.hpp>
#include <boost/preprocessor/seq/to_tuple.hpp>
#ifndef BOOST_PP_SEQ_ENUM_0
/**
 * Make BOOST_PP_SEQ_TO_TUPLE work for empty sequence.
 */
#define BOOST_PP_SEQ_ENUM_0
#endif
#include <boost/shared_ptr.hpp>

#include <gmock/gmock.h>

/**
 * Call macro within BOOST_PP_SEQ_FOR_EACH, which has an extra
 * argument r as the next nesting level.
 */
#define MOCK_HELPER_CALL_MACRO(r, macro, args) macro args

/**
 * Create a mock method generator macro name.  E.g.,
 *
 * MOCK_HELPER_METHOD_GEN(3, _T)
 * => MOCK_METHOD3_T
 */
#define MOCK_HELPER_METHOD_GEN(nargs, suffix, modifier)                 \
  BOOST_PP_CAT(BOOST_PP_CAT(MOCK_,                                      \
                            BOOST_PP_IIF(BOOST_PP_IS_EMPTY(modifier),,  \
                                         BOOST_PP_CAT(modifier, _))),   \
               BOOST_PP_CAT(BOOST_PP_CAT(METHOD, nargs), suffix))

/**
 * Create a mock method definition.  The rtype may be parenthesized,
 * so that comma within rtype can be expressed.  The var-arg argument
 * is a modifier, if exists it should always be CONST.  E.g.,
 *
 * MOCK_HELPER_MAKE_METHOD(foo, int, (int)(double))
 * => MOCK_METHOD2(foo, int(int, double));
 *
 * MOCK_HELPER_MAKE_METHOD(foo, int, (int)(double), CONST)
 * => MOCK_CONST_METHOD2(foo, int(int, double));
 */
#define MOCK_HELPER_MAKE_METHOD(mname, rtype, args, ...)                \
  MOCK_HELPER_METHOD_GEN(BOOST_PP_SEQ_SIZE(args), , __VA_ARGS__)        \
  (mname, rtype BOOST_PP_SEQ_TO_TUPLE(args));

/**
 * Create a mock method definition, version for class templates.  The
 * rtype may be parenthesized, so that comma within rtype can be
 * expressed.  The var-arg argument is a modifier, if exists it should
 * always be CONST.  E.g.,
 *
 * MOCK_HELPER_MAKE_METHOD(foo, int, (int)(double))
 * => MOCK_METHOD2_T(foo, int(int, double));
 *
 * MOCK_HELPER_MAKE_METHOD(foo, int, (int)(double), CONST)
 * => MOCK_CONST_METHOD2_T(foo, int(int, double));
 */
#define MOCK_HELPER_MAKE_METHOD_T(mname, rtype, args, ...)              \
  MOCK_HELPER_METHOD_GEN(BOOST_PP_SEQ_SIZE(args), _T, __VA_ARGS__)      \
  (mname, rtype BOOST_PP_SEQ_TO_TUPLE(args));

/**
 * Create mock method definition of a list of descriptions.
 */
#define MAKE_MOCK_METHODS(descs)                                        \
  public:                                                               \
  BOOST_PP_SEQ_FOR_EACH(MOCK_HELPER_CALL_MACRO, MOCK_HELPER_MAKE_METHOD, descs)

/**
 * Create mock method definition of a list of descriptions, version
 * for class templates.
 */
#define MAKE_MOCK_METHODS_T(descs)                                      \
  public:                                                               \
  BOOST_PP_SEQ_FOR_EACH(MOCK_HELPER_CALL_MACRO,                         \
                        MOCK_HELPER_MAKE_METHOD_T, descs)


/**
 * Create a formal argument named argN with the specific type,
 * preceded by comma if i > 0.
 */
#define MOCK_HELPER_FORMAL(r, data, i, type) BOOST_PP_COMMA_IF(i) type arg##i

/**
 * Create an actual argument named argN, preceded by comma if i > 0.
 */
#define MOCK_HELPER_ACTUAL(r, data, i, type) BOOST_PP_COMMA_IF(i) arg##i

/**
 * Create a forwarder function.  E.g.,
 *
 * MOCK_HELPER_MAKE_FORWARDER(foo, int, (int)(double))
 * => int foo(int arg1, double arg2) { return impl_->foo(arg1, arg2); }
 */
#define MOCK_HELPER_MAKE_FORWARDER(mname, rtype, args, ...)             \
  rtype mname(BOOST_PP_SEQ_FOR_EACH_I(MOCK_HELPER_FORMAL, _, args))     \
      BOOST_PP_IIF(BOOST_PP_IS_EMPTY(__VA_ARGS__), , const) {           \
    return impl_->mname(BOOST_PP_SEQ_FOR_EACH_I(MOCK_HELPER_ACTUAL,     \
                                                _, args));              \
  }

/**
 * Create forwarder class, which should always be called the same name
 * as the forwarded class with an F suffix.  Forwarder classes are
 * copyable and constructable and convertable from a shared pointer of
 * the forwarded class.  E.g.,
 *
 * MAKE_MOCK_FORWARDER(Foo, descs)
 */
#define MAKE_MOCK_FORWARDER(cls, descs)                                 \
  public:                                                               \
  cls ## F (boost::shared_ptr<cls> impl) : impl_(impl) {}               \
  BOOST_PP_SEQ_FOR_EACH(MOCK_HELPER_CALL_MACRO,                         \
                        MOCK_HELPER_MAKE_FORWARDER, descs)              \
  private:                                                              \
  boost::shared_ptr<cls> impl_;

/**
 * Create forwarder class template, which should always be called the
 * same name as the forwarded class with an F suffix.  Forwarder
 * classes templates are copyable and constructable and convertable
 * from a shared pointer of the forwarded class.
 *
 * MAKE_MOCK_FORWARDER(Foo, descs, (TBar)(CHello))
 */
#define MAKE_MOCK_FORWARDER_T(cls, descs, tparams)                      \
  public:                                                               \
  typedef cls<BOOST_PP_SEQ_ENUM(tparams)> TMockHelperForwarded;         \
  cls ## F (boost::shared_ptr<TMockHelperForwarded> impl) : impl_(impl) {} \
  BOOST_PP_SEQ_FOR_EACH(MOCK_HELPER_CALL_MACRO,                         \
                        MOCK_HELPER_MAKE_FORWARDER, descs)              \
  private:                                                              \
  boost::shared_ptr<TMockHelperForwarded> impl_;
