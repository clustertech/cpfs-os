#pragma once

/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Define Authenticator.
 */

#include <boost/function.hpp>

#include "fim.hpp"
#include "periodic_timer.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IFim;
class IFimSocket;

/** Type of callback to call once authentication is successful */
typedef boost::function<void()> AuthHandler;

/**
 * Perform mutual authentication with the connected peer. Fimsockets added
 * to this class will be authenticated and callback is called once completed.
 * If the authentication failed or is unable to complete within timeout,
 * the Fimsocket will be disconnected
 */
class IAuthenticator {
 public:
  virtual ~IAuthenticator() {}

  /**
   * Initialize the authenticator
   */
  virtual void Init() = 0;

  /**
   * Set PeriodicTimerMaker to use.  Must be called before calling
   * other methods.
   *
   * @param maker The PeriodicTimerMaker to use
   *
   * @param timeout The timeout for unauthenticated socket
   */
  virtual void SetPeriodicTimerMaker(
      PeriodicTimerMaker maker, unsigned timeout) = 0;

  /**
   * Start authenticating the specified socket
   *
   * @param socket The fim socket to authenticate
   *
   * @param handler The callback to call once authenticated
   *
   * @param initiator True if caller is the initiator of the authentication
   */
  virtual void AsyncAuth(
      const boost::shared_ptr<IFimSocket>& socket,
      AuthHandler handler, bool initiator) = 0;
};

}  // namespace cpfs
