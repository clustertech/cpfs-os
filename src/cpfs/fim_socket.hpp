#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines cpfs::IFimSocket.
 */

#include <ostream>
#include <string>

#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>

#include "asio_common.hpp"
#include "fim.hpp"

namespace boost {

template <class Y> class shared_ptr;

}  // namespace boost

namespace cpfs {

class IAsioPolicy;
class IFimProcessor;
class IPeriodicTimer;
class IReqTracker;

/**
 * Callback function to call when FimSocket is disconnected from peer
 */
typedef boost::function<void()> FimSocketCleanupCallback;

/**
 * Send and receive Fims via TcpSockets.  For receiving, a timer can
 * be associated so that the connection is terminated if no message is
 * received for the period.  For sending, a timer can be associated so
 * that a HeartbeatFim is generated if no other messages need to be
 * sent for a period of time.
 *
 * Fim sockets knows the request tracker of itself, so that the
 * BaseFimProcessor can associate the request tracker needed for
 * request processing.  We assume that the request tracker, once set,
 * will not be set again (so that it is (thread-)safe for
 * GetReqTracker() to return a const ref).  This also aids other
 * operations like automatic unsetting of Fim socket in request
 * trackers.
 *
 * FimSocket is thread-safe, in the sense that multiple threads can
 * concurrently use it to send messages, and one thread may be using
 * it when another is setting its properties.  Only the thread running
 * the io service should ever process a message read from the socket.
 *
 * When error occurs, the FimSocket object shuts itself down.  The
 * shutdown procedures can also be triggered externally.  During
 * shutdown, it is removed from request trackers.  That is done after
 * a previously set cleanup callback is called, which can do other
 * cleanup tasks before the request tracker is unset.  Then the
 * underlying socket is closed, so that Boost.Asio will stop
 * operations in progress.  The FimSocket will only be destructed when
 * the last Boost.Asio operation on the socket is completed.
 */
class IFimSocket : public boost::enable_shared_from_this<IFimSocket> {
 public:
  virtual ~IFimSocket() {}

  /**
   * @return The Asio policy for the socket
   */
  virtual IAsioPolicy* asio_policy() const = 0;

  /**
   * @return The underlying TcpSocket
   */
  virtual TcpSocket* socket() const = 0;

  /**
   * @return The name of the request tracker associated with the socket
   */
  virtual std::string name() const = 0;

  /**
   * @return The ip:port of the remote endpoint
   */
  virtual std::string remote_info() const = 0;

  /**
   * Set the FimProcessor to handle Fims.
   *
   * @param fim_processor The FimProcessor
   */
  virtual void SetFimProcessor(IFimProcessor* fim_processor) = 0;

  /**
   * Start reading FIM messages
   */
  virtual void StartRead() = 0;

  /**
   * Set timer for heartbeat loss detection
   *
   * @param timer The periodic timer
   */
  virtual void SetIdleTimer(
      const boost::shared_ptr<IPeriodicTimer>& timer) = 0;

  /**
   * Send a FIM message
   *
   * @param write_msg The FIM to send
   */
  virtual void WriteMsg(const FIM_PTR<IFim>& write_msg) = 0;

  /**
   * Check whether there is pending write
   */
  virtual bool pending_write() const = 0;

  /**
   * Set timer for heartbeat generation
   *
   * @param timer The periodic timer
   */
  virtual void SetHeartbeatTimer(
      const boost::shared_ptr<IPeriodicTimer>& timer) = 0;

  /**
   * Set the request tracker.  This function is normally called only
   * by the tracker mapper, and only once for each socket.
   *
   * @param req_tracker The request tracker to set for the Fim processor
   */
  virtual void SetReqTracker(
      const boost::shared_ptr<IReqTracker>& req_tracker) = 0;

  /**
   * Get the request tracker set for the FimSocket.  The returned
   * pointer is valid for as long as the FimSocket is alive.
   *
   * @return The request tracker as set in SetReqTracker
   */
  virtual IReqTracker* GetReqTracker() const = 0;

  /**
   * Migrate to a different AsioPolicy.  The user must ensure that the
   * socket does not have any pending read or write when calling this.
   *
   * @param policy The new AsioPolicy to migrate to
   */
  virtual void Migrate(IAsioPolicy* policy) = 0;

  /**
   * Remove internal references in the FimSocket so that it can be
   * destroyed.  It is called automatically by Shutdown().  This
   * removes the heartbeat and idle timer, as well as other internal
   * structures.
   */
  virtual void Reset() = 0;

  /**
   * Shutdown the FimSocket.  It is legal to call only if the calling
   * thread does not hold any mutex.
   */
  virtual void Shutdown() = 0;

  /**
   * Return whether Shutdown() has been called.
   */
  virtual bool ShutdownCalled() = 0;

  /**
   * Set the cleanup callback
   */
  virtual void OnCleanup(FimSocketCleanupCallback callback) = 0;
};

/**
 * Type for changing the behavior of creating IFimSocket instances.
 * The first is the TcpSocket to wrap as a FimSocket.  The second
 * parameter is the IAsioPolicy used to perform async operations.  It
 * is possible to create an empty socket, by passing null as the first
 * argument.  In this case the socket is created in a shutdown'ed
 * state.
 */
typedef boost::function<
  boost::shared_ptr<IFimSocket>(
      TcpSocket* socket,
      IAsioPolicy* policy)> FimSocketMaker;

/**
 * Print a FimSocket to a stream.
 *
 * @param ost The stream to print to
 *
 * @param socket The FimSocket to print
 */
std::ostream& operator<<(std::ostream& ost, const IFimSocket& socket);

}  // namespace cpfs
