/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of the cpfs::ListeningSocket for meta / data server
 */
#include "listening_socket_impl.hpp"

#include <memory>
#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/functional/factory.hpp>
#include <boost/functional/forward_adapter.hpp>
#include <boost/scoped_ptr.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "listening_socket.hpp"
#include "logger.hpp"

namespace boost { template <class Y> class shared_ptr; }

namespace cpfs {

namespace {

/**
 * ListeningSocket class for meta / data server
 */
class ListeningSocket : public IListeningSocket {
 public:
  /**
   * Create the ListeningSocket that listens on the endpoint
   */
  explicit ListeningSocket(IAsioPolicy* policy) : policy_(policy) {}

  /**
   * Listen for connections
   */
  void Listen(const TcpEndpoint& bind_addr, AcceptCallback callback) {
    callback_ = callback;
    bind_addr_ = bind_addr;
    acceptor_.reset(policy_->MakeAcceptor(bind_addr));
    DoAccept();
  }

 protected:
  /**
   * Acception connections from client
   */
  void DoAccept() {
    socket_.reset(new TcpSocket(*policy_->io_service()));
    policy_->AsyncAccept(
        acceptor_.get(),
        socket_.get(),
        boost::bind(&ListeningSocket::HandleAccept,
                    this,
                    boost::asio::placeholders::error));
  }

  /**
   * Handle connection accept
   */
  void HandleAccept(const boost::system::error_code& error) {
    if (!error) {
      callback_(socket_.release());
    } else {
      HandleError(error.message());
    }
    DoAccept();
  }

  /**
   * Handle connection error
   *
   * @param msg The error message
   */
  void HandleError(const std::string& msg) {
    LOG(error, Fim, "Error in connection accept: ", msg,
        ", Bind Address: ", PVal(bind_addr_));
  }

 private:
  IAsioPolicy* policy_;
  TcpEndpoint bind_addr_;
  boost::scoped_ptr<TcpAcceptor> acceptor_;
  UNIQUE_PTR<TcpSocket> socket_;
  AcceptCallback callback_;
};

}  // namespace

ListeningSocketMaker kListeningSocketMaker = boost::forward_adapter<
  boost::factory<boost::shared_ptr<ListeningSocket> > >();

}  // namespace cpfs
