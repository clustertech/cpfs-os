/* Copyright 2014 ClusterTech Ltd */

/**
 * @file
 *
 * Implementation of Authenticator.
 */

#include "authenticator.hpp"

#include <sys/time.h>

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <list>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "asio_common.hpp"
#include "asio_policy.hpp"
#include "common.hpp"
#include "crypto.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "logger.hpp"
#include "member_fim_processor.hpp"
#include "mutex_util.hpp"
#include "periodic_timer.hpp"
#include "util.hpp"

namespace cpfs {
namespace {

const std::size_t kNumAuth = 2;  /**< Number of nonces and endpoints */

/**
 * Structure for tracking authentication info of a FimSocket
 */
struct AuthInfo {
  std::vector<Nonce> nonces;  /**< The local and remote nonces */
  std::vector<TcpEndpoint> endpoints; /**< The local and remote endpoint */
  AuthHandler handler;  /**< Handler to call once authenticated */
  struct timespec begin_time;  /**< The time authentication begins */

  /**
   * Create the AuthInfo
   */
  AuthInfo() : nonces(kNumAuth, 0), endpoints(kNumAuth) {
    clock_gettime(CLOCK_MONOTONIC, &begin_time);
  }

  /**
   * Whether the nonces are initialized and valid
   */
  bool filled() const {
    return nonces[0] != nonces[1] && nonces[0] != 0 && nonces[1] != 0;
  }

  /**
   * Convert nonces to binary data
   *
   * @param mode Mode 1 to get from the beginning, otherwise it is from end
   *
   * @return Return the nonces in binary
   */
  std::string to_binary(int mode) const {
    std::stringstream ss;
    if (mode == 1)
      ss << nonces[0] << nonces[1] << endpoints[0];
    else
      ss << nonces[1] << nonces[0] << endpoints[1];
    return ss.str();
  }
};

/**
 * Implementation of the Authenticator. Connector uses this class to perform
 * perform mutual authentication by FIM exchanges in two stages. The tail
 * buffer of these FIM stores the challenge response (Encrypt with shared key)
 */
class Authenticator
    : public IAuthenticator, private MemberFimProcessor<Authenticator> {
  friend class MemberFimProcessor<Authenticator>;
 public:
  /** Type for mapping FimSocket to AuthInfo */
  typedef boost::unordered_map<boost::shared_ptr<IFimSocket>, AuthInfo> AuthMap;

  /**
   * Create the authenticator
   *
   * @param crypto The crypto
   *
   * @param asio_policy The policy
   */
  explicit Authenticator(ICrypto* crypto, IAsioPolicy* asio_policy)
      : key_(std::string(32, 'a')), crypto_(crypto),
        data_mutex_(MUTEX_INIT), asio_policy_(asio_policy) {
    AddHandler(&Authenticator::HandleAuthMsg1);
    AddHandler(&Authenticator::HandleAuthMsg2);
    AddHandler(&Authenticator::HandleAuthMsg3);
  }

  void Init() {
    timer_ =
        timer_maker_(asio_policy_->io_service(), timeout_);
    timer_->OnTimeout(boost::bind(&Authenticator::Expire, this, timeout_));
    // Load secret key
    char key_data[kEncryptKeySize * 2 + 1] = {'\0'};
    const char* key_path = std::getenv("CPFS_KEY_PATH");
    key_path = key_path ? key_path : CPFS_KEY_PATH;
    FILE* fp = fopen(key_path, "r");
    if (!fp)
      throw std::runtime_error("Cannot open " + std::string(key_path));
    fread(key_data, 1, kEncryptKeySize * 2, fp);
    fclose(fp);
    key_ = std::string(key_data);
  }

  void AsyncAuth(const boost::shared_ptr<IFimSocket>& socket,
                 AuthHandler handler, bool initiator) {
    MUTEX_LOCK_GUARD(data_mutex_);
    auth_[socket].handler = handler;
    auth_[socket].nonces[0] = crypto_->CreateNonce();
    auth_[socket].endpoints[0] =
        asio_policy_->GetLocalEndpoint(socket->socket());
    socket->SetFimProcessor(this);
    socket->StartRead();
    if (initiator) {
      FIM_PTR<AuthMsg1Fim> fim = AuthMsg1Fim::MakePtr();
      (*fim)->nonce = auth_[socket].nonces[0];
      socket->WriteMsg(fim);
    }
  }

  void SetPeriodicTimerMaker(PeriodicTimerMaker maker, unsigned timeout) {
    timer_maker_ = maker;
    timeout_ = timeout;
  }

 private:
  std::string key_;  // The key loaded from file
  boost::scoped_ptr<ICrypto> crypto_;  // The crypto library
  mutable MUTEX_TYPE data_mutex_;  // Protect everything below
  AuthMap auth_;  // Map of socket info pending for authentication
  IAsioPolicy* asio_policy_;  // The policy
  PeriodicTimerMaker timer_maker_;  // Maker for expiring socket
  boost::shared_ptr<IPeriodicTimer> timer_;  // The timer
  unsigned timeout_;  // Timeout for unauthenticated socket

  /**
   * Listener responses to challenge from initiator
   */
  bool HandleAuthMsg1(const FIM_PTR<AuthMsg1Fim>& fim,
                      const boost::shared_ptr<IFimSocket>& socket) {
    MUTEX_LOCK_GUARD(data_mutex_);
    auth_[socket].nonces[1] = (*fim)->nonce;
    auth_[socket].endpoints[1] =
        asio_policy_->GetRemoteEndpoint(socket->socket());
    FIM_PTR<AuthMsg2Fim> reply = AuthMsg2Fim::MakePtr(kChallengeSize);
    (*reply)->nonce = auth_[socket].nonces[0];
    crypto_->Sign(key_, auth_[socket].to_binary(1), reply->tail_buf());
    socket->WriteMsg(reply);
    return true;
  }

  /**
   * Connector verifies challenge reply from listener and generates a
   * challenge reply to the listener
   */
  bool HandleAuthMsg2(const FIM_PTR<AuthMsg2Fim>& fim,
                      const boost::shared_ptr<IFimSocket>& socket) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    auth_[socket].nonces[1] = (*fim)->nonce;
    auth_[socket].endpoints[1] =
        asio_policy_->GetRemoteEndpoint(socket->socket());
    char buffer[kChallengeSize];
    crypto_->Sign(key_, auth_[socket].to_binary(2), buffer);
    if (!Verify_(fim, socket, buffer, &lock))
      return true;
    FIM_PTR<AuthMsg3Fim> reply = AuthMsg3Fim::MakePtr(kChallengeSize);
    crypto_->Sign(key_, auth_[socket].to_binary(1), reply->tail_buf());
    socket->WriteMsg(reply);
    CompleteAuth_(socket, &lock);
    return true;
  }

  /**
   * Listener verifies the challenge reply and completes authentication
   */
  bool HandleAuthMsg3(const FIM_PTR<AuthMsg3Fim>& fim,
                      const boost::shared_ptr<IFimSocket>& socket) {
    MUTEX_LOCK(boost::unique_lock, data_mutex_, lock);
    char buffer[kChallengeSize];
    crypto_->Sign(key_, auth_[socket].to_binary(2), buffer);
    if (!Verify_(fim, socket, buffer, &lock))
      return true;
    CompleteAuth_(socket, &lock);
    return true;
  }

  /**
   * Verify the challenge response against given buffer
   */
  bool Verify_(const FIM_PTR<IFim>& fim,
               const boost::shared_ptr<IFimSocket>& socket,
               const char buffer[],
               boost::unique_lock<MUTEX_TYPE>* lock) {
    if (!auth_[socket].filled())
      return false;
    if (fim->tail_buf_size() != kChallengeSize ||
        std::memcmp(fim->tail_buf(), buffer, kChallengeSize) != 0) {
      LOG(warning, Fim,
          "Security challenge from ", socket->remote_info(), " is corrupted. "
          "Connection rejected");
      auth_.erase(socket);
      lock->unlock();
      socket->Shutdown();
      return false;
    }
    return true;
  }

  /**
   * Invoke authentication completion handler and perform cleanup
   */
  void CompleteAuth_(const boost::shared_ptr<IFimSocket>& socket,
                     boost::unique_lock<MUTEX_TYPE>* lock) {
    AuthHandler handler = auth_[socket].handler;
    auth_.erase(socket);
    lock->unlock();
    handler();
  }

  /**
   * Expire unauthenticated sockets after timeout
   */
  bool Expire(unsigned age) {
    std::list<boost::shared_ptr<IFimSocket> > sockets;
    {
      MUTEX_LOCK_GUARD(data_mutex_);
      CompareTime ct;
      struct timespec max_time_added;
      clock_gettime(CLOCK_MONOTONIC, &max_time_added);
      max_time_added.tv_sec -= age;

      AuthMap::iterator itr = auth_.begin();
      while (itr != auth_.end()) {
        if (!ct(itr->second.begin_time, max_time_added))
          break;
        boost::shared_ptr<IFimSocket> socket = itr->first;
        LOG(warning, Fim,
            "Timeout expired for ", socket->remote_info(),
            " authentication dialog, connection dropped");
        sockets.push_back(socket);
        itr = auth_.erase(itr);
      }
    }
    BOOST_FOREACH(boost::shared_ptr<IFimSocket> socket, sockets) {
      socket->Shutdown();
    }
    return true;
  }
};

}  // namespace

IAuthenticator* MakeAuthenticator(ICrypto* crypto, IAsioPolicy* asio_policy) {
  return new Authenticator(crypto, asio_policy);
}

}  // namespace cpfs
