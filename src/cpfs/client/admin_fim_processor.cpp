/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Implement the AdminFimProcessor.
 */
#include "client/admin_fim_processor.hpp"

#include <boost/shared_ptr.hpp>

#include "common.hpp"
#include "fim.hpp"
#include "fim_socket.hpp"
#include "fims.hpp"
#include "member_fim_processor.hpp"
#include "tracker_mapper.hpp"
#include "client/base_client.hpp"
#include "client/conn_mgr.hpp"

namespace cpfs {

class IFimProcessor;

namespace client {
namespace {

/**
 * Implement the AdminFimProcessor
 */
class AdminFimProcessor : public MemberFimProcessor<AdminFimProcessor> {
 public:
  /**
   * @param client The client
   */
  explicit AdminFimProcessor(BaseAdminClient* client) : client_(client) {
    AddHandler(&AdminFimProcessor::HandleFCMSRegSuccess);
    AddHandler(&AdminFimProcessor::HandleMSRegRejected);
    AddHandler(&AdminFimProcessor::HandleTopologyChange);
    AddHandler(&AdminFimProcessor::HandleDSGStateChange);
  }

 private:
  BaseAdminClient* client_;

  bool HandleFCMSRegSuccess(const FIM_PTR<FCMSRegSuccessFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    // Set to inode allocator
    client_->set_client_num((*fim)->client_num);
    ITrackerMapper* tracker_mapper = client_->tracker_mapper();
    tracker_mapper->SetClientNum((*fim)->client_num);
    tracker_mapper->SetMSFimSocket(socket, true);
    client_->SetReady(true);
    return true;
  }

  bool HandleMSRegRejected(const FIM_PTR<MSRegRejectedFim>& fim,
                           const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    socket->Shutdown();
    client_->conn_mgr()->ForgetMS(socket, true);
    return true;
  }

  bool HandleTopologyChange(const FIM_PTR<TopologyChangeFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) socket;
    if ((*fim)->type == 'D') {
      GroupId group = (*fim)->ds_group;
      GroupRole role = (*fim)->ds_role;
      if ((*fim)->joined) {
        client_->conn_mgr()->ConnectDS((*fim)->ip, (*fim)->port, group, role);
      } else {
        boost::shared_ptr<IFimSocket> ds_socket =
            client_->tracker_mapper()->GetDSFimSocket(group, role);
        if (ds_socket) {
          ds_socket->Shutdown();
        }
      }
    }
    return true;
  }

  bool HandleDSGStateChange(const FIM_PTR<DSGStateChangeFim>& fim,
                            const boost::shared_ptr<IFimSocket>& socket) {
    (void) fim;
    (void) socket;
    return true;
  }
};

}  // namespace

IFimProcessor* MakeAdminFimProcessor(BaseAdminClient* client) {
  return new AdminFimProcessor(client);
}

}  // namespace client
}  // namespace cpfs
