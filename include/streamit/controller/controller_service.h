#pragma once

#include "streamit/controller/topic_manager.h"
#include "streamit/proto/streamit.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>

namespace streamit::controller {

// Controller service implementation
class ControllerServiceImpl final : public streamit::v1::Controller::Service {
public:
  // Constructor
  ControllerServiceImpl(std::shared_ptr<TopicManager> topic_manager);

  // CreateTopic RPC implementation
  grpc::Status CreateTopic(grpc::ServerContext* context, const streamit::v1::CreateTopicRequest* request,
                           streamit::v1::CreateTopicResponse* response) override;

  // DescribeTopic RPC implementation
  grpc::Status DescribeTopic(grpc::ServerContext* context, const streamit::v1::DescribeTopicRequest* request,
                             streamit::v1::DescribeTopicResponse* response) override;

  // FindLeader RPC implementation
  grpc::Status FindLeader(grpc::ServerContext* context, const streamit::v1::FindLeaderRequest* request,
                          streamit::v1::FindLeaderResponse* response) override;

private:
  std::shared_ptr<TopicManager> topic_manager_;
  mutable std::mutex mutex_;

  // Helper to validate create topic request
  [[nodiscard]] grpc::Status ValidateCreateTopicRequest(const streamit::v1::CreateTopicRequest* request) const;

  // Helper to validate describe topic request
  [[nodiscard]] grpc::Status ValidateDescribeTopicRequest(const streamit::v1::DescribeTopicRequest* request) const;

  // Helper to validate find leader request
  [[nodiscard]] grpc::Status ValidateFindLeaderRequest(const streamit::v1::FindLeaderRequest* request) const;
};

// Controller server management
class ControllerServer {
public:
  // Constructor
  ControllerServer(const std::string& host, uint16_t port, std::shared_ptr<TopicManager> topic_manager);

  // Start the server
  [[nodiscard]] bool Start() noexcept;

  // Stop the server
  [[nodiscard]] bool Stop() noexcept;

  // Wait for server to finish
  void Wait() noexcept;

  // Check if server is running
  [[nodiscard]] bool IsRunning() const noexcept;

private:
  std::string host_;
  uint16_t port_;
  std::shared_ptr<TopicManager> topic_manager_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<ControllerServiceImpl> service_;
  std::atomic<bool> running_;
};

} // namespace streamit::controller
