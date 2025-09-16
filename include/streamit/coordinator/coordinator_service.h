#pragma once

#include "streamit/coordinator/consumer_group_manager.h"
#include "streamit/proto/streamit.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <mutex>

namespace streamit::coordinator {

// Coordinator service implementation
class CoordinatorServiceImpl final : public streamit::v1::Coordinator::Service {
public:
  // Constructor
  CoordinatorServiceImpl(std::shared_ptr<ConsumerGroupManager> group_manager);
  
  // CommitOffset RPC implementation
  grpc::Status CommitOffset(grpc::ServerContext* context,
                           const streamit::v1::CommitOffsetRequest* request,
                           streamit::v1::CommitOffsetResponse* response) override;
  
  // PollAssignment RPC implementation
  grpc::Status PollAssignment(grpc::ServerContext* context,
                             const streamit::v1::PollAssignmentRequest* request,
                             streamit::v1::PollAssignmentResponse* response) override;

private:
  std::shared_ptr<ConsumerGroupManager> group_manager_;
  mutable std::mutex mutex_;
  
  // Helper to validate commit offset request
  [[nodiscard]] grpc::Status ValidateCommitOffsetRequest(
    const streamit::v1::CommitOffsetRequest* request) const;
  
  // Helper to validate poll assignment request
  [[nodiscard]] grpc::Status ValidatePollAssignmentRequest(
    const streamit::v1::PollAssignmentRequest* request) const;
};

// Coordinator server management
class CoordinatorServer {
public:
  // Constructor
  CoordinatorServer(const std::string& host, uint16_t port,
                   std::shared_ptr<ConsumerGroupManager> group_manager);
  
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
  std::shared_ptr<ConsumerGroupManager> group_manager_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<CoordinatorServiceImpl> service_;
  std::atomic<bool> running_;
};

}

