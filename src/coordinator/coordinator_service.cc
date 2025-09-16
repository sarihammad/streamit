#include "streamit/coordinator/coordinator_service.h"
#include "streamit/common/status.h"

namespace streamit::coordinator {

CoordinatorServiceImpl::CoordinatorServiceImpl(std::shared_ptr<ConsumerGroupManager> group_manager)
  : group_manager_(std::move(group_manager)) {}

grpc::Status CoordinatorServiceImpl::CommitOffset(grpc::ServerContext* context,
                                                 const streamit::v1::CommitOffsetRequest* request,
                                                 streamit::v1::CommitOffsetResponse* response) {
  // Validate request
  auto validation_status = ValidateCommitOffsetRequest(request);
  if (!validation_status.ok()) {
    return validation_status;
  }
  
  // Commit offset
  auto commit_result = group_manager_->CommitOffset(
    request->group(), request->topic(), request->partition(), request->offset());
  
  if (!commit_result.ok()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, commit_result.status().message());
  }
  
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::PollAssignment(grpc::ServerContext* context,
                                                   const streamit::v1::PollAssignmentRequest* request,
                                                   streamit::v1::PollAssignmentResponse* response) {
  // Validate request
  auto validation_status = ValidatePollAssignmentRequest(request);
  if (!validation_status.ok()) {
    return validation_status;
  }
  
  // Join group
  auto join_result = group_manager_->JoinGroup(
    request->group(), request->member_id(), 
    std::vector<std::string>(request->topics().begin(), request->topics().end()));
  
  if (!join_result.ok()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, join_result.status().message());
  }
  
  // Send heartbeat
  auto heartbeat_result = group_manager_->Heartbeat(request->group(), request->member_id());
  if (!heartbeat_result.ok()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, heartbeat_result.status().message());
  }
  
  // Get assignments
  auto assignments_result = group_manager_->GetAssignments(request->group(), request->member_id());
  if (!assignments_result.ok()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, assignments_result.status().message());
  }
  
  const auto& assignments = assignments_result.value();
  
  // Convert to protobuf format
  for (const auto& assignment : assignments) {
    auto* proto_assignment = response->add_assignments();
    proto_assignment->set_topic(assignment.topic);
    for (int32_t partition : assignment.partitions) {
      proto_assignment->add_partitions(partition);
    }
  }
  
  // Set heartbeat interval
  response->set_heartbeat_interval_ms(10000); // 10 seconds
  
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::ValidateCommitOffsetRequest(
    const streamit::v1::CommitOffsetRequest* request) const {
  if (request->group().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Group cannot be empty");
  }
  
  if (request->topic().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Topic cannot be empty");
  }
  
  if (request->partition() < 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Partition must be non-negative");
  }
  
  if (request->offset() < 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Offset must be non-negative");
  }
  
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::ValidatePollAssignmentRequest(
    const streamit::v1::PollAssignmentRequest* request) const {
  if (request->group().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Group cannot be empty");
  }
  
  if (request->member_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Member ID cannot be empty");
  }
  
  if (request->topics().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Topics cannot be empty");
  }
  
  return grpc::Status::OK;
}

CoordinatorServer::CoordinatorServer(const std::string& host, uint16_t port,
                                    std::shared_ptr<ConsumerGroupManager> group_manager)
  : host_(host)
  , port_(port)
  , group_manager_(std::move(group_manager))
  , running_(false) {}

bool CoordinatorServer::Start() noexcept {
  try {
    service_ = std::make_unique<CoordinatorServiceImpl>(group_manager_);
    
    grpc::ServerBuilder builder;
    std::string server_address = host_ + ":" + std::to_string(port_);
    
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service_.get());
    
    server_ = builder.BuildAndStart();
    if (!server_) {
      return false;
    }
    
    running_.store(true);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool CoordinatorServer::Stop() noexcept {
  if (server_) {
    server_->Shutdown();
    running_.store(false);
    return true;
  }
  return false;
}

void CoordinatorServer::Wait() noexcept {
  if (server_) {
    server_->Wait();
  }
}

bool CoordinatorServer::IsRunning() const noexcept {
  return running_.load();
}

}

