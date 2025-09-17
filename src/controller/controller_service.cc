#include "streamit/controller/controller_service.h"
#include "proto/streamit.pb.h"
#include "streamit/common/status.h"

namespace streamit::controller {

ControllerServiceImpl::ControllerServiceImpl(std::shared_ptr<TopicManager> topic_manager)
    : topic_manager_(std::move(topic_manager)) {
}

grpc::Status ControllerServiceImpl::CreateTopic(grpc::ServerContext* context,
                                                const streamit::v1::CreateTopicRequest* request,
                                                streamit::v1::CreateTopicResponse* response) {
  // Validate request
  auto validation_status = ValidateCreateTopicRequest(request);
  if (!validation_status.ok()) {
    return validation_status;
  }

  // Create topic
  auto create_result =
      topic_manager_->CreateTopic(request->topic(), request->partitions(), request->replication_factor());

  if (!create_result.ok()) {
    response->set_success(false);
    response->set_error_message(create_result.status().message());
    return grpc::Status::OK;
  }

  response->set_success(true);
  return grpc::Status::OK;
}

grpc::Status ControllerServiceImpl::DescribeTopic(grpc::ServerContext* context,
                                                  const streamit::v1::DescribeTopicRequest* request,
                                                  streamit::v1::DescribeTopicResponse* response) {
  // Validate request
  auto validation_status = ValidateDescribeTopicRequest(request);
  if (!validation_status.ok()) {
    return validation_status;
  }

  // Get topic information
  auto topic_result = topic_manager_->GetTopic(request->topic());
  if (!topic_result.ok()) {
    response->set_error_code(streamit::v1::NOT_FOUND);
    response->set_error_message(topic_result.status().message());
    return grpc::Status::OK;
  }

  const auto& topic_info = topic_result.value();

  // Set response metadata
  auto* metadata = response->mutable_metadata();
  metadata->set_topic(topic_info.name);
  metadata->set_partitions(topic_info.partition_infos.size());
  metadata->set_replication_factor(3); // Default for now

  for (const auto& partition_info : topic_info.partition_infos) {
    auto* proto_partition = metadata->add_partition_metadata();
    proto_partition->set_partition(partition_info.partition);
    proto_partition->set_leader(partition_info.leader);

    for (int32_t replica : partition_info.replicas) {
      proto_partition->add_replicas(replica);
      proto_partition->add_isr(replica); // For now, all replicas are in ISR
    }
  }

  response->set_error_code(streamit::v1::OK);
  return grpc::Status::OK;
}

grpc::Status ControllerServiceImpl::FindLeader(grpc::ServerContext* context,
                                               const streamit::v1::FindLeaderRequest* request,
                                               streamit::v1::FindLeaderResponse* response) {
  // Validate request
  auto validation_status = ValidateFindLeaderRequest(request);
  if (!validation_status.ok()) {
    return validation_status;
  }

  // Get topic information
  auto topic_result = topic_manager_->GetTopic(request->topic());
  if (!topic_result.ok()) {
    response->set_error_code(streamit::v1::NOT_FOUND);
    response->set_error_message("Topic not found: " + request->topic());
    return grpc::Status::OK;
  }

  const auto& topic_info = topic_result.value();

  // Find the partition
  auto partition_it = std::find_if(topic_info.partition_infos.begin(), topic_info.partition_infos.end(),
                                   [&](const auto& partition) { return partition.partition == request->partition(); });

  if (partition_it == topic_info.partition_infos.end()) {
    response->set_error_code(streamit::v1::NOT_FOUND);
    response->set_error_message("Partition not found: " + std::to_string(request->partition()));
    return grpc::Status::OK;
  }

  // Set leader information
  response->set_leader_broker_id(partition_it->leader);
  response->set_leader_host("localhost");                 // Default for now
  response->set_leader_port(8080 + partition_it->leader); // Default port calculation
  response->set_error_code(streamit::v1::OK);

  return grpc::Status::OK;
}

grpc::Status ControllerServiceImpl::ValidateCreateTopicRequest(const streamit::v1::CreateTopicRequest* request) const {
  if (request->topic().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Topic name cannot be empty");
  }

  if (request->partitions() <= 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Partitions must be positive");
  }

  if (request->replication_factor() <= 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Replication factor must be positive");
  }

  return grpc::Status::OK;
}

grpc::Status ControllerServiceImpl::ValidateDescribeTopicRequest(
    const streamit::v1::DescribeTopicRequest* request) const {
  if (request->topic().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Topic name cannot be empty");
  }

  return grpc::Status::OK;
}

grpc::Status ControllerServiceImpl::ValidateFindLeaderRequest(const streamit::v1::FindLeaderRequest* request) const {
  if (request->topic().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Topic name cannot be empty");
  }

  if (request->partition() < 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Partition must be non-negative");
  }

  return grpc::Status::OK;
}

ControllerServer::ControllerServer(const std::string& host, uint16_t port, std::shared_ptr<TopicManager> topic_manager)
    : host_(host), port_(port), topic_manager_(std::move(topic_manager)), running_(false) {
}

bool ControllerServer::Start() noexcept {
  try {
    service_ = std::make_unique<ControllerServiceImpl>(topic_manager_);

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

bool ControllerServer::Stop() noexcept {
  if (server_) {
    server_->Shutdown();
    running_.store(false);
    return true;
  }
  return false;
}

void ControllerServer::Wait() noexcept {
  if (server_) {
    server_->Wait();
  }
}

bool ControllerServer::IsRunning() const noexcept {
  return running_.load();
}

} // namespace streamit::controller
