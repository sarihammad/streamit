#include "streamit/net/grpc_server.h"
#include <grpcpp/grpcpp.h>

namespace streamit::net {

GrpcServerBuilder::GrpcServerBuilder(const std::string& host, uint16_t port)
    : host_(host), port_(port), max_send_message_size_(4 * 1024 * 1024) // 4MB
      ,
      max_receive_message_size_(4 * 1024 * 1024) // 4MB
      ,
      keepalive_time_ms_(30000) // 30 seconds
      ,
      keepalive_timeout_ms_(5000) // 5 seconds
      ,
      keepalive_permit_without_calls_(true), max_connection_idle_ms_(300000) // 5 minutes
      ,
      max_connection_age_ms_(7200000) // 2 hours
      ,
      max_connection_age_grace_ms_(60000) // 1 minute
      ,
      num_threads_(4) {
}

GrpcServerBuilder& GrpcServerBuilder::AddService(grpc::Service* service) {
  services_.push_back(service);
  return *this;
}

GrpcServerBuilder& GrpcServerBuilder::SetMaxMessageSize(size_t max_send_size, size_t max_receive_size) {
  max_send_message_size_ = max_send_size;
  max_receive_message_size_ = max_receive_size;
  return *this;
}

GrpcServerBuilder& GrpcServerBuilder::SetKeepaliveOptions(int64_t keepalive_time_ms, int64_t keepalive_timeout_ms,
                                                          int64_t keepalive_permit_without_calls,
                                                          int64_t max_connection_idle_ms, int64_t max_connection_age_ms,
                                                          int64_t max_connection_age_grace_ms) {
  keepalive_time_ms_ = keepalive_time_ms;
  keepalive_timeout_ms_ = keepalive_timeout_ms;
  keepalive_permit_without_calls_ = keepalive_permit_without_calls;
  max_connection_idle_ms_ = max_connection_idle_ms;
  max_connection_age_ms_ = max_connection_age_ms;
  max_connection_age_grace_ms_ = max_connection_age_grace_ms;
  return *this;
}

GrpcServerBuilder& GrpcServerBuilder::SetThreadPoolSize(int num_threads) {
  num_threads_ = num_threads;
  return *this;
}

std::unique_ptr<grpc::Server> GrpcServerBuilder::Build() {
  grpc::ServerBuilder builder;

  // Set server address
  std::string server_address = host_ + ":" + std::to_string(port_);
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  // Register services
  for (auto* service : services_) {
    builder.RegisterService(service);
  }

  // Set message size limits
  grpc::ChannelArguments args;
  args.SetMaxSendMessageSize(max_send_message_size_);
  args.SetMaxReceiveMessageSize(max_receive_message_size_);
  builder.SetChannelArguments(args);

  // Set keepalive options
  grpc::ServerBuilder::Option option1(GRPC_ARG_KEEPALIVE_TIME_MS, keepalive_time_ms_);
  grpc::ServerBuilder::Option option2(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, keepalive_timeout_ms_);
  grpc::ServerBuilder::Option option3(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, keepalive_permit_without_calls_);
  grpc::ServerBuilder::Option option4(GRPC_ARG_MAX_CONNECTION_IDLE_MS, max_connection_idle_ms_);
  grpc::ServerBuilder::Option option5(GRPC_ARG_MAX_CONNECTION_AGE_MS, max_connection_age_ms_);
  grpc::ServerBuilder::Option option6(GRPC_ARG_MAX_CONNECTION_AGE_GRACE_MS, max_connection_age_grace_ms_);

  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, keepalive_time_ms_);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, keepalive_timeout_ms_);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, keepalive_permit_without_calls_);
  builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_IDLE_MS, max_connection_idle_ms_);
  builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_AGE_MS, max_connection_age_ms_);
  builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_AGE_GRACE_MS, max_connection_age_grace_ms_);

  // Set thread pool
  std::unique_ptr<grpc::ThreadPoolInterface> thread_pool = std::make_unique<grpc::ThreadPool>(num_threads_);
  builder.SetThreadPool(thread_pool.release());

  return builder.BuildAndStart();
}

GrpcServer::GrpcServer(std::unique_ptr<grpc::Server> server) : server_(std::move(server)), running_(false) {
  if (server_) {
    address_ = "localhost"; // Simplified - in practice would extract from server
  }
}

bool GrpcServer::Start() noexcept {
  if (!server_) {
    return false;
  }

  running_.store(true);
  return true;
}

bool GrpcServer::Stop() noexcept {
  if (server_) {
    server_->Shutdown();
    running_.store(false);
    return true;
  }
  return false;
}

void GrpcServer::Wait() noexcept {
  if (server_) {
    server_->Wait();
  }
}

bool GrpcServer::IsRunning() const noexcept {
  return running_.load();
}

std::string GrpcServer::GetAddress() const noexcept {
  return address_;
}

} // namespace streamit::net
