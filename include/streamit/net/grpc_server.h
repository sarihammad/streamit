#pragma once

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>

namespace streamit::net {

// gRPC server builder with common configuration
class GrpcServerBuilder {
public:
  // Constructor
  GrpcServerBuilder(const std::string& host, uint16_t port);
  
  // Add a service
  GrpcServerBuilder& AddService(grpc::Service* service);
  
  // Set maximum message size
  GrpcServerBuilder& SetMaxMessageSize(size_t max_send_size, size_t max_receive_size);
  
  // Set keepalive options
  GrpcServerBuilder& SetKeepaliveOptions(int64_t keepalive_time_ms,
                                        int64_t keepalive_timeout_ms,
                                        int64_t keepalive_permit_without_calls,
                                        int64_t max_connection_idle_ms,
                                        int64_t max_connection_age_ms,
                                        int64_t max_connection_age_grace_ms);
  
  // Set thread pool size
  GrpcServerBuilder& SetThreadPoolSize(int num_threads);
  
  // Build the server
  [[nodiscard]] std::unique_ptr<grpc::Server> Build();

private:
  std::string host_;
  uint16_t port_;
  std::vector<grpc::Service*> services_;
  size_t max_send_message_size_;
  size_t max_receive_message_size_;
  int64_t keepalive_time_ms_;
  int64_t keepalive_timeout_ms_;
  int64_t keepalive_permit_without_calls_;
  int64_t max_connection_idle_ms_;
  int64_t max_connection_age_ms_;
  int64_t max_connection_age_grace_ms_;
  int num_threads_;
};

// gRPC server wrapper with lifecycle management
class GrpcServer {
public:
  // Constructor
  GrpcServer(std::unique_ptr<grpc::Server> server);
  
  // Start the server
  [[nodiscard]] bool Start() noexcept;
  
  // Stop the server
  [[nodiscard]] bool Stop() noexcept;
  
  // Wait for server to finish
  void Wait() noexcept;
  
  // Check if server is running
  [[nodiscard]] bool IsRunning() const noexcept;
  
  // Get server address
  [[nodiscard]] std::string GetAddress() const noexcept;

private:
  std::unique_ptr<grpc::Server> server_;
  std::atomic<bool> running_;
  std::string address_;
};

}

