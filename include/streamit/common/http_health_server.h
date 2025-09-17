#pragma once

#include "streamit/common/health_check.h"
#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace streamit::common {

// Simple HTTP health check server
class HttpHealthServer {
public:
  // Constructor
  HttpHealthServer(const std::string& host, uint16_t port, std::shared_ptr<HealthCheckManager> manager);

  // Destructor
  ~HttpHealthServer();

  // Start the server
  [[nodiscard]] bool Start() noexcept;

  // Stop the server
  [[nodiscard]] bool Stop() noexcept;

  // Check if running
  [[nodiscard]] bool IsRunning() const noexcept;

private:
  std::string host_;
  uint16_t port_;
  std::shared_ptr<HealthCheckManager> manager_;
  std::atomic<bool> running_;
  std::unique_ptr<std::thread> server_thread_;

  // Server loop
  void ServerLoop();

  // Handle HTTP request
  void HandleRequest(int client_socket);

  // Send HTTP response
  void SendResponse(int client_socket, int status_code, const std::string& body);
};

} // namespace streamit::common
