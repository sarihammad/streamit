#pragma once

#include <string>
#include <functional>
#include <memory>
#include <grpcpp/grpcpp.h>

namespace streamit::common {

// Health check status
enum class HealthStatus {
  HEALTHY,
  UNHEALTHY,
  UNKNOWN
};

// Health check result
struct HealthCheckResult {
  HealthStatus status;
  std::string message;
  
  HealthCheckResult(HealthStatus s, const std::string& m) : status(s), message(m) {}
};

// Health check function type
using HealthCheckFunction = std::function<HealthCheckResult()>;

// Health check manager
class HealthCheckManager {
public:
  // Add a health check
  void AddCheck(const std::string& name, HealthCheckFunction check);
  
  // Run all health checks
  [[nodiscard]] HealthCheckResult RunChecks() const;
  
  // Run a specific health check
  [[nodiscard]] HealthCheckResult RunCheck(const std::string& name) const;
  
  // Get all check names
  [[nodiscard]] std::vector<std::string> GetCheckNames() const;

private:
  std::map<std::string, HealthCheckFunction> checks_;
};

// HTTP health check server
class HealthCheckServer {
public:
  // Constructor
  HealthCheckServer(const std::string& host, uint16_t port, 
                   std::shared_ptr<HealthCheckManager> manager);
  
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
  std::unique_ptr<grpc::Server> server_;
  std::atomic<bool> running_;
  
  // Handle HTTP requests
  void HandleRequest(const std::string& path, const std::string& response);
};

} // namespace streamit::common
