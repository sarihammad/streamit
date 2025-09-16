#include "streamit/common/health_check.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <iostream>
#include <sstream>

namespace streamit::common {

void HealthCheckManager::AddCheck(const std::string& name, HealthCheckFunction check) {
  checks_[name] = std::move(check);
}

HealthCheckResult HealthCheckManager::RunChecks() const {
  if (checks_.empty()) {
    return HealthCheckResult(HealthStatus::UNKNOWN, "No health checks configured");
  }
  
  for (const auto& [name, check] : checks_) {
    auto result = check();
    if (result.status != HealthStatus::HEALTHY) {
      return HealthCheckResult(result.status, 
        "Check '" + name + "' failed: " + result.message);
    }
  }
  
  return HealthCheckResult(HealthStatus::HEALTHY, "All checks passed");
}

HealthCheckResult HealthCheckManager::RunCheck(const std::string& name) const {
  auto it = checks_.find(name);
  if (it == checks_.end()) {
    return HealthCheckResult(HealthStatus::UNKNOWN, "Check not found: " + name);
  }
  
  return it->second();
}

std::vector<std::string> HealthCheckManager::GetCheckNames() const {
  std::vector<std::string> names;
  for (const auto& [name, _] : checks_) {
    names.push_back(name);
  }
  return names;
}

HealthCheckServer::HealthCheckServer(const std::string& host, uint16_t port, 
                                   std::shared_ptr<HealthCheckManager> manager)
  : host_(host)
  , port_(port)
  , manager_(std::move(manager))
  , running_(false) {}

bool HealthCheckServer::Start() noexcept {
  try {
    grpc::EnableDefaultHealthCheckService(true);
    
    grpc::ServerBuilder builder;
    std::string server_address = host_ + ":" + std::to_string(port_);
    
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    
    // Register health check service
    grpc::health::v1::Health::AsyncService health_service;
    builder.RegisterService(&health_service);
    
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

bool HealthCheckServer::Stop() noexcept {
  if (server_) {
    server_->Shutdown();
    running_.store(false);
    return true;
  }
  return false;
}

bool HealthCheckServer::IsRunning() const noexcept {
  return running_.load();
}

} // namespace streamit::common
