#include "streamit/broker/broker_service.h"
#include "streamit/storage/log_dir.h"
#include "streamit/common/config.h"
#include "streamit/common/signal_shutdown.h"
#include "streamit/common/tracing.h"
#include "streamit/common/health_check.h"
#include "streamit/common/http_health_server.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <iostream>
#include <thread>

namespace {
std::unique_ptr<streamit::broker::BrokerServer> g_server;
std::unique_ptr<streamit::common::HttpHealthServer> g_health_server;

void ShutdownCallback() {
  if (g_server) {
    spdlog::info("Shutdown requested, stopping server...");
    g_server->Stop();
  }
  if (g_health_server) {
    g_health_server->Stop();
  }
}

void SetupLogging(const std::string& level) {
  auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("streamit", console_sink);
  
  if (level == "debug") {
    logger->set_level(spdlog::level::debug);
  } else if (level == "info") {
    logger->set_level(spdlog::level::info);
  } else if (level == "warn") {
    logger->set_level(spdlog::level::warn);
  } else if (level == "error") {
    logger->set_level(spdlog::level::err);
  } else {
    logger->set_level(spdlog::level::info);
  }
  
  spdlog::set_default_logger(logger);
}

}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
    return 1;
  }
  
  try {
    // Load configuration
    auto config = streamit::common::ConfigLoader::LoadBrokerConfig(argv[1]);
    
    // Setup structured logging
    streamit::common::StructuredLogger::Initialize(config.log_level);
    
    spdlog::info("Starting StreamIt broker {} on {}:{}", 
                 config.id, config.host, config.port);
    
    // Create log directory
    auto log_dir = std::make_shared<streamit::storage::LogDir>(
      config.log_dir, config.max_segment_size_bytes);
    
    // Create idempotency table
    auto idempotency_table = std::make_shared<streamit::broker::IdempotencyTable>();
    
    // Create and start server
    g_server = std::make_unique<streamit::broker::BrokerServer>(
      config.host, config.port, log_dir, idempotency_table);
    
    if (!g_server->Start()) {
      spdlog::error("Failed to start broker server");
      return 1;
    }
    
    spdlog::info("Broker server started successfully");
    
    // Setup health checks
    auto health_manager = std::make_shared<streamit::common::HealthCheckManager>();
    
    // Add storage health check
    health_manager->AddCheck("storage", [log_dir]() {
      // Simple check - try to get a segment
      auto result = log_dir->GetSegment("health_check", 0);
      if (result.ok()) {
        return streamit::common::HealthCheckResult(streamit::common::HealthStatus::HEALTHY, "Storage OK");
      } else {
        return streamit::common::HealthCheckResult(streamit::common::HealthStatus::UNHEALTHY, 
          "Storage error: " + result.status().message());
      }
    });
    
    // Start health check server
    g_health_server = std::make_unique<streamit::common::HttpHealthServer>(
      "0.0.0.0", 8081, health_manager);
    
    if (!g_health_server->Start()) {
      spdlog::warn("Failed to start health check server");
    } else {
      spdlog::info("Health check server started on port 8081");
    }
    
    // Setup signal handlers
    streamit::common::SignalHandler::Install();
    streamit::common::SignalHandler::SetShutdownCallback(ShutdownCallback);
    
    // Use std::jthread for clean shutdown
    std::jthread server_thread([&](std::stop_token stop_token) {
      while (!stop_token.stop_requested() && !streamit::common::SignalHandler::IsShutdownRequested()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      
      if (g_server) {
        spdlog::info("Stopping server due to shutdown request...");
        g_server->Stop();
      }
    });
    
    // Wait for server to finish
    g_server->Wait();
    
    // Request stop and wait for thread
    server_thread.request_stop();
    server_thread.join();
    
    spdlog::info("Broker server stopped");
    return 0;
    
  } catch (const std::exception& e) {
    spdlog::error("Fatal error: {}", e.what());
    return 1;
  }
}

