#include "streamit/common/config.h"
#include "streamit/coordinator/coordinator_service.h"
#include <iostream>
#include <signal.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <thread>

namespace {
std::unique_ptr<streamit::coordinator::CoordinatorServer> g_server;
std::unique_ptr<streamit::coordinator::ConsumerGroupManager> g_group_manager;

void SignalHandler(int signal) {
  if (g_server) {
    spdlog::info("Received signal {}, shutting down...", signal);
    g_server->Stop();
  }
}

void SetupLogging(const std::string& level) {
  auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("streamit-coordinator", console_sink);

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

void CleanupTask() {
  while (g_server && g_server->IsRunning()) {
    if (g_group_manager) {
      g_group_manager->CleanupInactiveMembers();
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
  }
}

} // namespace

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
    return 1;
  }

  try {
    // Load configuration
    auto config = streamit::common::ConfigLoader::LoadCoordinatorConfig(argv[1]);

    // Setup logging
    SetupLogging(config.log_level);

    spdlog::info("Starting StreamIt coordinator {} on {}:{}", config.id, config.host, config.port);

    // Create consumer group manager
    g_group_manager = std::make_unique<streamit::coordinator::ConsumerGroupManager>(config.heartbeat_interval_ms,
                                                                                    config.session_timeout_ms);

    // Create and start server
    g_server =
        std::make_unique<streamit::coordinator::CoordinatorServer>(config.host, config.port, g_group_manager.get());

    if (!g_server->Start()) {
      spdlog::error("Failed to start coordinator server");
      return 1;
    }

    spdlog::info("Coordinator server started successfully");

    // Start cleanup task
    std::thread cleanup_thread(CleanupTask);

    // Setup signal handlers
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);

    // Wait for server to finish
    g_server->Wait();

    // Wait for cleanup thread
    cleanup_thread.join();

    spdlog::info("Coordinator server stopped");
    return 0;

  } catch (const std::exception& e) {
    spdlog::error("Fatal error: {}", e.what());
    return 1;
  }
}
