#include "streamit/common/config.h"
#include "streamit/controller/controller_service.h"
#include <iostream>
#include <signal.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace {
std::unique_ptr<streamit::controller::ControllerServer> g_server;

void SignalHandler(int signal) {
  if (g_server) {
    spdlog::info("Received signal {}, shutting down...", signal);
    g_server->Stop();
  }
}

void SetupLogging(const std::string& level) {
  auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("streamit-controller", console_sink);

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

} // namespace

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
    return 1;
  }

  try {
    // Load configuration
    auto config = streamit::common::ConfigLoader::LoadControllerConfig(argv[1]);

    // Setup logging
    SetupLogging(config.log_level);

    spdlog::info("Starting StreamIt controller {} on {}:{}", config.id, config.host, config.port);

    // Create topic manager
    auto topic_manager = std::make_shared<streamit::controller::TopicManager>();

    // Load existing topics from config
    auto topics = streamit::common::ConfigLoader::LoadTopicConfigs(config.config_file);
    for (const auto& topic_config : topics) {
      auto create_result =
          topic_manager->CreateTopic(topic_config.name, topic_config.partitions, topic_config.replication_factor);
      if (create_result.ok()) {
        spdlog::info("Loaded topic: {} with {} partitions", topic_config.name, topic_config.partitions);
      } else {
        spdlog::warn("Failed to load topic {}: {}", topic_config.name, create_result.status().message());
      }
    }

    // Create and start server
    g_server = std::make_unique<streamit::controller::ControllerServer>(config.host, config.port, topic_manager);

    if (!g_server->Start()) {
      spdlog::error("Failed to start controller server");
      return 1;
    }

    spdlog::info("Controller server started successfully");

    // Setup signal handlers
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);

    // Wait for server to finish
    g_server->Wait();

    spdlog::info("Controller server stopped");
    return 0;

  } catch (const std::exception& e) {
    spdlog::error("Fatal error: {}", e.what());
    return 1;
  }
}
