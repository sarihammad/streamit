#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

namespace streamit::common {

// Broker configuration
struct BrokerConfig {
  std::string id;
  std::string host;
  uint16_t port;
  std::string log_dir;
  size_t max_segment_size_bytes = 128 * 1024 * 1024; // 128MB
  int64_t segment_roll_interval_ms = 3600000; // 1 hour
  size_t max_inflight_bytes = 100 * 1024 * 1024; // 100MB
  int32_t replication_factor = 1;
  int32_t min_insync_replicas = 1;
  int32_t request_timeout_ms = 30000;
  int32_t replication_timeout_ms = 10000;
  bool enable_metrics = true;
  uint16_t metrics_port = 8080;
  std::string log_level = "info";
};

// Controller configuration
struct ControllerConfig {
  std::string id;
  std::string host;
  uint16_t port;
  std::string config_file;
  int32_t heartbeat_interval_ms = 10000;
  int32_t session_timeout_ms = 30000;
  bool enable_metrics = true;
  uint16_t metrics_port = 8081;
  std::string log_level = "info";
};

// Coordinator configuration
struct CoordinatorConfig {
  std::string id;
  std::string host;
  uint16_t port;
  std::string offset_storage_path;
  int32_t heartbeat_interval_ms = 10000;
  int32_t session_timeout_ms = 30000;
  int32_t rebalance_timeout_ms = 300000; // 5 minutes
  bool enable_metrics = true;
  uint16_t metrics_port = 8082;
  std::string log_level = "info";
};

// Topic configuration
struct TopicConfig {
  std::string name;
  int32_t partitions;
  int32_t replication_factor;
  std::unordered_map<std::string, std::string> properties;
};

// Configuration loader
class ConfigLoader {
public:
  // Load broker config from YAML file
  [[nodiscard]] static BrokerConfig LoadBrokerConfig(const std::string& config_path);
  
  // Load controller config from YAML file
  [[nodiscard]] static ControllerConfig LoadControllerConfig(const std::string& config_path);
  
  // Load coordinator config from YAML file
  [[nodiscard]] static CoordinatorConfig LoadCoordinatorConfig(const std::string& config_path);
  
  // Load topic configurations
  [[nodiscard]] static std::vector<TopicConfig> LoadTopicConfigs(const std::string& config_path);

private:
  // Helper to parse YAML (simplified implementation)
  static std::unordered_map<std::string, std::string> ParseYaml(const std::string& content);
};

}

