#include "streamit/common/config.h"
#include <fstream>
#include <regex>
#include <sstream>

namespace streamit::common {

namespace {
// Simple YAML parser for basic key-value pairs
std::unordered_map<std::string, std::string> ParseYaml(const std::string& content) {
  std::unordered_map<std::string, std::string> result;
  std::istringstream stream(content);
  std::string line;

  while (std::getline(stream, line)) {
    // Skip empty lines and comments
    if (line.empty() || line[0] == '#') {
      continue;
    }

    // Find colon separator
    size_t colon_pos = line.find(':');
    if (colon_pos == std::string::npos) {
      continue;
    }

    std::string key = line.substr(0, colon_pos);
    std::string value = line.substr(colon_pos + 1);

    // Trim whitespace
    key.erase(0, key.find_first_not_of(" \t"));
    key.erase(key.find_last_not_of(" \t") + 1);
    value.erase(0, value.find_first_not_of(" \t"));
    value.erase(value.find_last_not_of(" \t") + 1);

    // Remove quotes if present
    if (value.length() >= 2 && value[0] == '"' && value.back() == '"') {
      value = value.substr(1, value.length() - 2);
    }

    result[key] = value;
  }

  return result;
}

// Helper to get string value with default
std::string GetString(const std::unordered_map<std::string, std::string>& config, const std::string& key,
                      const std::string& default_value) {
  auto it = config.find(key);
  return it != config.end() ? it->second : default_value;
}

// Helper to get int value with default
int32_t GetInt32(const std::unordered_map<std::string, std::string>& config, const std::string& key,
                 int32_t default_value) {
  auto it = config.find(key);
  if (it == config.end()) {
    return default_value;
  }
  try {
    return std::stoi(it->second);
  } catch (...) {
    return default_value;
  }
}

// Helper to get uint16 value with default
uint16_t GetUint16(const std::unordered_map<std::string, std::string>& config, const std::string& key,
                   uint16_t default_value) {
  auto it = config.find(key);
  if (it == config.end()) {
    return default_value;
  }
  try {
    return static_cast<uint16_t>(std::stoi(it->second));
  } catch (...) {
    return default_value;
  }
}

// Helper to get size_t value with default
size_t GetSizeT(const std::unordered_map<std::string, std::string>& config, const std::string& key,
                size_t default_value) {
  auto it = config.find(key);
  if (it == config.end()) {
    return default_value;
  }
  try {
    return std::stoull(it->second);
  } catch (...) {
    return default_value;
  }
}

// Helper to get int64_t value with default
int64_t GetInt64(const std::unordered_map<std::string, std::string>& config, const std::string& key,
                 int64_t default_value) {
  auto it = config.find(key);
  if (it == config.end()) {
    return default_value;
  }
  try {
    return std::stoll(it->second);
  } catch (...) {
    return default_value;
  }
}

} // namespace

BrokerConfig ConfigLoader::LoadBrokerConfig(const std::string& config_path) {
  std::ifstream file(config_path);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open config file: " + config_path);
  }

  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  auto config = ParseYaml(content);

  BrokerConfig broker_config;
  broker_config.id = GetString(config, "id", "broker-1");
  broker_config.host = GetString(config, "host", "localhost");
  broker_config.port = GetUint16(config, "port", 9092);
  broker_config.log_dir = GetString(config, "log_dir", "./logs");
  broker_config.max_segment_size_bytes = GetSizeT(config, "max_segment_size_bytes", 128 * 1024 * 1024);
  broker_config.segment_roll_interval_ms = GetInt64(config, "segment_roll_interval_ms", 3600000);
  broker_config.max_inflight_bytes = GetSizeT(config, "max_inflight_bytes", 100 * 1024 * 1024);
  broker_config.replication_factor = GetInt32(config, "replication_factor", 1);
  broker_config.min_insync_replicas = GetInt32(config, "min_insync_replicas", 1);
  broker_config.request_timeout_ms = GetInt32(config, "request_timeout_ms", 30000);
  broker_config.replication_timeout_ms = GetInt32(config, "replication_timeout_ms", 10000);
  broker_config.enable_metrics = GetString(config, "enable_metrics", "true") == "true";
  broker_config.metrics_port = GetUint16(config, "metrics_port", 8080);
  broker_config.log_level = GetString(config, "log_level", "info");

  return broker_config;
}

ControllerConfig ConfigLoader::LoadControllerConfig(const std::string& config_path) {
  std::ifstream file(config_path);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open config file: " + config_path);
  }

  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  auto config = ParseYaml(content);

  ControllerConfig controller_config;
  controller_config.id = GetString(config, "id", "controller-1");
  controller_config.host = GetString(config, "host", "localhost");
  controller_config.port = GetUint16(config, "port", 9093);
  controller_config.config_file = GetString(config, "config_file", "./config/topics.yaml");
  controller_config.heartbeat_interval_ms = GetInt32(config, "heartbeat_interval_ms", 10000);
  controller_config.session_timeout_ms = GetInt32(config, "session_timeout_ms", 30000);
  controller_config.enable_metrics = GetString(config, "enable_metrics", "true") == "true";
  controller_config.metrics_port = GetUint16(config, "metrics_port", 8081);
  controller_config.log_level = GetString(config, "log_level", "info");

  return controller_config;
}

CoordinatorConfig ConfigLoader::LoadCoordinatorConfig(const std::string& config_path) {
  std::ifstream file(config_path);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open config file: " + config_path);
  }

  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  auto config = ParseYaml(config_path);

  CoordinatorConfig coordinator_config;
  coordinator_config.id = GetString(config, "id", "coordinator-1");
  coordinator_config.host = GetString(config, "host", "localhost");
  coordinator_config.port = GetUint16(config, "port", 9094);
  coordinator_config.offset_storage_path = GetString(config, "offset_storage_path", "./offsets");
  coordinator_config.heartbeat_interval_ms = GetInt32(config, "heartbeat_interval_ms", 10000);
  coordinator_config.session_timeout_ms = GetInt32(config, "session_timeout_ms", 30000);
  coordinator_config.rebalance_timeout_ms = GetInt32(config, "rebalance_timeout_ms", 300000);
  coordinator_config.enable_metrics = GetString(config, "enable_metrics", "true") == "true";
  coordinator_config.metrics_port = GetUint16(config, "metrics_port", 8082);
  coordinator_config.log_level = GetString(config, "log_level", "info");

  return coordinator_config;
}

std::vector<TopicConfig> ConfigLoader::LoadTopicConfigs(const std::string& config_path) {
  // Simplified implementation - in practice, this would parse a more complex YAML structure
  std::vector<TopicConfig> topics;

  // For now, return some default topics
  topics.push_back({"orders", 6, 1, {}});
  topics.push_back({"events", 3, 1, {}});

  return topics;
}

} // namespace streamit::common
